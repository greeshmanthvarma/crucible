from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from crucible.application.errors import (
    ApplicationError,
    IdempotencyConflict,
    RepositoryNotFound,
    RevisionNotFound,
    TaskNotFound,
    WorkspaceProvisioningFailed,
)
from crucible.application.idempotency import (
    IdempotencyRecord,
    canonical_request_hash,
)
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.context.manifests import ContextManifest
from crucible.domain.clock import Clock
from crucible.domain.events import Event, EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.domain.task import Task
from crucible.domain.tools import ToolCall, ToolResult
from crucible.workspaces.manager import WorkspaceManager


@dataclass(frozen=True)
class StepTrace:
    step: Step
    manifest: ContextManifest | None
    calls: tuple[ToolCall, ...]
    results: tuple[ToolResult, ...]


@dataclass(frozen=True)
class WorkspaceState:
    status: str
    diff: str
    status_truncated: bool
    diff_truncated: bool


class TaskService:
    def __init__(
        self,
        workspaces: WorkspaceManager,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
    ) -> None:
        self._workspaces = workspaces
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._events = EventFactory()

    async def create(
        self, repository_id: UUID, source_ref: str, idempotency_key: str
    ) -> Task:
        scope = f"repository:{repository_id}:create-task"
        request_hash = canonical_request_hash({"source_ref": source_ref})
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                task = await uow.tasks.get(UUID(str(previous.response_json["taskId"])))
                if task is None:
                    raise RuntimeError("Idempotency record references a missing Task")
                if previous.response_json.get("errorCode") == RevisionNotFound.code:
                    raise RevisionNotFound(
                        str(
                            previous.response_json.get(
                                "errorDetail", "Revision not found"
                            )
                        )
                    )
                return task
            repository = await uow.repositories.get(repository_id)
        if repository is None:
            raise RepositoryNotFound(f"Repository not found: {repository_id}")

        task_id = new_id()
        try:
            plan = await self._workspaces.plan(
                repository.root_path, source_ref, task_id
            )
        except RevisionNotFound as error:
            await self._record_unresolved_failure(
                task_id,
                repository_id,
                source_ref,
                error,
                scope,
                idempotency_key,
                request_hash,
            )
            raise

        task = Task.provisioning(
            task_id=task_id,
            repository_id=repository_id,
            source_ref=source_ref,
            base_revision=plan.base_revision,
            workspace_path=plan.workspace_path,
            clock=self._clock,
        )
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                existing = await uow.tasks.get(
                    UUID(str(previous.response_json["taskId"]))
                )
                if existing is None:
                    raise RuntimeError("Idempotency record references a missing Task")
                return existing
            await uow.tasks.add(task)
            await uow.events.append(
                self._event(task, EventType.TASK_PROVISIONING_STARTED)
            )
            await uow.idempotency.add(
                IdempotencyRecord(
                    id=new_id(),
                    scope=scope,
                    key=idempotency_key,
                    request_hash=request_hash,
                    response_status=201,
                    response_json={"taskId": str(task.id)},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        await self._notify(task.id)

        try:
            await self._workspaces.create(plan)
        except Exception as error:
            await self._record_provisioning_failure(task, error)
            if isinstance(error, ApplicationError):
                raise
            raise WorkspaceProvisioningFailed(str(error)[:1000]) from error

        active = task.activate(
            workspace_path=plan.workspace_path,
            clock=self._clock,
        )
        async with self._unit_of_work() as uow:
            await uow.tasks.update(active)
            await uow.events.append(
                self._event(active, EventType.TASK_PROVISIONING_SUCCEEDED)
            )
            await uow.commit()
        await self._notify(active.id)
        return active

    async def get(self, task_id: UUID) -> Task:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
        if task is None:
            raise TaskNotFound(f"Task not found: {task_id}")
        return task

    async def trace(self, task_id: UUID) -> tuple[StepTrace, ...]:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            runs = await uow.runs.list_for_task(task_id)
            traces: list[StepTrace] = []
            for run in runs:
                for step in await uow.steps.list_for_run(run.id):
                    traces.append(
                        StepTrace(
                            step,
                            await uow.context_manifests.get_for_step(step.id),
                            await uow.tool_calls.list_for_step(step.id),
                            await uow.tool_results.list_for_step(step.id),
                        )
                    )
            return tuple(traces)

    async def workspace_state(
        self, task_id: UUID, *, limit: int = 200_000
    ) -> WorkspaceState:
        task = await self.get(task_id)
        status = await self._workspaces.status(task.workspace_path)
        diff = await self._workspaces.diff(task.workspace_path)
        status_text, status_truncated = _bounded(status.stdout, limit)
        diff_text, diff_truncated = _bounded(diff.stdout, limit)
        return WorkspaceState(
            status_text,
            diff_text,
            status_truncated,
            diff_truncated,
        )

    async def _record_unresolved_failure(
        self,
        task_id: UUID,
        repository_id: UUID,
        source_ref: str,
        error: RevisionNotFound,
        scope: str,
        idempotency_key: str,
        request_hash: str,
    ) -> None:
        task = Task.failed_without_revision(
            task_id=task_id,
            repository_id=repository_id,
            source_ref=source_ref,
            workspace_path=self._workspaces.destination_for(task_id),
            detail=error.detail[:1000],
            clock=self._clock,
        )
        async with self._unit_of_work() as uow:
            await uow.tasks.add(task)
            await uow.events.append(
                self._event(task, EventType.TASK_PROVISIONING_STARTED)
            )
            await uow.events.append(
                self._event(task, EventType.TASK_PROVISIONING_FAILED)
            )
            await uow.idempotency.add(
                IdempotencyRecord(
                    id=new_id(),
                    scope=scope,
                    key=idempotency_key,
                    request_hash=request_hash,
                    response_status=error.status_code,
                    response_json={
                        "taskId": str(task.id),
                        "errorCode": error.code,
                        "errorDetail": error.detail,
                    },
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        await self._notify(task.id)

    async def _record_provisioning_failure(self, task: Task, error: Exception) -> None:
        code = (
            error.code
            if isinstance(error, ApplicationError)
            else WorkspaceProvisioningFailed.code
        )
        failed = task.fail_provisioning(code, str(error)[:1000], self._clock)
        async with self._unit_of_work() as uow:
            await uow.tasks.update(failed)
            await uow.events.append(
                self._event(failed, EventType.TASK_PROVISIONING_FAILED)
            )
            await uow.commit()
        await self._notify(failed.id)

    async def _notify(self, task_id: UUID) -> None:
        if self._notifier is not None:
            await self._notifier.notify(task_id)

    def _event(self, task: Task, event_type: EventType) -> Event:
        payload: dict[str, object] = {
            "schema_version": 1,
            "task_id": str(task.id),
            "status": task.status,
        }
        if task.failure_code is not None:
            payload["failure_code"] = task.failure_code
        return self._events.create(
            task_id=task.id,
            run_id=None,
            type=event_type,
            payload=payload,
            created_at=self._clock.now(),
        )


def _bounded(value: str, limit: int) -> tuple[str, bool]:
    encoded = value.encode()
    if len(encoded) <= limit:
        return value, False
    return encoded[:limit].decode(errors="replace"), True
