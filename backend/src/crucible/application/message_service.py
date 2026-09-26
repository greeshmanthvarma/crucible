import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from crucible.application.errors import (
    IdempotencyConflict,
    TaskNotActive,
    TaskNotFound,
    WorkspaceProvisioningFailed,
)
from crucible.application.idempotency import (
    IdempotencyRecord,
    canonical_request_hash,
)
from crucible.application.ports import EventNotifier, RunSupervisor, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.results import IntegrationStatus
from crucible.domain.run import Run, RunStatus
from crucible.domain.task import TaskStatus
from crucible.workspaces.git import GitClient
from crucible.workspaces.manager import WorkspaceManager, WorkspacePlan

logger = logging.getLogger(__name__)


class MessageSubmissionKind(StrEnum):
    STEERING = "steering"
    NEW_RUN = "new_run"


@dataclass(frozen=True)
class SubmittedRun:
    message_id: UUID
    run_id: UUID
    run_status: RunStatus
    kind: MessageSubmissionKind = MessageSubmissionKind.NEW_RUN
    response_status: int = 202

    def response_json(self) -> dict[str, object]:
        return {
            "messageId": str(self.message_id),
            "runId": str(self.run_id),
            "runStatus": self.run_status,
            "kind": self.kind,
        }


class MessageService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        supervisor: RunSupervisor,
        notifier: EventNotifier | None = None,
        git: GitClient | None = None,
        workspaces: WorkspaceManager | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._supervisor = supervisor
        self._notifier = notifier
        self._events = EventFactory()
        self._git = git
        self._workspaces = workspaces
        self._task_locks: dict[UUID, asyncio.Lock] = {}

    async def submit(
        self, task_id: UUID, text: str, idempotency_key: str
    ) -> SubmittedRun:
        async with self._task_locks.setdefault(task_id, asyncio.Lock()):
            return await self._submit_locked(task_id, text, idempotency_key)

    async def _submit_locked(
        self, task_id: UUID, text: str, idempotency_key: str
    ) -> SubmittedRun:
        scope = f"POST:/api/tasks/{task_id}/messages"
        request_hash = canonical_request_hash({"text": text})
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                return SubmittedRun(
                    message_id=UUID(str(previous.response_json["messageId"])),
                    run_id=UUID(str(previous.response_json["runId"])),
                    run_status=RunStatus(str(previous.response_json["runStatus"])),
                    kind=MessageSubmissionKind(
                        str(previous.response_json.get("kind", "new_run"))
                    ),
                    response_status=previous.response_status,
                )
        await self._ensure_continuation(task_id)
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                return SubmittedRun(
                    message_id=UUID(str(previous.response_json["messageId"])),
                    run_id=UUID(str(previous.response_json["runId"])),
                    run_status=RunStatus(str(previous.response_json["runStatus"])),
                    kind=MessageSubmissionKind(
                        str(previous.response_json.get("kind", "new_run"))
                    ),
                    response_status=previous.response_status,
                )
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            if task.status is TaskStatus.ACCEPTED:
                task = task.reopen(self._clock)
                await uow.tasks.update(task)
                await uow.events.append(
                    self._events.create(
                        task_id=task_id,
                        run_id=None,
                        type=EventType.TASK_REOPENED,
                        created_at=self._clock.now(),
                    )
                )
            elif task.status is not TaskStatus.ACTIVE:
                raise TaskNotActive(f"Task is not active: {task_id}")

            now = self._clock.now()
            active_run = await uow.runs.get_nonterminal_for_task(task_id)
            message_id = new_id()
            if active_run is not None:
                message = await uow.messages.add(
                    Message(
                        id=message_id,
                        task_id=task_id,
                        run_id=active_run.id,
                        step_id=None,
                        conversation_sequence=0,
                        role=MessageRole.USER,
                        status=MessageStatus.COMPLETED,
                        parts=(
                            MessagePart(
                                id=new_id(),
                                part_sequence=1,
                                kind=MessagePartKind.TEXT,
                                text_content=text,
                            ),
                        ),
                        created_at=now,
                        completed_at=now,
                    )
                )
                await uow.events.append(
                    self._events.create(
                        task_id=task_id,
                        run_id=active_run.id,
                        type=EventType.MESSAGE_STEERING_ADDED,
                        payload={
                            "run_id": str(active_run.id),
                            "message_id": str(message.id),
                        },
                        created_at=now,
                    )
                )
                result = SubmittedRun(
                    message.id,
                    active_run.id,
                    active_run.status,
                    MessageSubmissionKind.STEERING,
                )
                await uow.idempotency.add(
                    IdempotencyRecord(
                        id=new_id(),
                        scope=scope,
                        key=idempotency_key,
                        request_hash=request_hash,
                        response_status=result.response_status,
                        response_json=result.response_json(),
                        created_at=now,
                    )
                )
                await uow.commit()
                run_id = active_run.id
                should_submit = False
            else:
                run_id = new_id()
                repository = await uow.repositories.get(task.repository_id)
                if repository is None:
                    raise TaskNotFound(f"Repository not found for Task: {task_id}")
                run = Run.queued(
                    run_id=run_id,
                    task_id=task_id,
                    triggering_message_id=None,
                    created_at=now,
                    settings_snapshot=repository.settings,
                )
                await uow.runs.add(run)
                message = await uow.messages.add(
                    Message(
                        id=message_id,
                        task_id=task_id,
                        run_id=run_id,
                        step_id=None,
                        conversation_sequence=0,
                        role=MessageRole.USER,
                        status=MessageStatus.COMPLETED,
                        parts=(
                            MessagePart(
                                id=new_id(),
                                part_sequence=1,
                                kind=MessagePartKind.TEXT,
                                text_content=text,
                            ),
                        ),
                        created_at=now,
                        completed_at=now,
                    )
                )
                await uow.runs.set_triggering_message(run_id, message.id)
                await uow.events.append(
                    self._events.create(
                        task_id=task_id,
                        run_id=run_id,
                        type=EventType.RUN_QUEUED,
                        payload={"run_id": str(run_id), "message_id": str(message_id)},
                        created_at=now,
                    )
                )
                result = SubmittedRun(
                    message_id, run_id, RunStatus.QUEUED, MessageSubmissionKind.NEW_RUN
                )
                await uow.idempotency.add(
                    IdempotencyRecord(
                        id=new_id(),
                        scope=scope,
                        key=idempotency_key,
                        request_hash=request_hash,
                        response_status=result.response_status,
                        response_json=result.response_json(),
                        created_at=now,
                    )
                )
                await uow.commit()
                should_submit = True

        if self._notifier is not None:
            await self._notifier.notify(task_id)
        if should_submit:
            try:
                await self._supervisor.submit(run_id)
            except Exception:
                logger.exception(
                    "Run submission failed after durable commit",
                    extra={"run_id": str(run_id)},
                )
        return result

    async def _ensure_continuation(self, task_id: UUID) -> None:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            if task.status not in (TaskStatus.INTEGRATED, TaskStatus.CONTINUING):
                return
            if self._git is None or self._workspaces is None:
                raise TaskNotActive("Integrated chat continuation is not configured")
            repository = await uow.repositories.get(task.repository_id)
            if repository is None:
                raise TaskNotFound(f"Repository not found for Task: {task_id}")
            if task.status is TaskStatus.INTEGRATED:
                results = await uow.result_revisions.list_for_task(task_id)
                integrations = [
                    integration
                    for result in results
                    for integration in await uow.integrations.list_for_result(result.id)
                    if integration.status is IntegrationStatus.COMPLETED
                ]
                if not integrations:
                    raise WorkspaceProvisioningFailed(
                        "Integrated Task has no completed Integration"
                    )
                latest = max(
                    integrations,
                    key=lambda value: value.completed_at or value.created_at,
                )
                snapshot = await self._git.target_snapshot(repository.root_path)
                if snapshot.current_ref != latest.target_ref:
                    raise WorkspaceProvisioningFailed(
                        f"Checkout must be on integrated target {latest.target_ref}"
                    )
                task = task.begin_continuation(
                    base_revision=snapshot.head_revision,
                    workspace_path=self._workspaces.destination_for(
                        task_id, task.workspace_generation + 1
                    ),
                    clock=self._clock,
                )
                await uow.tasks.update(task)
                await uow.events.append(
                    self._events.create(
                        task_id=task_id,
                        run_id=None,
                        type=EventType.TASK_CONTINUATION_STARTED,
                        payload={"generation": task.workspace_generation},
                        created_at=self._clock.now(),
                    )
                )
                await uow.commit()

        base = task.workspace_base_revision
        if base is None:
            raise WorkspaceProvisioningFailed("Continuation has no recorded revision")
        await self._workspaces.recover(
            WorkspacePlan(
                repository.root_path, task.source_ref, base, task.workspace_path
            )
        )
        async with self._unit_of_work() as uow:
            current = await uow.tasks.get(task_id)
            if current is None or current.status is not TaskStatus.CONTINUING:
                raise WorkspaceProvisioningFailed("Continuation state changed")
            active = current.finish_continuation(self._clock)
            await uow.tasks.update(active)
            await uow.events.append(
                self._events.create(
                    task_id=task_id,
                    run_id=None,
                    type=EventType.TASK_CONTINUATION_SUCCEEDED,
                    payload={"generation": active.workspace_generation},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(task_id)

    async def list(self, task_id: UUID) -> tuple[Message, ...]:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            return await uow.messages.list_for_task(task_id)
