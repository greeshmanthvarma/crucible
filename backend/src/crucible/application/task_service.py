from collections.abc import Callable
from uuid import UUID

from crucible.application.errors import (
    ApplicationError,
    RepositoryNotFound,
    RevisionNotFound,
    TaskNotFound,
    WorkspaceProvisioningFailed,
)
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id
from crucible.domain.task import Task
from crucible.workspaces.manager import WorkspaceManager


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

    async def create(self, repository_id: UUID, source_ref: str) -> Task:
        async with self._unit_of_work() as uow:
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
                task_id, repository_id, source_ref, error
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
            await uow.tasks.add(task)
            await uow.events.append(
                self._event(task, EventType.TASK_PROVISIONING_STARTED)
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

    async def _record_unresolved_failure(
        self,
        task_id: UUID,
        repository_id: UUID,
        source_ref: str,
        error: RevisionNotFound,
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
        return Event(
            id=new_id(),
            task_id=task.id,
            run_id=None,
            task_sequence=0,
            run_sequence=None,
            type=event_type,
            schema_version=1,
            payload=payload,
            created_at=self._clock.now(),
        )
