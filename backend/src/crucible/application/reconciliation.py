from collections.abc import Callable
from typing import TYPE_CHECKING

from crucible.application.errors import ApplicationError, WorkspaceProvisioningFailed
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.task import Task, TaskStatus
from crucible.workspaces.manager import WorkspaceManager, WorkspacePlan

if TYPE_CHECKING:
    from crucible.sandbox.resources import TaskResourceManager


class StartupReconciler:
    def __init__(
        self,
        workspaces: WorkspaceManager,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
        resource_manager: "TaskResourceManager | None" = None,
    ) -> None:
        self._workspaces = workspaces
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._events = EventFactory()
        self._resource_manager = resource_manager

    async def reconcile(self) -> None:
        if self._resource_manager is not None:
            await self._resource_manager.reconcile()
        async with self._unit_of_work() as uow:
            tasks = await uow.tasks.list_provisioning()
        for task in tasks:
            await self._reconcile_task(task)

    async def _reconcile_task(self, task: Task) -> None:
        async with self._unit_of_work() as uow:
            repository = await uow.repositories.get(task.repository_id)
        if repository is None or task.base_revision is None:
            await self._fail(task, "repository_not_found", "Repository is unavailable")
            return
        plan = WorkspacePlan(
            repository_root=repository.root_path,
            source_ref=task.source_ref,
            base_revision=task.base_revision,
            workspace_path=task.workspace_path,
        )
        try:
            provisioned = await self._workspaces.recover(plan)
            if self._resource_manager is not None:
                await self._resource_manager.ensure_dependency_volume(task.id)
        except Exception as error:
            code = (
                error.code
                if isinstance(error, ApplicationError)
                else WorkspaceProvisioningFailed.code
            )
            await self._fail(task, code, str(error)[:1000])
            return
        active = task.activate(
            workspace_path=provisioned.workspace_path, clock=self._clock
        )
        await self._persist(active, EventType.TASK_PROVISIONING_SUCCEEDED)

    async def _fail(self, task: Task, code: str, detail: str) -> None:
        failed = task.fail_provisioning(code, detail, self._clock)
        await self._persist(failed, EventType.TASK_PROVISIONING_FAILED)

    async def _persist(self, task: Task, event_type: EventType) -> None:
        async with self._unit_of_work() as uow:
            current = await uow.tasks.get(task.id)
            if current is None or current.status is not TaskStatus.PROVISIONING:
                return
            await uow.tasks.update(task)
            await uow.events.append(
                self._events.create(
                    task_id=task.id,
                    run_id=None,
                    type=event_type,
                    payload={
                        "task_id": str(task.id),
                        "status": task.status,
                    },
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(task.id)
