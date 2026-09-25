from collections.abc import Callable

from crucible.application.ports import UnitOfWork
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.resources import ExternalResourceKind
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.sandbox.protocol import SandboxBackend
from crucible.sandbox.resources import TaskResourceManager


class SandboxReconciler:
    def __init__(
        self,
        sandbox: SandboxBackend,
        resources: TaskResourceManager,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        broker: InMemoryApprovalBroker,
    ) -> None:
        self._sandbox = sandbox
        self._resources = resources
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._broker = broker
        self._events = EventFactory()

    async def reconcile(self) -> None:
        async with self._unit_of_work() as uow:
            managed = await uow.external_resources.list_managed()
            runs = await uow.runs.list_running()
        containers = tuple(
            resource
            for resource in managed
            if resource.kind is ExternalResourceKind.CONTAINER
        )
        await self._sandbox.reconcile(containers)
        await self._resources.reconcile()

        invalidated = []
        async with self._unit_of_work() as uow:
            for run in runs:
                unresolved = {
                    call.id
                    for call in await uow.tool_calls.list_without_result_for_run(run.id)
                }
                for approval in await uow.approvals.list_for_run(run.id):
                    if (
                        approval.tool_call_id not in unresolved
                        or approval.status
                        not in (
                            ApprovalStatus.PENDING,
                            ApprovalStatus.APPROVED,
                        )
                    ):
                        continue
                    changed = approval.invalidate_for_recovery(
                        "process_restarted", self._clock.now()
                    )
                    await uow.approvals.update(changed)
                    await uow.events.append(
                        self._events.create(
                            task_id=changed.task_id,
                            run_id=changed.run_id,
                            type=EventType.APPROVAL_INVALIDATED,
                            payload={
                                "approval_id": str(changed.id),
                                "status": changed.status.value,
                            },
                            created_at=self._clock.now(),
                        )
                    )
                    invalidated.append(changed)
            await uow.commit()
        for approval in invalidated:
            await self._broker.publish(approval.id, approval.status)
