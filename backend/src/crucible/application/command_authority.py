from collections.abc import Callable
from uuid import UUID

from crucible.application.approval_service import ApprovalService
from crucible.application.ports import UnitOfWork
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.commands import CommandSpec
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.repair import RepairStage, RepairStatus
from crucible.domain.run import RunStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker


class CommandAuthority:
    def __init__(
        self,
        approvals: ApprovalService,
        broker: InMemoryApprovalBroker,
        clock: Clock,
        unit_of_work: Callable[[], UnitOfWork] | None = None,
        repair_execution_id: Callable[[], str] | None = None,
    ) -> None:
        self._approvals = approvals
        self._broker = broker
        self._clock = clock
        self._unit_of_work = unit_of_work
        self._repair_execution_id = repair_execution_id

    async def request_and_wait(
        self,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        tool_call_id: UUID,
        spec: CommandSpec,
    ) -> Approval:
        requested = Approval.requested(
            new_id(),
            task_id,
            run_id,
            step_id,
            tool_call_id,
            spec,
            self._clock.now(),
        )
        await self._approvals.request(requested)
        automatic = await self._approve_repair(requested)
        if not automatic:
            await self._broker.wait(requested.id)
        decided = await self._approvals.get(requested.id)
        if decided.status is ApprovalStatus.PENDING:
            raise RuntimeError("Approval wakeup did not have a durable decision")
        return decided

    async def _approve_repair(self, requested: Approval) -> bool:
        if self._unit_of_work is None or self._repair_execution_id is None:
            return False
        async with self._unit_of_work() as uow:
            await uow.repairs.lock()
            attempt = await uow.repairs.attempt_for_run(requested.run_id)
            if attempt is None:
                return False
            repair = await uow.repairs.get(attempt.repair_id)
            owner = await uow.repairs.owner()
            run = await uow.runs.get(requested.run_id)
            task = await uow.tasks.get(requested.task_id)
            if (
                repair is None
                or repair.status is not RepairStatus.RUNNING
                or repair.stage is not RepairStage.PROPOSAL
                or attempt.role != "proposal"
                or attempt.status != "running"
                or not attempt.charged
                or attempt.fix_id is None
                or attempt.fix_id != repair.current_fix_id
                or owner.repair_id != repair.id
                or owner.owner_execution_id != self._repair_execution_id()
                or task is None
                or task.internal_repair_id != repair.id
                or str(task.workspace_path) != repair.worktree_path
                or run is None
                or run.task_id != task.id
                or run.status is not RunStatus.RUNNING
            ):
                raise RuntimeError("Repair command has no live controller authority")
            unresolved = await uow.tool_calls.list_without_result_for_run(run.id)
            if not any(
                call.id == requested.tool_call_id and call.step_id == requested.step_id
                for call in unresolved
            ):
                raise RuntimeError("Repair command is no longer outstanding")
            # Exact CommandSpec and digest already reside on the requested Approval.
            decided = requested.approve(
                f"repair-controller:{repair.id}", self._clock.now()
            )
            if not await uow.approvals.decide_pending(decided):
                raise RuntimeError("Repair command decision changed concurrently")
            await uow.events.append(
                EventFactory().create(
                    task_id=task.id,
                    run_id=run.id,
                    type=EventType.APPROVAL_APPROVED,
                    created_at=self._clock.now(),
                    payload={
                        "approval_id": str(decided.id),
                        "tool_call_id": str(decided.tool_call_id),
                        "spec_digest": decided.spec_digest,
                        "status": decided.status,
                    },
                )
            )
            await uow.commit()
        return True
