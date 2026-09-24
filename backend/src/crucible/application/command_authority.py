from uuid import UUID

from crucible.application.approval_service import ApprovalService
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.commands import CommandSpec
from crucible.domain.ids import new_id
from crucible.engine.approval_broker import InMemoryApprovalBroker


class CommandAuthority:
    def __init__(
        self,
        approvals: ApprovalService,
        broker: InMemoryApprovalBroker,
        clock: Clock,
    ) -> None:
        self._approvals = approvals
        self._broker = broker
        self._clock = clock

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
        await self._broker.wait(requested.id)
        decided = await self._approvals.get(requested.id)
        if decided.status is ApprovalStatus.PENDING:
            raise RuntimeError("Approval wakeup did not have a durable decision")
        return decided
