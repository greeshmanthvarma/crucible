import asyncio
from collections import defaultdict

from crucible.domain.approvals import ApprovalStatus
from crucible.domain.ids import ApprovalId


class InMemoryApprovalBroker:
    """Process-local wakeups; callers must still treat SQLite as authoritative."""

    def __init__(self) -> None:
        self._conditions: defaultdict[ApprovalId, asyncio.Condition] = defaultdict(
            asyncio.Condition
        )
        self._decisions: dict[ApprovalId, ApprovalStatus] = {}

    async def wait(self, approval_id: ApprovalId) -> ApprovalStatus:
        condition = self._conditions[approval_id]
        async with condition:
            await condition.wait_for(lambda: approval_id in self._decisions)
            return self._decisions[approval_id]

    async def publish(self, approval_id: ApprovalId, decision: ApprovalStatus) -> None:
        if decision is ApprovalStatus.PENDING:
            raise ValueError("Cannot publish a pending Approval")
        condition = self._conditions[approval_id]
        async with condition:
            self._decisions[approval_id] = decision
            condition.notify_all()
