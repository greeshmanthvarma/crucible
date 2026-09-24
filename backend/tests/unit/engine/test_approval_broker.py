import asyncio

from crucible.domain.approvals import ApprovalStatus
from crucible.domain.ids import new_id
from crucible.engine.approval_broker import InMemoryApprovalBroker


async def test_broker_wakes_all_waiters_and_remembers_early_publication() -> None:
    broker = InMemoryApprovalBroker()
    approval_id = new_id()
    first = asyncio.create_task(broker.wait(approval_id))
    second = asyncio.create_task(broker.wait(approval_id))
    await asyncio.sleep(0)

    await broker.publish(approval_id, ApprovalStatus.APPROVED)

    assert await first is ApprovalStatus.APPROVED
    assert await second is ApprovalStatus.APPROVED
    assert await broker.wait(approval_id) is ApprovalStatus.APPROVED
