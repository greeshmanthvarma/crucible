import asyncio
from datetime import UTC, datetime

from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.tools import ToolCall, ToolExecutionMode, ToolResultStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


class WaitingEngine:
    process_execution_id = new_id()

    async def execute(self, run_id) -> bool:
        await asyncio.Future()
        return True


async def test_cancel_pending_command_terminalizes_every_record(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "cancel-command")
        call = ToolCall(
            new_id(),
            task.id,
            run.id,
            step.id,
            message.id,
            1,
            "execute_command",
            {},
            1,
            None,
            ToolExecutionMode.SEQUENTIAL,
            NOW,
        )
        await uow.tool_calls.add(call)
        spec = CommandSpec(
            "true",
            (),
            ".",
            30,
            CommandNetwork.NONE,
            {},
            "runner@sha256:" + "a" * 64,
            "test",
            CommandLimits(1, 1024, 16, 100),
        )
        approval = Approval.requested(
            new_id(), task.id, run.id, step.id, call.id, spec, NOW
        )
        await uow.approvals.add(approval)
        await uow.commit()
    broker = InMemoryApprovalBroker()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    supervisor = LocalRunSupervisor(
        WaitingEngine(),
        factory,
        CLOCK,
        approval_broker=broker,  # type: ignore[arg-type]
    )
    await supervisor.submit(run.id)
    await asyncio.sleep(0)

    await supervisor.cancel(run.id)

    async with SqlAlchemyUnitOfWork(database) as uow:
        stored_run = await uow.runs.get(run.id)
        stored_approval = await uow.approvals.get(approval.id)
        results = await uow.tool_results.list_for_step(step.id)
    assert stored_run is not None and stored_run.outcome_code == "cancelled"
    assert stored_approval is not None
    assert stored_approval.status is ApprovalStatus.CANCELLED
    assert results[0].status is ToolResultStatus.CANCELLED
