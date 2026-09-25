from datetime import UTC, datetime, timedelta

from crucible.application.sandbox_reconciliation import SandboxReconciler
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.resources import ExternalResource
from crucible.domain.tools import ToolCall, ToolExecutionMode, ToolResultStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.sandbox.fake import FakeSandboxBackend
from crucible.sandbox.protocol import SandboxOutcome, SandboxTermination
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.engine.test_command_cancellation import WaitingEngine
from tests.integration.storage.test_tool_loop_storage import seed_exchange
from tests.unit.sandbox.test_resources import FakeDockerClient

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


async def test_restart_removes_owned_container_and_never_replays_uncertain_command(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "sandbox-restart")
        execution_id = new_id()
        assert await uow.runs.claim_queued(
            run.id, execution_id, NOW, NOW + timedelta(minutes=5)
        )
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
        ).approve("local-user", NOW)
        await uow.approvals.add(approval)
        container = ExternalResource.container(
            new_id(),
            task.id,
            run.id,
            call.id,
            "exact-id",
            NOW,
            labels={
                "harness.managed": "true",
                "harness.task_id": str(task.id),
                "harness.resource_kind": "container",
                "harness.resource_id": "resource",
            },
        )
        await uow.external_resources.add(container)
        await uow.commit()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    broker = InMemoryApprovalBroker()
    sandbox = FakeSandboxBackend(
        SandboxOutcome(
            0,
            SandboxTermination.COMPLETED,
            NOW,
            NOW,
            "image",
            "unused",
            0,
            0,
            False,
        )
    )
    reconciler = SandboxReconciler(
        sandbox,
        TaskResourceManager(FakeDockerClient(), factory, CLOCK),
        factory,
        CLOCK,
        broker,
    )
    supervisor = LocalRunSupervisor(
        WaitingEngine(),
        factory,
        CLOCK,  # type: ignore[arg-type]
    )

    await reconciler.reconcile()
    await supervisor.reconcile()

    async with SqlAlchemyUnitOfWork(database) as uow:
        stored_run = await uow.runs.get(run.id)
        stored_approval = await uow.approvals.get(approval.id)
        results = await uow.tool_results.list_for_step(step.id)
    assert sandbox.requests == []
    assert sandbox.reconciled == [(container,)]
    assert stored_run is not None and stored_run.outcome_code == "process_restarted"
    assert stored_approval is not None
    assert stored_approval.status is ApprovalStatus.INVALIDATED
    assert results[0].status is ToolResultStatus.INTERRUPTED
