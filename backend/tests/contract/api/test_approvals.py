from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.approval_service import ApprovalService
from crucible.domain.approvals import Approval
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.tools import ToolCall, ToolExecutionMode
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.notifier import TaskEventNotifier
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


def command() -> CommandSpec:
    return CommandSpec(
        "python",
        ("-V",),
        ".",
        30,
        CommandNetwork.NONE,
        {"CI": "1"},
        "runner@sha256:" + "a" * 64,
        "Check Python",
        CommandLimits(1, 1024**3, 64, 10_000),
    )


async def pending_approval(database: Database) -> Approval:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "approval-api")
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
        approval = Approval.requested(
            new_id(), task.id, run.id, step.id, call.id, command(), NOW
        )
        await uow.approvals.add(approval)
        await uow.commit()
        return approval


async def test_decision_is_single_use_digest_bound_and_idempotent(
    database: Database,
) -> None:
    approval = await pending_approval(database)
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    service = ApprovalService(
        factory, CLOCK, InMemoryApprovalBroker(), TaskEventNotifier()
    )
    app = create_app(approval_service=service)
    endpoint = f"/api/approvals/{approval.id}/decision"
    body = {"decision": "approved", "specDigest": approval.spec_digest}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        approved = await client.post(
            endpoint, json=body, headers={"Idempotency-Key": "decision-1"}
        )
        missing_key = await client.post(endpoint, json=body)
        replay = await client.post(
            endpoint, json=body, headers={"Idempotency-Key": "decision-1"}
        )
        stale = await client.post(
            endpoint,
            json={"decision": "approved", "specDigest": "changed"},
            headers={"Idempotency-Key": "decision-2"},
        )
        listed = await client.get(f"/api/tasks/{approval.task_id}/approvals")

    assert approved.status_code == 200
    assert missing_key.status_code == 400
    assert approved.json()["status"] == "approved"
    assert replay.json() == approved.json()
    assert stale.status_code == 409
    assert listed.json() == [approved.json()]
