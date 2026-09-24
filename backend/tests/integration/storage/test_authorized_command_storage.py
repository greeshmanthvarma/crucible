from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import IntegrityError

from crucible.domain.approvals import Approval
from crucible.domain.artifacts import Artifact
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.resources import ExternalResource, ExternalResourceKind
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)


def spec() -> CommandSpec:
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


async def test_authorized_command_records_round_trip_with_task_ownership(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "authorized")
        from crucible.domain.tools import ToolCall, ToolExecutionMode

        call = ToolCall(
            new_id(),
            task.id,
            run.id,
            step.id,
            message.id,
            1,
            "execute_command",
            {"executable": "python"},
            1,
            None,
            ToolExecutionMode.SEQUENTIAL,
            NOW,
        )
        await uow.tool_calls.add(call)
        approval = Approval.requested(
            new_id(), task.id, run.id, step.id, call.id, spec(), NOW
        )
        artifact = Artifact(
            new_id(), task.id, "abc", "text/plain", 3, "ab/abc", "private", {}, NOW
        )
        resource = ExternalResource.volume(
            new_id(), task.id, "docker-volume-id", "/workspace/node_modules", NOW
        )
        await uow.approvals.add(approval)
        await uow.artifacts.add(artifact)
        await uow.external_resources.add(resource)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(database) as uow:
        assert await uow.approvals.get(approval.id) == approval
        assert await uow.approvals.get_for_tool_call(call.id) == approval
        assert await uow.artifacts.get(artifact.id) == artifact
        assert await uow.external_resources.get(resource.id) == resource
        assert (await uow.external_resources.list_for_task(task.id))[
            0
        ].kind is ExternalResourceKind.VOLUME


async def test_one_approval_per_tool_call_is_enforced(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "duplicate-approval")
        from crucible.domain.tools import ToolCall, ToolExecutionMode

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
        await uow.approvals.add(
            Approval.requested(new_id(), task.id, run.id, step.id, call.id, spec(), NOW)
        )
        with pytest.raises(IntegrityError):
            await uow.approvals.add(
                Approval.requested(
                    new_id(), task.id, run.id, step.id, call.id, spec(), NOW
                )
            )
            await uow.commit()
