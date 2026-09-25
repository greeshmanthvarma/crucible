from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from crucible.context.manifests import ContextManifest
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository
from crucible.domain.run import Run
from crucible.domain.steps import Step
from crucible.domain.task import Task
from crucible.domain.tools import (
    ToolCall,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork

NOW = datetime(2026, 9, 21, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


async def seed_exchange(uow: SqlAlchemyUnitOfWork, suffix: str):
    repository = Repository(new_id(), Path(f"/repo/{suffix}"), NOW)
    await uow.repositories.add(repository)
    task = Task.provisioning(
        task_id=new_id(),
        repository_id=repository.id,
        source_ref="HEAD",
        base_revision="a" * 40,
        workspace_path=Path(f"/work/{suffix}"),
        clock=CLOCK,
    )
    await uow.tasks.add(task)
    run = Run.queued(
        run_id=new_id(), task_id=task.id, triggering_message_id=None, created_at=NOW
    )
    await uow.runs.add(run)
    step = Step.preparing(new_id(), task.id, run.id, 1, NOW)
    await uow.steps.add(step)
    message = await uow.messages.add(
        Message(
            id=new_id(),
            task_id=task.id,
            run_id=run.id,
            step_id=step.id,
            conversation_sequence=0,
            role=MessageRole.ASSISTANT,
            status=MessageStatus.COMPLETED,
            parts=(MessagePart(new_id(), 1, MessagePartKind.TEXT, "inspect"),),
            created_at=NOW,
            completed_at=NOW,
        )
    )
    return task, run, step, message


async def test_structured_exchange_round_trips_in_source_and_completion_order(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "round-trip")
        manifest = ContextManifest(
            new_id(),
            task.id,
            run.id,
            step.id,
            "fixture",
            {"temperature": 0},
            1000,
            200,
            0.8,
            300,
            [message.id],
            [message.parts[0].id],
            {"harness": "abc"},
            "tools",
            NOW,
        )
        await uow.context_manifests.add(manifest)
        calls = []
        for sequence in (1, 2):
            call = ToolCall(
                new_id(),
                task.id,
                run.id,
                step.id,
                message.id,
                sequence,
                "read_file",
                {"path": f"{sequence}.txt"},
                1,
                None,
                ToolExecutionMode.PARALLEL,
                NOW,
            )
            await uow.tool_calls.add(call)
            calls.append(call)
        for completion_sequence, call in enumerate(reversed(calls), 1):
            await uow.tool_results.add(
                ToolResult(
                    new_id(),
                    task.id,
                    run.id,
                    step.id,
                    call.id,
                    ToolResultStatus.SUCCEEDED,
                    {"ok": True},
                    1,
                    "ok",
                    None,
                    completion_sequence,
                    NOW,
                    NOW,
                )
            )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(database) as uow:
        assert [
            call.call_sequence for call in await uow.tool_calls.list_for_step(step.id)
        ] == [1, 2]
        results = await uow.tool_results.list_for_step(step.id)
        assert [result.tool_call_id for result in results] == [calls[1].id, calls[0].id]
        assert await uow.context_manifests.get_for_step(step.id) == manifest


async def test_duplicate_terminal_result_is_rejected(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "duplicate")
        call = ToolCall(
            new_id(),
            task.id,
            run.id,
            step.id,
            message.id,
            1,
            "read_file",
            {"path": "x"},
            1,
            None,
            ToolExecutionMode.PARALLEL,
            NOW,
        )
        await uow.tool_calls.add(call)
        result = ToolResult(
            new_id(),
            task.id,
            run.id,
            step.id,
            call.id,
            ToolResultStatus.SUCCEEDED,
            {"ok": True},
            1,
            "ok",
            None,
            1,
            NOW,
            NOW,
        )
        await uow.tool_results.add(result)
        with pytest.raises(IntegrityError):
            await uow.tool_results.add(
                ToolResult(
                    new_id(),
                    task.id,
                    run.id,
                    step.id,
                    call.id,
                    ToolResultStatus.FAILED,
                    {},
                    1,
                    "failed",
                    "duplicate",
                    2,
                    NOW,
                    NOW,
                )
            )
