from pathlib import Path

import pytest

from crucible.context import selection
from crucible.context.invariants import ContextInvariantError
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


@pytest.mark.parametrize("fault", ["instructions", "orphan", "order"])
async def test_selection_cannot_drop_required_instructions_or_orphan_results(
    database: Database, tmp_path: Path, monkeypatch, fault: str
):
    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    original = selection.assemble_visible_messages

    def broken(*args, **kwargs):
        messages = original(*args, **kwargs)
        if fault == "instructions":
            return messages[1:]
        if fault == "order":
            return messages + messages[-1:]
        return messages + (
            ModelMessage(
                ModelRole.TOOL, (ModelPart("tool_result", tool_call_id=new_id()),)
            ),
        )

    monkeypatch.setattr(selection, "assemble_visible_messages", broken)
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="contract",
        model="fixture",
        input_limit=10_000,
        output_reserve=500,
    )
    gateway_calls = []
    with pytest.raises(ContextInvariantError):
        prepared = await manager.prepare(run, step)
        gateway_calls.append(prepared.request)
    assert gateway_calls == []
    async with factory() as uow:
        assert await uow.context_manifests.get_for_step(step.id) is None


async def test_selection_cannot_leave_a_known_result_without_its_call(
    database: Database, tmp_path: Path, monkeypatch
):
    from crucible.domain.conversation import (
        Message,
        MessagePart,
        MessagePartKind,
        MessageRole,
        MessageStatus,
    )
    from crucible.domain.tools import (
        ToolCall,
        ToolExecutionMode,
        ToolResult,
        ToolResultStatus,
    )

    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        call_id = new_id()
        assistant = await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                step.id,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (
                    MessagePart(
                        new_id(),
                        1,
                        MessagePartKind.TOOL_CALL,
                        None,
                        tool_call_id=call_id,
                    ),
                ),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        call = ToolCall(
            call_id,
            run.task_id,
            run.id,
            step.id,
            assistant.id,
            1,
            "read_file",
            {"path": "x"},
            1,
            "provider-id",
            ToolExecutionMode.SEQUENTIAL,
            FixedClock().now(),
        )
        await uow.tool_calls.add(call)
        result = ToolResult(
            new_id(),
            run.task_id,
            run.id,
            step.id,
            call.id,
            ToolResultStatus.SUCCEEDED,
            {},
            1,
            "contents",
            None,
            1,
            FixedClock().now(),
            FixedClock().now(),
        )
        await uow.tool_results.add(result)
        await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                step.id,
                0,
                MessageRole.TOOL,
                MessageStatus.COMPLETED,
                (
                    MessagePart(
                        new_id(),
                        1,
                        MessagePartKind.TOOL_RESULT,
                        "contents",
                        tool_result_id=result.id,
                    ),
                ),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.commit()
    original = selection.assemble_visible_messages

    def orphan(*args, **kwargs):
        return tuple(
            message
            for message in original(*args, **kwargs)
            if not any(part.kind == "tool_call" for part in message.parts)
        )

    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="contract",
        model="fixture",
        input_limit=10_000,
        output_reserve=500,
    )
    # A complete exchange remains valid before fault injection.
    await manager.prepare(run, step)
    monkeypatch.setattr(selection, "assemble_visible_messages", orphan)
    with pytest.raises(ContextInvariantError, match="Orphan"):
        await manager.prepare(run, step)


async def test_forged_compaction_boundary_cannot_consume_incomplete_unit(
    database: Database, tmp_path: Path, monkeypatch
):
    from crucible.context.selection import CompactionBoundary, ConversationUnit

    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        source = await uow.messages.list_for_task(run.task_id)
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    boundary = CompactionBoundary(
        source[0].conversation_sequence,
        source[-1].conversation_sequence,
        source[-1].conversation_sequence + 1,
        (ConversationUnit(source, True),),
    )
    monkeypatch.setattr(
        selection, "select_compaction_boundary", lambda *args, **kwargs: boundary
    )
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy" * 100,
        tool_contract="contract",
        model="fixture",
        input_limit=20,
        output_reserve=1,
    )
    with pytest.raises(ContextInvariantError, match="incomplete"):
        await manager.prepare(run, step)
