from pathlib import Path

import pytest

from crucible.context.manager import (
    CompactionNeeded,
    ContextLimitExceeded,
    ContextManager,
    SimpleTokenEstimator,
)
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.engine.gateway import ModelMessage
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_context_over_threshold_requests_a_safe_compaction_boundary(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "compaction-trigger")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                step.id,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (MessagePart(new_id(), 1, MessagePartKind.TEXT, "done"),),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.commit()
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="p" * 100,
        tool_contract="t",
        model="fixture",
        input_limit=20,
        output_reserve=1,
        threshold=0.8,
        recent_complete_units=0,
    )
    with pytest.raises(CompactionNeeded) as raised:
        await manager.prepare(run, step)
    assert raised.value.boundary.source_start_sequence == 1


@pytest.mark.parametrize("estimate", [80, 90, 91])
async def test_no_boundary_uses_hard_limit_not_compaction_threshold(
    database: Database, tmp_path: Path, estimate: int
) -> None:
    class FixedEstimator:
        def estimate(self, messages: tuple[ModelMessage, ...]) -> int:
            return estimate

    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    manager = ContextManager(
        factory,
        FixedClock(),
        FixedEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=100,
        output_reserve=10,
        threshold=0.8,
    )
    if estimate > 90:
        with pytest.raises(ContextLimitExceeded):
            await manager.prepare(run, step)
    else:
        prepared = await manager.prepare(run, step)
        assert prepared.manifest.estimated_tokens == estimate


@pytest.mark.parametrize("batch_size", [1, 16])
async def test_oversized_indivisible_tool_batch_fails_before_model_dispatch(
    database, tmp_path, batch_size
):
    from crucible.domain.tools import (
        ToolCall,
        ToolExecutionMode,
        ToolResult,
        ToolResultStatus,
    )

    submitted = await queued_run(database, tmp_path, "oversized-batch")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        assistant_id = new_id()
        calls = [new_id() for _ in range(batch_size)]
        await uow.messages.add(
            Message(
                assistant_id,
                run.task_id,
                run.id,
                step.id,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                tuple(
                    MessagePart(
                        new_id(),
                        i + 1,
                        MessagePartKind.TOOL_CALL,
                        None,
                        tool_call_id=call,
                    )
                    for i, call in enumerate(calls)
                ),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        for i, call in enumerate(calls):
            await uow.tool_calls.add(
                ToolCall(
                    call,
                    run.task_id,
                    run.id,
                    step.id,
                    assistant_id,
                    i + 1,
                    "read_file",
                    {"path": f"file-{i}.txt"},
                    1,
                    f"provider-{i}",
                    ToolExecutionMode.PARALLEL,
                    FixedClock().now(),
                )
            )
            result = ToolResult(
                new_id(),
                run.task_id,
                run.id,
                step.id,
                call,
                ToolResultStatus.SUCCEEDED,
                {"text": "x" * (131072 // batch_size)},
                1,
                "bounded read",
                None,
                i + 1,
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
                            "x" * (131072 // batch_size),
                            tool_result_id=result.id,
                        ),
                    ),
                    FixedClock().now(),
                    FixedClock().now(),
                )
            )
        await uow.commit()
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="p",
        tool_contract="t",
        model="fixture",
        input_limit=10000,
        output_reserve=100,
        recent_complete_units=2,
    )
    with pytest.raises(ContextLimitExceeded):
        await manager.prepare(run, step)
    # Every result remains linked and durable; no partial unit was silently dropped.
    async with factory() as uow:
        assert len(await uow.tool_results.list_for_step(step.id)) == batch_size
        assert await uow.compactions.latest_for_task(run.task_id) is None
