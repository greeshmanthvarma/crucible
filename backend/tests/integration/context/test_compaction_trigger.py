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
