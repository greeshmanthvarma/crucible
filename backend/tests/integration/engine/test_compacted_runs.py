from pathlib import Path

import pytest

from crucible.application.artifact_service import ArtifactService
from crucible.application.message_service import MessageService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.compaction import CompactionLifecycle, ScriptedCompactionGateway
from crucible.context.manager import (
    ContextLimitExceeded,
    ContextManager,
    SimpleTokenEstimator,
)
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.storage.database import Database
from tests.integration.context.test_compaction_lifecycle import (
    completed_messages,
    summary,
)
from tests.integration.engine.conftest import (
    FixedClock,
    PassiveSupervisor,
    queued_run,
    uow_factory,
)


async def test_prepared_context_inherits_latest_compaction_without_deleting_history(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "compacted-context")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    original = await completed_messages(factory, run, "x" * 1000)
    lifecycle = CompactionLifecycle(
        factory,
        ArtifactService(
            LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
        ),
        ScriptedCompactionGateway((summary("preserved objective"),)),
        FixedClock(),
    )
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=200,
        output_reserve=10,
        threshold=0.8,
        recent_complete_units=0,
        compaction_lifecycle=lifecycle,
    )
    async with factory() as uow:
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()

    prepared = await manager.prepare(run, step)

    texts = [
        part.text_content
        for message in prepared.request.messages
        for part in message.parts
    ]
    assert any(text and "preserved objective" in text for text in texts)
    assert prepared.manifest.compaction_id is not None
    async with factory() as uow:
        assert await uow.messages.list_for_task(run.task_id) == original
        current = await uow.runs.get(run.id)
        assert current is not None
        completed = current.claim(
            execution_id=new_id(),
            now=FixedClock().now(),
            lease_expires_at=FixedClock().now(),
        ).complete(now=FixedClock().now())
        await uow.runs.update(completed)
        await uow.commit()
    next_submission = await MessageService(
        factory, FixedClock(), PassiveSupervisor()
    ).submit(run.task_id, "continue with the retained context", "next-run-key")
    async with factory() as uow:
        next_run = await uow.runs.get(next_submission.run_id)
        assert next_run is not None
        next_step = Step.preparing(
            new_id(), next_run.task_id, next_run.id, 1, FixedClock().now()
        )
        await uow.steps.add(next_step)
        await uow.commit()

    inherited = await manager.prepare(next_run, next_step)

    assert inherited.manifest.compaction_id == prepared.manifest.compaction_id


@pytest.mark.parametrize(("input_limit", "expect_failure"), ((600, False), (200, True)))
async def test_exhausted_compaction_uses_only_a_still_fitting_full_context(
    database: Database,
    tmp_path: Path,
    input_limit: int,
    expect_failure: bool,
) -> None:
    submitted = await queued_run(
        database, tmp_path, f"compaction-failure-{input_limit}"
    )
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    await completed_messages(factory, run, "x" * 1000)
    lifecycle = CompactionLifecycle(
        factory,
        ArtifactService(
            LocalArtifactStore(tmp_path / f"artifacts-{input_limit}", FixedClock()),
            factory,
        ),
        ScriptedCompactionGateway((RuntimeError("one"), RuntimeError("two"))),
        FixedClock(),
        attempt_limit=2,
    )
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=input_limit,
        output_reserve=10,
        threshold=0.4,
        recent_complete_units=0,
        compaction_lifecycle=lifecycle,
    )
    async with factory() as uow:
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()

    if expect_failure:
        with pytest.raises(ContextLimitExceeded):
            await manager.prepare(run, step)
    else:
        prepared = await manager.prepare(run, step)
        assert prepared.manifest.compaction_id is None
