from pathlib import Path

from crucible.application.message_service import MessageService, MessageSubmissionKind
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.storage.database import Database
from tests.integration.engine.conftest import (
    FixedClock,
    PassiveSupervisor,
    queued_run,
    uow_factory,
)


async def test_steering_enters_only_the_next_prepared_model_boundary(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "steering-boundary")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        first = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(first)
        await uow.commit()

    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=10_000,
        output_reserve=500,
    )
    before = await manager.prepare(run, first)
    steering = await MessageService(factory, FixedClock(), PassiveSupervisor()).submit(
        run.task_id, "Use the smaller interface", "steering-key"
    )
    assert steering.kind is MessageSubmissionKind.STEERING

    async with factory() as uow:
        second = Step.preparing(new_id(), run.task_id, run.id, 2, FixedClock().now())
        await uow.steps.add(second)
        await uow.commit()
    after = await manager.prepare(run, second)

    assert "Use the smaller interface" not in [
        part.text_content
        for message in before.request.messages
        for part in message.parts
    ]
    assert "Use the smaller interface" in [
        part.text_content
        for message in after.request.messages
        for part in message.parts
    ]
    assert before.manifest.message_ids != after.manifest.message_ids
