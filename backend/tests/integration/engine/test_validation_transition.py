from dataclasses import replace
from pathlib import Path

from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.repository import RepositorySettings
from crucible.domain.validation import ValidationStatus
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_model_completion_creates_proposal_and_explicit_no_validation_attempt(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "validation-none")
    factory = uow_factory(database)

    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(
        submitted.run_id
    )

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        proposal = await uow.completion_proposals.get_for_run(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
        events = await uow.events.list_after(run.task_id, 0) if run else ()
    assert run is not None and run.status.value == "completed"
    assert proposal is not None and proposal.summary.startswith("Fake response")
    assert attempts[-1].status is ValidationStatus.NOT_CONFIGURED
    assert "validation.completed" in [event.type for event in events]


async def test_model_text_cannot_override_failed_authoritative_validation(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "validation-authority")
    factory = uow_factory(database)
    spec = CommandSpec(
        "python",
        ("-V",),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        "runner@sha256:" + "a" * 64,
        "authoritative",
        CommandLimits(1, 1024**3, 64, 100_000),
    )
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        await uow.runs.update(
            replace(
                run, settings_snapshot=RepositorySettings(validation_commands=(spec,))
            )
        )
        await uow.commit()

    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(
        submitted.run_id
    )

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.outcome_code == "validation_failed"
    assert attempts[-1].status is ValidationStatus.FAILED
