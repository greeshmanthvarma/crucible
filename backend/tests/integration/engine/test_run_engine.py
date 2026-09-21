from pathlib import Path

from sqlalchemy import select

from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.storage import models
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_engine_claims_and_completes_a_run_once(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    gateway = FakeModelGateway()
    engine = RunEngine(uow_factory(database), FixedClock(), gateway)

    assert await engine.execute(submitted.run_id) is True
    assert await engine.execute(submitted.run_id) is False

    async with database.engine.connect() as connection:
        run = (
            (
                await connection.execute(
                    select(models.runs).where(models.runs.c.id == str(submitted.run_id))
                )
            )
            .mappings()
            .one()
        )
        messages = (
            (
                await connection.execute(
                    select(models.messages)
                    .where(models.messages.c.task_id == run["task_id"])
                    .order_by(models.messages.c.conversation_sequence)
                )
            )
            .mappings()
            .all()
        )
        events = (
            (
                await connection.execute(
                    select(models.task_events)
                    .where(models.task_events.c.task_id == run["task_id"])
                    .order_by(models.task_events.c.task_sequence)
                )
            )
            .mappings()
            .all()
        )

    assert run["status"] == "completed"
    assert run["execution_id"] is not None
    assert run["lease_expires_at"] is None
    assert [message["conversation_sequence"] for message in messages] == [1, 2]
    assert [message["role"] for message in messages] == ["user", "assistant"]
    assert [event["type"] for event in events][-3:] == [
        "run.started",
        "message.completed",
        "run.completed",
    ]
    assert len(gateway.requests) == 1
    assert gateway.requests[0].messages[0].parts[0].text_content == "Explain the change"


async def test_gateway_failure_persists_stable_outcome(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    gateway = FakeModelGateway(error=RuntimeError("provider unavailable"))

    assert await RunEngine(uow_factory(database), FixedClock(), gateway).execute(
        submitted.run_id
    )

    async with database.engine.connect() as connection:
        run = (
            (
                await connection.execute(
                    select(models.runs).where(models.runs.c.id == str(submitted.run_id))
                )
            )
            .mappings()
            .one()
        )
        messages = (
            (
                await connection.execute(
                    select(models.messages).where(
                        models.messages.c.task_id == run["task_id"]
                    )
                )
            )
            .mappings()
            .all()
        )
        event_types = (
            (
                await connection.execute(
                    select(models.task_events.c.type)
                    .where(models.task_events.c.task_id == run["task_id"])
                    .order_by(models.task_events.c.task_sequence)
                )
            )
            .scalars()
            .all()
        )

    assert run["status"] == "failed"
    assert run["outcome_code"] == "model_gateway_error"
    assert len(messages) == 1
    assert event_types[-1] == "run.failed"
