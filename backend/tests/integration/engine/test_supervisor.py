import asyncio
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.storage import models
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_supervisor_deduplicates_submission_and_reconciles(
    database: Database, tmp_path: Path
) -> None:
    first = await queued_run(database, tmp_path, "first")
    await queued_run(database, tmp_path, "second")
    barrier = asyncio.Event()
    gateway = FakeModelGateway(barrier=barrier)
    factory = uow_factory(database)
    supervisor = LocalRunSupervisor(
        RunEngine(factory, FixedClock(), gateway), factory, FixedClock()
    )

    await supervisor.submit(first.run_id)
    await supervisor.submit(first.run_id)
    await supervisor.reconcile()
    for _ in range(100):
        if gateway.requests:
            break
        await asyncio.sleep(0.01)
    assert len(gateway.requests) == 1
    barrier.set()
    await supervisor.close()

    async with database.engine.connect() as connection:
        statuses = (
            (
                await connection.execute(
                    select(models.runs.c.status).order_by(models.runs.c.created_at)
                )
            )
            .scalars()
            .all()
        )
    assert statuses == ["completed", "completed"]
    assert len(gateway.requests) == 2


async def test_reconcile_interrupts_expired_running_run_without_submission(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    clock = FixedClock()
    async with factory() as uow:
        assert await uow.runs.claim_queued(
            submitted.run_id,
            uuid4(),
            clock.now() - timedelta(minutes=10),
            clock.now() - timedelta(minutes=5),
        )
        await uow.commit()
    gateway = FakeModelGateway()
    supervisor = LocalRunSupervisor(RunEngine(factory, clock, gateway), factory, clock)

    await supervisor.reconcile()
    await supervisor.close()

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
        event_type = await connection.scalar(
            select(models.task_events.c.type)
            .where(models.task_events.c.run_id == str(submitted.run_id))
            .order_by(models.task_events.c.run_sequence.desc())
            .limit(1)
        )
    assert run["status"] == "interrupted"
    assert run["outcome_code"] == "stale_run"
    assert event_type == "run.interrupted"
    assert gateway.requests == []
