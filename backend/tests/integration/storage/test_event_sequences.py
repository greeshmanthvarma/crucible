from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository
from crucible.domain.run import Run
from crucible.domain.task import Task
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork

NOW = datetime(2026, 9, 20, tzinfo=UTC)


async def seed_task(uow: SqlAlchemyUnitOfWork, suffix: str) -> Task:
    repository = Repository(new_id(), Path(f"/repo/{suffix}"), NOW)
    await uow.repositories.add(repository)
    task = Task.provisioning(
        task_id=new_id(),
        repository_id=repository.id,
        source_ref="main",
        base_revision="a" * 40,
        workspace_path=Path(f"/work/{suffix}"),
        clock=type("Clock", (), {"now": lambda self: NOW})(),
    )
    await uow.tasks.add(task)
    return task


async def test_event_sequences_and_run_claim_are_atomic(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task = await seed_task(uow, "sequence")
        run = Run.queued(
            run_id=new_id(), task_id=task.id, triggering_message_id=None, created_at=NOW
        )
        await uow.runs.add(run)
        events = []
        for event_type in (EventType.RUN_QUEUED, EventType.RUN_STARTED):
            events.append(
                await uow.events.append(
                    Event(
                        new_id(),
                        task.id,
                        run.id,
                        0,
                        0,
                        event_type,
                        1,
                        {"schema_version": 1},
                        NOW,
                    )
                )
            )

        future = NOW + timedelta(minutes=1)
        assert await uow.runs.claim_queued(run.id, new_id(), NOW, future)
        assert not await uow.runs.claim_queued(run.id, new_id(), NOW, future)

    assert [(event.task_sequence, event.run_sequence) for event in events] == [
        (1, 1),
        (2, 2),
    ]


async def test_cross_task_run_event_is_rejected(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        first, second = await seed_task(uow, "first"), await seed_task(uow, "second")
        run = Run.queued(
            run_id=new_id(),
            task_id=first.id,
            triggering_message_id=None,
            created_at=NOW,
        )
        await uow.runs.add(run)
        event = Event(
            new_id(),
            second.id,
            run.id,
            0,
            0,
            EventType.RUN_QUEUED,
            1,
            {"schema_version": 1},
            datetime(2026, 9, 20, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="same Task"):
            await uow.events.append(event)
