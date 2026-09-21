from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from crucible.application.errors import (
    EventCursorNotFound,
    EventCursorTaskMismatch,
)
from crucible.application.event_service import TaskEventSource
from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id
from crucible.engine.notifier import TaskEventNotifier
from crucible.storage import models
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def task_id_for_run(database: Database, run_id: UUID) -> UUID:
    async with database.engine.connect() as connection:
        value = await connection.scalar(
            select(models.runs.c.task_id).where(models.runs.c.id == str(run_id))
        )
    assert value is not None
    return UUID(value)


async def test_event_replay_is_ordered_and_validates_cursor_ownership(
    database: Database, tmp_path: Path
) -> None:
    first = await queued_run(database, tmp_path, "first")
    second = await queued_run(database, tmp_path, "second")
    first_task_id = await task_id_for_run(database, first.run_id)
    second_task_id = await task_id_for_run(database, second.run_id)
    source = TaskEventSource(uow_factory(database), TaskEventNotifier())
    async with uow_factory(database)() as uow:
        for event_type in (EventType.RUN_STARTED, EventType.RUN_FAILED):
            await uow.events.append(
                Event(
                    id=new_id(),
                    task_id=first_task_id,
                    run_id=first.run_id,
                    task_sequence=0,
                    run_sequence=0,
                    type=event_type,
                    schema_version=1,
                    payload={"schema_version": 1},
                    created_at=FixedClock().now(),
                )
            )
        await uow.commit()

    all_events = await source.list_after(first_task_id, 0)
    after_second = await source.list_after(first_task_id, all_events[1].task_sequence)
    after_last = await source.list_after(first_task_id, all_events[-1].task_sequence)

    assert [event.task_sequence for event in all_events] == [1, 2, 3, 4, 5]
    assert [event.task_sequence for event in after_second] == [3, 4, 5]
    assert after_last == ()
    assert await source.resolve_cursor(first_task_id, all_events[1].id) == 2
    with pytest.raises(EventCursorNotFound):
        await source.resolve_cursor(first_task_id, uuid4())
    second_events = await source.list_after(second_task_id, 0)
    with pytest.raises(EventCursorTaskMismatch):
        await source.resolve_cursor(first_task_id, second_events[0].id)
