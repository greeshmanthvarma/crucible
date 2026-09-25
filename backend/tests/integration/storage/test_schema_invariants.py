from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository
from crucible.domain.run import Run
from crucible.domain.task import Task
from crucible.storage import models
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


async def test_duplicate_repository_root_is_rejected(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        await uow.repositories.add(Repository(new_id(), Path("/repo"), NOW))
        with pytest.raises(IntegrityError):
            await uow.repositories.add(Repository(new_id(), Path("/repo"), NOW))


async def test_cross_task_message_is_rejected_before_flush(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        first, second = await seed_task(uow, "first"), await seed_task(uow, "second")
        run = Run.queued(
            run_id=new_id(),
            task_id=first.id,
            triggering_message_id=None,
            created_at=NOW,
        )
        await uow.runs.add(run)
        message = Message(
            new_id(),
            second.id,
            run.id,
            None,
            0,
            MessageRole.USER,
            MessageStatus.COMPLETED,
            (MessagePart(new_id(), 1, MessagePartKind.TEXT, "hello"),),
            NOW,
            NOW,
        )
        with pytest.raises(ValueError, match="same Task"):
            await uow.messages.add(message)


async def test_cross_task_triggering_message_is_rejected(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        first, second = await seed_task(uow, "first"), await seed_task(uow, "second")
        run = Run.queued(
            run_id=new_id(),
            task_id=first.id,
            triggering_message_id=None,
            created_at=NOW,
        )
        await uow.runs.add(run)
        message = Message(
            new_id(),
            second.id,
            None,
            None,
            0,
            MessageRole.USER,
            MessageStatus.COMPLETED,
            (MessagePart(new_id(), 1, MessagePartKind.TEXT, "hello"),),
            NOW,
            NOW,
        )
        stored = await uow.messages.add(message)

        with pytest.raises(ValueError, match="same Task"):
            await uow.runs.set_triggering_message(run.id, stored.id)

        corrupt_run = Run.queued(
            run_id=new_id(),
            task_id=first.id,
            triggering_message_id=stored.id,
            created_at=NOW,
        )
        with pytest.raises(ValueError, match="same Task"):
            await uow.runs.add(corrupt_run)


async def test_repository_timestamp_round_trip_remains_utc(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        repository = Repository(new_id(), Path("/repo/utc"), NOW)
        await uow.repositories.add(repository)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(database) as uow:
        stored = await uow.repositories.get_by_root(repository.root_path)

    assert stored is not None
    assert stored.created_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("table", "values"),
    [
        (
            models.messages,
            {
                "id": "m2",
                "task_id": "t",
                "conversation_sequence": 1,
                "role": "user",
                "status": "completed",
                "created_at": NOW,
                "completed_at": NOW,
            },
        ),
        (
            models.message_parts,
            {
                "id": "p2",
                "message_id": "m",
                "part_sequence": 1,
                "kind": "text",
                "text_content": "two",
            },
        ),
        (
            models.task_events,
            {
                "id": "e2",
                "task_id": "t",
                "task_sequence": 1,
                "run_id": None,
                "run_sequence": None,
                "type": "run.queued",
                "schema_version": 1,
                "payload_json": {"schema_version": 1},
                "created_at": NOW,
            },
        ),
        (
            models.task_events,
            {
                "id": "e2",
                "task_id": "t",
                "task_sequence": 2,
                "run_id": "r",
                "run_sequence": 1,
                "type": "run.queued",
                "schema_version": 1,
                "payload_json": {"schema_version": 1},
                "created_at": NOW,
            },
        ),
        (
            models.idempotency_records,
            {
                "id": "i2",
                "scope": "message:t",
                "key": "key",
                "request_hash": "hash",
                "response_status": 202,
                "response_json": {},
                "created_at": NOW,
            },
        ),
    ],
)
async def test_compound_uniqueness_is_enforced(
    database: Database, table: object, values: dict[str, object]
) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(
            insert(models.repositories).values(
                id="repo", root_path="/repo", created_at=NOW
            )
        )
        await connection.execute(
            insert(models.tasks).values(
                id="t",
                repository_id="repo",
                source_ref="main",
                base_revision="a",
                workspace_path="/work",
                status="active",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        await connection.execute(
            insert(models.runs).values(
                id="r", task_id="t", status="queued", created_at=NOW
            )
        )
        await connection.execute(
            insert(models.messages).values(
                id="m",
                task_id="t",
                conversation_sequence=1,
                role="user",
                status="completed",
                created_at=NOW,
                completed_at=NOW,
            )
        )
        await connection.execute(
            insert(models.message_parts).values(
                id="p", message_id="m", part_sequence=1, kind="text", text_content="one"
            )
        )
        await connection.execute(
            insert(models.task_events).values(
                id="e",
                task_id="t",
                task_sequence=1,
                run_id="r",
                run_sequence=1,
                type="run.queued",
                schema_version=1,
                payload_json={"schema_version": 1},
                created_at=NOW,
            )
        )
        await connection.execute(
            insert(models.idempotency_records).values(
                id="i",
                scope="message:t",
                key="key",
                request_hash="hash",
                response_status=202,
                response_json={},
                created_at=NOW,
            )
        )
        with pytest.raises(IntegrityError):
            await connection.execute(insert(table).values(**values))
