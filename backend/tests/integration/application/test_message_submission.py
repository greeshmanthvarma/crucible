import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import func, select

from crucible.application.errors import IdempotencyConflict
from crucible.application.message_service import MessageService
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.storage import models
from crucible.storage.database import Database
from crucible.storage.repositories import IdempotencyRepository
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 20, tzinfo=UTC)


class RecordingSupervisor:
    def __init__(self) -> None:
        self.submissions: list[UUID] = []

    async def submit(self, run_id: UUID) -> None:
        self.submissions.append(run_id)

    async def reconcile(self) -> None:
        return None


def uow_factory(database: Database):
    def create() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(database)

    return create


def create_repository(path: Path) -> None:
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=path, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=path, check=True)
    (path / "README.md").write_text("fixture\n")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


async def active_task(database: Database, tmp_path: Path, name: str):
    root = tmp_path / name
    create_repository(root)
    factory = uow_factory(database)
    git = SubprocessGitClient()
    registered = await RepositoryService(git, factory, FixedClock()).register(root)
    return await TaskService(
        WorkspaceManager(git, tmp_path / "data"), factory, FixedClock()
    ).create(registered.repository.id, "HEAD", "message-submission-task")


async def test_submission_is_atomic_ordered_and_idempotent(
    database: Database, tmp_path: Path
) -> None:
    task = await active_task(database, tmp_path, "repository")
    supervisor = RecordingSupervisor()
    service = MessageService(uow_factory(database), FixedClock(), supervisor)

    submitted = await service.submit(task.id, "Please inspect this", "request-1")
    retried = await service.submit(task.id, "Please inspect this", "request-1")

    assert retried == submitted
    assert supervisor.submissions == [submitted.run_id]
    messages = await service.list(task.id)
    assert len(messages) == 1
    assert messages[0].conversation_sequence == 1
    assert messages[0].parts[0].part_sequence == 1
    assert messages[0].parts[0].text_content == "Please inspect this"

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
        event = (
            (
                await connection.execute(
                    select(models.task_events).where(
                        models.task_events.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
        idempotency_count = await connection.scalar(
            select(func.count())
            .select_from(models.idempotency_records)
            .where(
                models.idempotency_records.c.scope
                == f"POST:/api/tasks/{task.id}/messages"
            )
        )
    assert run["triggering_message_id"] == str(submitted.message_id)
    assert run["status"] == "queued"
    assert event["type"] == "run.queued"
    assert event["task_sequence"] == 3
    assert event["run_sequence"] == 1
    assert idempotency_count == 1

    with pytest.raises(IdempotencyConflict):
        await service.submit(task.id, "Different text", "request-1")


async def test_same_key_is_independent_per_task_and_failure_rolls_back(
    database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = await active_task(database, tmp_path, "first")
    second = await active_task(database, tmp_path, "second")
    service = MessageService(uow_factory(database), FixedClock(), RecordingSupervisor())
    await service.submit(first.id, "hello", "shared-key")
    await service.submit(second.id, "hello", "shared-key")

    tables = (
        models.messages,
        models.message_parts,
        models.runs,
        models.task_events,
        models.idempotency_records,
    )
    async with database.engine.connect() as connection:
        before = {
            table.name: await connection.scalar(select(func.count()).select_from(table))
            for table in tables
        }

    async def fail_add(self: IdempotencyRepository, record: object) -> None:
        raise RuntimeError("injected failure before idempotency persistence")

    monkeypatch.setattr(IdempotencyRepository, "add", fail_add)
    with pytest.raises(RuntimeError, match="injected failure"):
        await service.submit(first.id, "rollback me", "failure-key")

    async with database.engine.connect() as connection:
        after = {
            table.name: await connection.scalar(select(func.count()).select_from(table))
            for table in tables
        }
    assert after == before
