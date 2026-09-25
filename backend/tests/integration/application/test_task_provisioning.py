import asyncio
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from crucible.application.errors import RevisionNotFound, WorkspaceProvisioningFailed
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 20, tzinfo=UTC)


def uow_factory(database: Database):
    def create() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(database)

    return create


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def create_repository(path: Path) -> None:
    path.mkdir()
    git("init", "-q", cwd=path)
    git("config", "user.email", "test@example.com", cwd=path)
    git("config", "user.name", "Test User", cwd=path)
    (path / "tracked.txt").write_text("original\n")
    git("add", "tracked.txt", cwd=path)
    git("commit", "-qm", "fixture", cwd=path)


async def test_task_provisioning_keeps_registered_checkout_unchanged(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    original_head = git("rev-parse", "HEAD", cwd=root)
    original_status = git("status", "--porcelain=v1", cwd=root)
    factory = uow_factory(database)
    git_client = SubprocessGitClient()
    registered = await RepositoryService(git_client, factory, FixedClock()).register(
        root
    )
    service = TaskService(
        WorkspaceManager(git_client, tmp_path / "data"), factory, FixedClock()
    )

    task = await service.create(registered.repository.id, "HEAD", "create-task-1")
    (task.workspace_path / "tracked.txt").write_text("changed\n")

    assert task.base_revision == original_head
    assert git("status", "--porcelain=v1", cwd=root) == original_status
    assert (root / "tracked.txt").read_text() == "original\n"
    assert git("rev-parse", "HEAD", cwd=root) == original_head

    changed = replace(task, base_revision="b" * 40)
    async with factory() as uow:
        await uow.tasks.update(changed)
        await uow.commit()
    assert (await service.get(task.id)).base_revision == original_head


async def test_unresolved_ref_evidence_blocks_lossy_downgrade(
    database: Database, database_url: str, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    factory = uow_factory(database)
    git_client = SubprocessGitClient()
    registered = await RepositoryService(git_client, factory, FixedClock()).register(
        root
    )
    service = TaskService(
        WorkspaceManager(git_client, tmp_path / "data"), factory, FixedClock()
    )
    with pytest.raises(RevisionNotFound):
        await service.create(registered.repository.id, "missing", "invalid-ref")
    await database.dispose()
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)

    with pytest.raises(RuntimeError, match="Task evidence exists"):
        await asyncio.to_thread(command.downgrade, config, "0001_run_spine")


async def test_retry_after_provisioning_failure_returns_original_failed_task(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    factory = uow_factory(database)
    git_client = SubprocessGitClient()
    registered = await RepositoryService(git_client, factory, FixedClock()).register(
        root
    )
    manager = WorkspaceManager(git_client, tmp_path / "data")
    service = TaskService(manager, factory, FixedClock())

    original_create = manager.create
    calls = 0

    async def fail_once(plan):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise WorkspaceProvisioningFailed("injected provisioning failure")
        return await original_create(plan)

    manager.create = fail_once  # type: ignore[method-assign]

    with pytest.raises(WorkspaceProvisioningFailed):
        await service.create(registered.repository.id, "HEAD", "retry-key")
    retried = await service.create(registered.repository.id, "HEAD", "retry-key")

    assert retried.status == "provisioning_failed"
    assert retried.failure_code == "workspace_provisioning_failed"
    assert calls == 1
