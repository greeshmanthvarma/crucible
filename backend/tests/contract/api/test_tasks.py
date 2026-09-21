import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from crucible.api.app import create_app
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.storage import models
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


def services(database: Database, data_dir: Path):
    factory = uow_factory(database)
    git = SubprocessGitClient()
    return (
        RepositoryService(git, factory, FixedClock()),
        TaskService(WorkspaceManager(git, data_dir), factory, FixedClock()),
    )


async def test_task_create_and_get_contract(database: Database, tmp_path: Path) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    repositories, tasks = services(database, tmp_path / "data")
    registered = await repositories.register(root)
    app = create_app(repositories, tasks)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        created = await client.post(
            f"/api/repositories/{registered.repository.id}/tasks",
            json={"sourceRef": "HEAD"},
        )
        fetched = await client.get(f"/api/tasks/{created.json()['id']}")

    assert created.status_code == 201
    assert created.json()["status"] == "active"
    assert created.json()["baseRevision"] == registered.head_revision
    assert fetched.status_code == 200
    assert fetched.json() == created.json()


async def test_task_errors_and_unresolved_ref_evidence(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    repositories, tasks = services(database, tmp_path / "data")
    registered = await repositories.register(root)
    app = create_app(repositories, tasks)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        unknown = await client.post(
            f"/api/repositories/{uuid4()}/tasks", json={"sourceRef": "HEAD"}
        )
        invalid = await client.post(
            f"/api/repositories/{registered.repository.id}/tasks",
            json={"sourceRef": "missing"},
        )

    async with database.engine.connect() as connection:
        failed = (
            (
                await connection.execute(
                    select(models.tasks).where(
                        models.tasks.c.status == "provisioning_failed"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert unknown.status_code == 404
    assert unknown.json()["code"] == "repository_not_found"
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "revision_not_found"
    assert failed["base_revision"] is None
    assert failed["failure_code"] == "revision_not_found"

    with pytest.raises(IntegrityError):
        async with database.engine.begin() as connection:
            await connection.execute(
                insert(models.tasks).values(
                    id=str(uuid4()),
                    repository_id=str(registered.repository.id),
                    source_ref="missing-again",
                    base_revision=None,
                    workspace_path=str(tmp_path / "invalid-workspace"),
                    status="provisioning_failed",
                    failure_code=None,
                    failure_detail="invalid",
                    created_at=datetime(2026, 9, 20, tzinfo=UTC),
                    updated_at=datetime(2026, 9, 20, tzinfo=UTC),
                )
            )
