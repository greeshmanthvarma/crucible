import asyncio
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from crucible.application.repository_service import RepositoryService
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork, UnitOfWork
from crucible.workspaces.git import SubprocessGitClient


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


async def test_registration_deduplicates_canonical_repository_root(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    nested = root / "nested"
    nested.mkdir()
    service = RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    )

    first = await service.register(nested)
    second = await service.register(root)

    assert first.created
    assert not second.created
    assert second.repository.id == first.repository.id
    assert second.repository.root_path == root.resolve()
    assert await service.list() == (second,)


async def test_concurrent_alias_registration_returns_one_stable_repository(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    nested = root / "nested"
    nested.mkdir()
    service = RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    )

    first, second = await asyncio.gather(
        service.register(root), service.register(nested)
    )

    assert {first.created, second.created} == {True, False}
    assert first.repository.id == second.repository.id
