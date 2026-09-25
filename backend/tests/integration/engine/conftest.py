import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from crucible.application.message_service import MessageService, SubmittedRun
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


class PassiveSupervisor:
    async def submit(self, run_id: UUID) -> None:
        return None

    async def reconcile(self) -> None:
        return None


def uow_factory(database: Database):
    def create() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(database)

    return create


async def queued_run(
    database: Database,
    tmp_path: Path,
    name: str = "repository",
    instructions: str | None = None,
) -> SubmittedRun:
    root = tmp_path / name
    root.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=root, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=root, check=True)
    (root / "README.md").write_text("fixture\n")
    tracked = ["README.md"]
    if instructions is not None:
        (root / "AGENTS.md").write_text(instructions)
        tracked.append("AGENTS.md")
    subprocess.run(["git", "add", *tracked], cwd=root, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=root, check=True)
    factory = uow_factory(database)
    git = SubprocessGitClient()
    repository = await RepositoryService(git, factory, FixedClock()).register(root)
    task = await TaskService(
        WorkspaceManager(git, tmp_path / "data"), factory, FixedClock()
    ).create(repository.repository.id, "HEAD", f"engine-task-{name}")
    return await MessageService(factory, FixedClock(), PassiveSupervisor()).submit(
        task.id, "Explain the change", f"key-{name}"
    )
