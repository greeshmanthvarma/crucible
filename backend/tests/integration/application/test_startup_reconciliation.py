import subprocess
from pathlib import Path

from sqlalchemy import func, select

from crucible.application.reconciliation import StartupReconciler
from crucible.application.repository_service import RepositoryService
from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id
from crucible.domain.task import Task
from crucible.storage import models
from crucible.storage.database import Database
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager, WorkspacePlan
from tests.integration.engine.conftest import FixedClock, uow_factory


def create_repository(path: Path) -> None:
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=path, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=path, check=True)
    (path / "tracked.txt").write_text("original\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


async def seed_provisioning_task(
    database: Database, root: Path, data_dir: Path
) -> tuple[Task, WorkspaceManager]:
    factory = uow_factory(database)
    git = SubprocessGitClient()
    registered = await RepositoryService(git, factory, FixedClock()).register(root)
    manager = WorkspaceManager(git, data_dir)
    task_id = new_id()
    revision = await git.resolve_revision(root, "HEAD")
    task = Task.provisioning(
        task_id=task_id,
        repository_id=registered.repository.id,
        source_ref="HEAD",
        base_revision=revision,
        workspace_path=manager.destination_for(task_id),
        clock=FixedClock(),
    )
    async with factory() as uow:
        await uow.tasks.add(task)
        await uow.events.append(
            Event(
                new_id(),
                task.id,
                None,
                0,
                None,
                EventType.TASK_PROVISIONING_STARTED,
                1,
                {"schema_version": 1},
                FixedClock().now(),
            )
        )
        await uow.commit()
    return task, manager


async def test_reconciles_missing_and_existing_workspaces_idempotently(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    task, manager = await seed_provisioning_task(database, root, tmp_path / "data")
    factory = uow_factory(database)
    reconciler = StartupReconciler(manager, factory, FixedClock())

    await reconciler.reconcile()
    await reconciler.reconcile()

    async with factory() as uow:
        stored = await uow.tasks.get(task.id)
    assert stored is not None
    assert stored.status == "active"
    assert stored.workspace_path.exists()
    async with database.engine.connect() as connection:
        terminal_count = await connection.scalar(
            select(func.count())
            .select_from(models.task_events)
            .where(
                models.task_events.c.task_id == str(task.id),
                models.task_events.c.type == "task.provisioning_succeeded",
            )
        )
    assert terminal_count == 1

    existing, _ = await seed_provisioning_task(database, root, tmp_path / "other-data")
    plan = WorkspacePlan(
        root, "HEAD", existing.base_revision or "", existing.workspace_path
    )
    await manager.recover(plan)
    await StartupReconciler(manager, factory, FixedClock()).reconcile()
    async with factory() as uow:
        recovered = await uow.tasks.get(existing.id)
    assert recovered is not None and recovered.status == "active"


async def test_unexpected_destination_is_preserved_as_failure_evidence(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    task, manager = await seed_provisioning_task(database, root, tmp_path / "data")
    task.workspace_path.parent.mkdir(parents=True)
    subprocess.run(
        ["git", "clone", "-q", str(root), str(task.workspace_path)], check=True
    )
    evidence = task.workspace_path / "evidence.txt"
    evidence.write_text("keep me")

    await StartupReconciler(manager, uow_factory(database), FixedClock()).reconcile()

    async with uow_factory(database)() as uow:
        failed = await uow.tasks.get(task.id)
    assert failed is not None and failed.status == "provisioning_failed"
    assert evidence.read_text() == "keep me"
