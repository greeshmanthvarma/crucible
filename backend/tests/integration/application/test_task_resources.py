from pathlib import Path

from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager
from tests.contract.api.test_tasks import FixedClock, create_repository
from tests.unit.sandbox.test_resources import FakeDockerClient


async def test_task_activates_only_after_dependency_volume_is_persisted(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    clock = FixedClock()
    git = SubprocessGitClient()
    docker = FakeDockerClient()
    resources = TaskResourceManager(docker, factory, clock)
    repositories = RepositoryService(git, factory, clock)
    tasks = TaskService(
        WorkspaceManager(git, tmp_path / "data"),
        factory,
        clock,
        resource_manager=resources,
    )
    registered = await repositories.register(root)

    task = await tasks.create(registered.repository.id, "HEAD", "resource-task")

    async with SqlAlchemyUnitOfWork(database) as uow:
        stored = await uow.external_resources.list_for_task(task.id)
    assert task.status == "active"
    assert len(stored) == 1
    assert stored[0].external_identity in docker.volumes
