import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.message_service import MessageService
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


async def test_submit_retry_conflict_validation_and_listing_contract(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    factory = uow_factory(database)
    git = SubprocessGitClient()
    repositories = RepositoryService(git, factory, FixedClock())
    tasks = TaskService(WorkspaceManager(git, tmp_path / "data"), factory, FixedClock())
    supervisor = RecordingSupervisor()
    messages = MessageService(factory, FixedClock(), supervisor)
    registered = await repositories.register(root)
    task = await tasks.create(registered.repository.id, "HEAD")

    async with AsyncClient(
        transport=ASGITransport(app=create_app(repositories, tasks, messages)),
        base_url="http://test",
    ) as client:
        missing_key = await client.post(
            f"/api/tasks/{task.id}/messages", json={"text": "hello"}
        )
        blank = await client.post(
            f"/api/tasks/{task.id}/messages",
            json={"text": "   "},
            headers={"Idempotency-Key": "blank"},
        )
        created = await client.post(
            f"/api/tasks/{task.id}/messages",
            json={"text": "hello"},
            headers={"Idempotency-Key": "message-1"},
        )
        retried = await client.post(
            f"/api/tasks/{task.id}/messages",
            json={"text": "hello"},
            headers={"Idempotency-Key": "message-1"},
        )
        conflict = await client.post(
            f"/api/tasks/{task.id}/messages",
            json={"text": "changed"},
            headers={"Idempotency-Key": "message-1"},
        )
        listed = await client.get(f"/api/tasks/{task.id}/messages")

    assert missing_key.status_code == 400
    assert missing_key.json()["code"] == "idempotency_key_required"
    assert blank.status_code == 422
    assert created.status_code == 202
    assert retried.json() == created.json()
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "idempotency_conflict"
    assert listed.status_code == 200
    assert listed.json()[0]["conversationSequence"] == 1
    assert listed.json()[0]["parts"][0]["textContent"] == "hello"
    assert supervisor.submissions == [UUID(created.json()["runId"])]
