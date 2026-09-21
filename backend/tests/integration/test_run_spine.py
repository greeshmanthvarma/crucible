import asyncio
import subprocess
from pathlib import Path

from crucible.application.container import ApplicationContainer
from crucible.storage.database import Database


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


async def test_complete_run_spine_survives_container_recreation(
    database: Database, database_url: str, tmp_path: Path
) -> None:
    await database.dispose()
    root = tmp_path / "repository"
    create_repository(root)
    original_status = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    container = await ApplicationContainer.create(database_url, tmp_path / "data")
    await container.start()
    repository = await container.repository_service.register(root)
    task = await container.task_service.create(
        repository.repository.id, "HEAD", "run-spine-task"
    )
    submitted = await container.message_service.submit(task.id, "Explain", "vertical")

    for _ in range(200):
        async with container.unit_of_work() as uow:
            run = await uow.runs.get(submitted.run_id)
        if run is not None and run.status in {"completed", "failed"}:
            break
        await asyncio.sleep(0.01)
    messages_before = await container.message_service.list(task.id)
    events_before = await container.event_source.list_after(task.id, 0)
    (task.workspace_path / "tracked.txt").write_text("changed\n")
    assert (root / "tracked.txt").read_text() == "original\n"
    assert (
        subprocess.run(
            ["git", "status", "--porcelain=v1"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        == original_status
    )
    await container.close()

    restored = await ApplicationContainer.create(database_url, tmp_path / "data")
    await restored.start()
    assert await restored.task_service.get(task.id) == task
    assert await restored.message_service.list(task.id) == messages_before
    assert await restored.event_source.list_after(task.id, 0) == events_before
    assert [message.role for message in messages_before] == ["user", "assistant"]
    await restored.close()
