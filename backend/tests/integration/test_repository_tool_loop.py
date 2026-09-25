import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.task_service import TaskService
from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    PreparedModelRequest,
    TextDelta,
)
from crucible.engine.run_engine import RunEngine
from crucible.storage.database import Database
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


class EditingGateway:
    def __init__(self) -> None:
        self.requests: list[PreparedModelRequest] = []

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        self.requests.append(request)
        if len(self.requests) == 1:
            yield CompleteToolCall(
                new_id(),
                "write_file",
                {"path": "README.md", "content": "changed by Crucible\n"},
            )
            yield ModelStop(ModelStopReason.TOOL_CALLS)
            return
        yield TextDelta("Updated README.md.")
        yield ModelStop(ModelStopReason.COMPLETE)


async def test_repository_tool_loop_survives_restart_and_exposes_evidence(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(
        database,
        tmp_path,
        "e2e-repository",
        instructions="Keep repository edits focused and evidenced.\n",
    )
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        task = await uow.tasks.get(run.task_id)
        assert task is not None

    registered_checkout = tmp_path / "e2e-repository"
    initial_status = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=registered_checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert await RunEngine(factory, FixedClock(), EditingGateway()).execute(
        submitted.run_id
    )
    assert (task.workspace_path / "README.md").read_text() == "changed by Crucible\n"
    assert (registered_checkout / "README.md").read_text() == "fixture\n"
    assert (
        subprocess.run(
            ["git", "status", "--porcelain=v1"],
            cwd=registered_checkout,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        == initial_status
    )

    await database.dispose()
    reopened = await Database.create(f"sqlite+aiosqlite:///{database.path}")
    try:
        reopened_factory = uow_factory(reopened)
        service = TaskService(
            WorkspaceManager(SubprocessGitClient(), tmp_path / "data"),
            reopened_factory,
            FixedClock(),
        )
        app = create_app(task_service=service)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            trace = await client.get(f"/api/tasks/{task.id}/trace")
            workspace = await client.get(f"/api/tasks/{task.id}/workspace")

        assert trace.status_code == 200
        assert [step["stepSequence"] for step in trace.json()] == [1, 2]
        assert trace.json()[0]["manifest"]["instructionDigests"]["repository"]
        assert trace.json()[0]["calls"][0]["name"] == "write_file"
        assert trace.json()[0]["results"][0]["status"] == "succeeded"
        assert workspace.status_code == 200
        assert "changed by Crucible" in workspace.json()["diff"]
    finally:
        await reopened.dispose()
