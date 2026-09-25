from pathlib import Path

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.acceptance_service import AcceptanceService
from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.storage.database import Database
from crucible.workspaces.git import SubprocessGitClient
from tests.integration.application.test_acceptance import completed_task
from tests.integration.engine.conftest import FixedClock


async def test_acceptance_http_contract_is_idempotent(
    database: Database, tmp_path: Path
) -> None:
    factory, task, _repository = await completed_task(
        database, tmp_path, "acceptance-api"
    )
    (task.workspace_path / "README.md").write_text("accepted\n")
    service = AcceptanceService(
        SubprocessGitClient(),
        ArtifactService(
            LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
        ),
        factory,
        FixedClock(),
    )
    async with AsyncClient(
        transport=ASGITransport(app=create_app(acceptance_service=service)),
        base_url="http://test",
    ) as client:
        missing = await client.post(f"/api/tasks/{task.id}/acceptances", json={})
        created = await client.post(
            f"/api/tasks/{task.id}/acceptances",
            json={},
            headers={"Idempotency-Key": "accept-api"},
        )
        retried = await client.post(
            f"/api/tasks/{task.id}/acceptances",
            json={},
            headers={"Idempotency-Key": "accept-api"},
        )

    assert missing.status_code == 400
    assert created.status_code == 201
    assert created.json() == retried.json()
    assert created.json()["taskId"] == str(task.id)
    assert len(created.json()["commitSha"]) == 40
    assert created.json()["validationSnapshot"]["status"] == "not_configured"
