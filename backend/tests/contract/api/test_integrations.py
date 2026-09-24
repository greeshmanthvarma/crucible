from pathlib import Path

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.integration_service import IntegrationService
from crucible.storage.database import Database
from crucible.workspaces.git import SubprocessGitClient
from tests.integration.application.test_acceptance import git
from tests.integration.application.test_integration import accepted_result
from tests.integration.engine.conftest import FixedClock


async def test_integration_http_contract_is_idempotent(
    database: Database, tmp_path: Path
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, "integration-api"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD").strip()
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD").strip()
    service = IntegrationService(SubprocessGitClient(), factory, FixedClock())
    request = {
        "repositoryId": str(repository.id),
        "targetRef": target_ref,
        "expectedRevision": expected,
    }
    async with AsyncClient(
        transport=ASGITransport(app=create_app(integration_service=service)),
        base_url="http://test",
    ) as client:
        missing = await client.post(
            f"/api/result-revisions/{result.id}/integrations", json=request
        )
        created = await client.post(
            f"/api/result-revisions/{result.id}/integrations",
            json=request,
            headers={"Idempotency-Key": "integrate-api"},
        )
        retried = await client.post(
            f"/api/result-revisions/{result.id}/integrations",
            json=request,
            headers={"Idempotency-Key": "integrate-api"},
        )

    assert missing.status_code == 400
    assert created.status_code == 201
    assert created.json() == retried.json()
    assert created.json()["status"] == "completed"
    assert created.json()["observedBeforeRevision"] == expected
