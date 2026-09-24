from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.ids import new_id
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


async def test_artifact_download_uses_opaque_registered_identity(
    database: Database, tmp_path
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, *_ = await seed_exchange(uow, "artifact-api")
        await uow.commit()

    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    service = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )

    async def content():
        yield b"private evidence"

    artifact = await service.put(
        task.id, "text/plain", "private", content(), hard_limit=100
    )
    app = create_app(artifact_service=service)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/api/artifacts/{artifact.id}")
        missing = await client.get(f"/api/artifacts/{new_id()}")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.content == b"private evidence"
    assert missing.status_code == 404
