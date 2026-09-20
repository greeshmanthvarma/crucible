from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app


async def test_health_reports_ready() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=create_app()),
        base_url="http://test",
    ) as client:
        response = await client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
