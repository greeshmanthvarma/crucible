from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.auth_service import AuthService
from tests.contract.api.test_auth import AuthClock, EmptyRepositories
from tests.contract.api.test_tasks import uow_factory


async def test_only_configured_development_origin_gets_credentialed_cors(
    database,
) -> None:  # type: ignore[no-untyped-def]
    origin = "http://127.0.0.1:5173"
    app = create_app(
        repository_service=EmptyRepositories(),  # type: ignore[arg-type]
        auth_service=AuthService(uow_factory(database), AuthClock(), "bootstrap"),
        allowed_origins=("http://127.0.0.1:8000",),
        development_origin=origin,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        allowed = await client.options(
            "/api/repositories",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "GET",
            },
        )
        rejected = await client.options(
            "/api/repositories",
            headers={
                "Origin": "http://127.0.0.1:5174",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert allowed.headers["access-control-allow-origin"] == origin
    assert allowed.headers["access-control-allow-credentials"] == "true"
    assert "access-control-allow-origin" not in rejected.headers
