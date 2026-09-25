from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.auth_service import AuthService
from tests.contract.api.test_auth import AuthClock, EmptyRepositories
from tests.contract.api.test_tasks import uow_factory


async def test_unsafe_requests_require_exact_origin_and_csrf(database) -> None:  # type: ignore[no-untyped-def]
    auth = AuthService(uow_factory(database), AuthClock(), "bootstrap")
    app = create_app(
        repository_service=EmptyRepositories(),  # type: ignore[arg-type]
        auth_service=auth,
        allowed_origins=("http://127.0.0.1:8000",),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        exchanged = await client.post(
            "/api/auth/bootstrap", json={"secret": "bootstrap"}
        )
        csrf = exchanged.json()["csrfToken"]
        cases = [
            ({"X-CSRF-Token": csrf}, "origin_rejected"),
            ({"Origin": "null", "X-CSRF-Token": csrf}, "origin_rejected"),
            (
                {"Origin": "http://127.0.0.1:8000.evil", "X-CSRF-Token": csrf},
                "origin_rejected",
            ),
            (
                {"Origin": "http://evil.127.0.0.1:8000", "X-CSRF-Token": csrf},
                "origin_rejected",
            ),
            (
                {"Origin": "http://127.0.0.1:8001", "X-CSRF-Token": csrf},
                "origin_rejected",
            ),
            ({"Origin": "http://127.0.0.1:8000"}, "csrf_rejected"),
            (
                {"Origin": "http://127.0.0.1:8000", "X-CSRF-Token": "wrong"},
                "csrf_rejected",
            ),
        ]
        for headers, code in cases:
            response = await client.post("/api/auth/logout", headers=headers)
            assert response.status_code in (401, 403)
            assert response.json()["code"] == code

        valid = await client.post(
            "/api/auth/logout",
            headers={
                "Origin": "http://127.0.0.1:8000",
                "X-CSRF-Token": csrf,
            },
        )
    assert valid.status_code == 204
