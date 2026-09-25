from datetime import UTC, datetime, timedelta
from uuid import uuid4

from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from crucible.api.app import create_app
from crucible.application.auth_service import AuthService
from crucible.application.event_service import TaskEventSource
from crucible.engine.notifier import TaskEventNotifier
from crucible.storage import models
from crucible.storage.database import Database
from tests.contract.api.test_tasks import uow_factory


class AuthClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, tzinfo=UTC)


class EmptyRepositories:
    async def list(self) -> tuple[()]:
        return ()


async def test_bootstrap_is_one_time_and_session_is_revocable(
    database: Database,
) -> None:
    auth = AuthService(uow_factory(database), AuthClock(), "bootstrap-secret")
    app = create_app(
        repository_service=EmptyRepositories(),  # type: ignore[arg-type]
        auth_service=auth,
        allowed_origins=("http://127.0.0.1:8000",),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        exchanged = await client.post(
            "/api/auth/bootstrap", json={"secret": "bootstrap-secret"}
        )
        replay = await client.post(
            "/api/auth/bootstrap", json={"secret": "bootstrap-secret"}
        )
        resumed = await client.get("/api/auth/session")
        csrf = resumed.json()["csrfToken"]
        authenticated = await client.get("/api/repositories")
        revoked = await client.post(
            "/api/auth/logout",
            headers={
                "Origin": "http://127.0.0.1:8000",
                "X-CSRF-Token": csrf,
            },
        )
        denied = await client.get("/api/repositories")

    assert exchanged.status_code == 201
    assert replay.status_code == 401
    assert resumed.status_code == 200
    assert authenticated.status_code != 401
    assert revoked.status_code == 204
    assert denied.status_code == 401
    assert "bootstrap-secret" not in replay.text
    assert csrf not in denied.text
    cookie = exchanged.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie
    assert "Path=/" in cookie
    assert "Domain=" not in cookie


async def test_session_secrets_are_hashed_expire_and_secure_cookie_is_configurable(
    database: Database,
) -> None:
    clock = AuthClock()
    auth = AuthService(
        uow_factory(database), clock, "never-store-me", ttl=timedelta(seconds=-1)
    )
    app = create_app(
        repository_service=EmptyRepositories(),  # type: ignore[arg-type]
        auth_service=auth,
        secure_cookie=True,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://127.0.0.1:8000"
    ) as client:
        exchanged = await client.post(
            "/api/auth/bootstrap", json={"secret": "never-store-me"}
        )
        session_token = client.cookies.get("crucible_session")
        denied = await client.get("/api/repositories")

    async with database.engine.connect() as connection:
        row = (
            (await connection.execute(select(models.browser_sessions))).mappings().one()
        )
    assert exchanged.status_code == 201
    assert "Secure" in exchanged.headers["set-cookie"]
    assert denied.status_code == 401
    assert session_token is not None and session_token not in str(row)
    assert "never-store-me" not in str(row)
    assert exchanged.json()["csrfToken"] not in str(row)


async def test_sse_requires_the_browser_session_cookie(database: Database) -> None:
    auth = AuthService(uow_factory(database), AuthClock(), "bootstrap")
    source = TaskEventSource(uow_factory(database), TaskEventNotifier())
    app = create_app(event_source=source, auth_service=auth)
    path = f"/api/tasks/{uuid4()}/events"
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as anonymous:
        denied = await anonymous.get(path)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as authenticated:
        await authenticated.post("/api/auth/bootstrap", json={"secret": "bootstrap"})
        allowed_to_route = await authenticated.get(path)

    assert denied.status_code == 401
    assert allowed_to_route.status_code == 404
