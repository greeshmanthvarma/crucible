import os
import secrets
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from crucible.api.acceptances import router as acceptances_router
from crucible.api.approvals import router as approvals_router
from crucible.api.artifacts import router as artifacts_router
from crucible.api.auth import router as auth_router
from crucible.api.errors import application_error_handler
from crucible.api.events import router as events_router
from crucible.api.integrations import router as integrations_router
from crucible.api.repositories import router as repositories_router
from crucible.api.runs import router as runs_router
from crucible.api.tasks import messages_router
from crucible.api.tasks import router as tasks_router
from crucible.application.acceptance_service import AcceptanceService
from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.auth_service import AuthService
from crucible.application.container import ApplicationContainer
from crucible.application.errors import ApplicationError
from crucible.application.event_service import TaskEventSource
from crucible.application.integration_service import IntegrationService
from crucible.application.message_service import MessageService
from crucible.application.repository_service import RepositoryService
from crucible.application.run_service import RunService
from crucible.application.task_service import TaskService
from crucible.domain.clock import SystemClock

SESSION_COOKIE = "crucible_session"
UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def create_app(
    repository_service: RepositoryService | None = None,
    task_service: TaskService | None = None,
    message_service: MessageService | None = None,
    event_source: TaskEventSource | None = None,
    approval_service: ApprovalService | None = None,
    artifact_service: ArtifactService | None = None,
    run_service: RunService | None = None,
    acceptance_service: AcceptanceService | None = None,
    integration_service: IntegrationService | None = None,
    auth_service: AuthService | None = None,
    *,
    allowed_origins: tuple[str, ...] | None = None,
    development_origin: str | None = None,
    secure_cookie: bool | None = None,
) -> FastAPI:
    if allowed_origins is None:
        allowed_origins = (os.environ.get("CRUCIBLE_ORIGIN", "http://127.0.0.1:8000"),)
    if development_origin is None:
        development_origin = os.environ.get("CRUCIBLE_DEVELOPMENT_ORIGIN")
    if secure_cookie is None:
        secure_cookie = os.environ.get("CRUCIBLE_SECURE_COOKIE", "false").lower() in (
            "1",
            "true",
            "yes",
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if any(
            service is not None
            for service in (
                repository_service,
                task_service,
                message_service,
                event_source,
                approval_service,
                artifact_service,
                run_service,
                acceptance_service,
                integration_service,
                auth_service,
            )
        ):
            app.state.repository_service = repository_service
            app.state.task_service = task_service
            app.state.message_service = message_service
            app.state.event_source = event_source
            app.state.approval_service = approval_service
            app.state.artifact_service = artifact_service
            app.state.run_service = run_service
            app.state.acceptance_service = acceptance_service
            app.state.integration_service = integration_service
            app.state.auth_service = auth_service
            yield
            return
        container = await ApplicationContainer.create(
            os.environ.get("CRUCIBLE_DATABASE_URL", "sqlite+aiosqlite:///crucible.db"),
            Path(os.environ.get("CRUCIBLE_DATA_DIR", "data")),
        )
        app.state.repository_service = container.repository_service
        app.state.task_service = container.task_service
        app.state.message_service = container.message_service
        app.state.event_source = container.event_source
        app.state.approval_service = container.approval_service
        app.state.artifact_service = container.artifact_service
        app.state.run_service = container.run_service
        app.state.acceptance_service = container.acceptance_service
        app.state.integration_service = container.integration_service
        bootstrap_secret = secrets.token_urlsafe(32)
        print(f"Crucible bootstrap secret: {bootstrap_secret}", file=sys.stderr)
        app.state.auth_service = AuthService(
            container.unit_of_work, SystemClock(), bootstrap_secret
        )
        await container.start()
        try:
            yield
        finally:
            await container.close()

    app = FastAPI(title="Crucible", version="0.1.0", lifespan=lifespan)
    if any(
        service is not None
        for service in (
            repository_service,
            task_service,
            message_service,
            event_source,
            approval_service,
            artifact_service,
            run_service,
            acceptance_service,
            integration_service,
            auth_service,
        )
    ):
        app.state.repository_service = repository_service
        app.state.task_service = task_service
        app.state.message_service = message_service
        app.state.event_source = event_source
        app.state.approval_service = approval_service
        app.state.artifact_service = artifact_service
        app.state.run_service = run_service
        app.state.acceptance_service = acceptance_service
        app.state.integration_service = integration_service
        app.state.auth_service = auth_service
    app.add_exception_handler(ApplicationError, application_error_handler)  # type: ignore[arg-type]
    app.state.session_cookie_name = SESSION_COOKIE
    app.state.secure_cookie = secure_cookie
    exact_origins = (
        *allowed_origins,
        *((development_origin,) if development_origin else ()),
    )
    if development_origin is not None:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[development_origin],
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Content-Type", "Idempotency-Key", "X-CSRF-Token"],
        )

    @app.middleware("http")
    async def protect_control_plane(request: Request, call_next):  # type: ignore[no-untyped-def]
        service = getattr(request.app.state, "auth_service", None)
        if (
            service is None
            or request.method == "OPTIONS"
            or request.url.path in {"/api/health", "/api/auth/bootstrap"}
            or not request.url.path.startswith("/api/")
        ):
            return await call_next(request)
        try:
            token = request.cookies.get(SESSION_COOKIE)
            if request.method in UNSAFE_METHODS:
                origin = request.headers.get("origin")
                if origin not in exact_origins:
                    from crucible.application.errors import OriginRejected

                    raise OriginRejected("Exact allowed Origin is required")
                if request.url.path == "/api/auth/session":
                    # Recover a readable CSRF token from a valid HttpOnly session.
                    # Exact Origin is still required above.
                    await service.authenticate(token)
                else:
                    csrf = request.headers.get("x-csrf-token")
                    if not csrf:
                        from crucible.application.errors import CsrfRejected

                        raise CsrfRejected("CSRF token is required")
                    await service.authenticate(token, csrf)
            else:
                await service.authenticate(token)
        except ApplicationError as error:
            return JSONResponse(
                status_code=error.status_code,
                content={"code": error.code, "detail": error.detail},
            )
        return await call_next(request)

    if auth_service is not None or repository_service is None:
        app.include_router(auth_router)
    app.include_router(repositories_router)
    if event_source is not None or repository_service is None:
        app.include_router(events_router)
    if task_service is not None or repository_service is None:
        app.include_router(tasks_router)
    if message_service is not None or repository_service is None:
        app.include_router(messages_router)
    if approval_service is not None or repository_service is None:
        app.include_router(approvals_router)
    if artifact_service is not None or repository_service is None:
        app.include_router(artifacts_router)
    if run_service is not None or repository_service is None:
        app.include_router(runs_router)
    if acceptance_service is not None or repository_service is None:
        app.include_router(acceptances_router)
    if integration_service is not None or repository_service is None:
        app.include_router(integrations_router)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
