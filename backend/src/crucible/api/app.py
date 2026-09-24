import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from crucible.api.approvals import router as approvals_router
from crucible.api.errors import application_error_handler
from crucible.api.events import router as events_router
from crucible.api.repositories import router as repositories_router
from crucible.api.tasks import messages_router
from crucible.api.tasks import router as tasks_router
from crucible.application.approval_service import ApprovalService
from crucible.application.container import ApplicationContainer
from crucible.application.errors import ApplicationError
from crucible.application.event_service import TaskEventSource
from crucible.application.message_service import MessageService
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService


def create_app(
    repository_service: RepositoryService | None = None,
    task_service: TaskService | None = None,
    message_service: MessageService | None = None,
    event_source: TaskEventSource | None = None,
    approval_service: ApprovalService | None = None,
) -> FastAPI:
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
            )
        ):
            app.state.repository_service = repository_service
            app.state.task_service = task_service
            app.state.message_service = message_service
            app.state.event_source = event_source
            app.state.approval_service = approval_service
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
        )
    ):
        app.state.repository_service = repository_service
        app.state.task_service = task_service
        app.state.message_service = message_service
        app.state.event_source = event_source
        app.state.approval_service = approval_service
    app.add_exception_handler(ApplicationError, application_error_handler)  # type: ignore[arg-type]
    app.include_router(repositories_router)
    if event_source is not None or repository_service is None:
        app.include_router(events_router)
    if task_service is not None or repository_service is None:
        app.include_router(tasks_router)
    if message_service is not None or repository_service is None:
        app.include_router(messages_router)
    if approval_service is not None or repository_service is None:
        app.include_router(approvals_router)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
