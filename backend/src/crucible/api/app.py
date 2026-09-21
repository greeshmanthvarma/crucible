import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from crucible.api.errors import application_error_handler
from crucible.api.repositories import router as repositories_router
from crucible.api.tasks import router as tasks_router
from crucible.application.errors import ApplicationError
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.domain.clock import SystemClock
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


def create_app(
    repository_service: RepositoryService | None = None,
    task_service: TaskService | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if repository_service is not None:
            app.state.repository_service = repository_service
            app.state.task_service = task_service
            yield
            return
        database = await Database.create(
            os.environ.get("CRUCIBLE_DATABASE_URL", "sqlite+aiosqlite:///crucible.db")
        )

        def unit_of_work() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(database)

        git = SubprocessGitClient()
        clock = SystemClock()
        app.state.repository_service = RepositoryService(git, unit_of_work, clock)
        app.state.task_service = TaskService(
            WorkspaceManager(git, Path(os.environ.get("CRUCIBLE_DATA_DIR", "data"))),
            unit_of_work,
            clock,
        )
        try:
            yield
        finally:
            await database.dispose()

    app = FastAPI(title="Crucible", version="0.1.0", lifespan=lifespan)
    if repository_service is not None:
        app.state.repository_service = repository_service
        app.state.task_service = task_service
    app.add_exception_handler(ApplicationError, application_error_handler)  # type: ignore[arg-type]
    app.include_router(repositories_router)
    if task_service is not None or repository_service is None:
        app.include_router(tasks_router)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
