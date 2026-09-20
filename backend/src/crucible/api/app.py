import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from crucible.api.errors import application_error_handler
from crucible.api.repositories import router as repositories_router
from crucible.application.errors import ApplicationError
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.domain.clock import SystemClock
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient


def create_app(repository_service: RepositoryService | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if repository_service is not None:
            app.state.repository_service = repository_service
            yield
            return
        database = await Database.create(
            os.environ.get("CRUCIBLE_DATABASE_URL", "sqlite+aiosqlite:///crucible.db")
        )

        def unit_of_work() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(database)

        app.state.repository_service = RepositoryService(
            SubprocessGitClient(), unit_of_work, SystemClock()
        )
        try:
            yield
        finally:
            await database.dispose()

    app = FastAPI(title="Crucible", version="0.1.0", lifespan=lifespan)
    if repository_service is not None:
        app.state.repository_service = repository_service
    app.add_exception_handler(ApplicationError, application_error_handler)  # type: ignore[arg-type]
    app.include_router(repositories_router)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
