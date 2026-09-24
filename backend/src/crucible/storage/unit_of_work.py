from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import async_sessionmaker

from crucible.application.ports import (
    ApprovalStore,
    ArtifactMetadataStore,
    ContextManifestStore,
    EventStore,
    ExternalResourceStore,
    MessageStore,
    RepositoryStore,
    RunStore,
    StepStore,
    TaskStore,
    ToolCallStore,
    ToolResultStore,
    UnitOfWork,
)
from crucible.application.ports import IdempotencyStore as IdempotencyStorePort
from crucible.storage.database import Database
from crucible.storage.repositories import (
    ApprovalRepository,
    ArtifactRepository,
    ContextManifestRepository,
    EventRepository,
    ExternalResourceRepository,
    IdempotencyRepository,
    MessageRepository,
    RepositoryRepository,
    RunRepository,
    StepRepository,
    TaskRepository,
    ToolCallRepository,
    ToolResultRepository,
)

__all__ = ["SqlAlchemyUnitOfWork", "UnitOfWork"]


class SqlAlchemyUnitOfWork:
    repositories: RepositoryStore
    tasks: TaskStore
    messages: MessageStore
    steps: StepStore
    context_manifests: ContextManifestStore
    tool_calls: ToolCallStore
    tool_results: ToolResultStore
    approvals: ApprovalStore
    artifacts: ArtifactMetadataStore
    external_resources: ExternalResourceStore
    runs: RunStore
    events: EventStore
    idempotency: IdempotencyStorePort

    def __init__(self, database: Database) -> None:
        self._session_factory = async_sessionmaker(
            database.engine, expire_on_commit=False
        )

    async def __aenter__(self) -> Self:
        self.session = self._session_factory()
        self.repositories = RepositoryRepository(self.session)
        self.tasks = TaskRepository(self.session)
        self.messages = MessageRepository(self.session)
        self.steps = StepRepository(self.session)
        self.context_manifests = ContextManifestRepository(self.session)
        self.tool_calls = ToolCallRepository(self.session)
        self.tool_results = ToolResultRepository(self.session)
        self.approvals = ApprovalRepository(self.session)
        self.artifacts = ArtifactRepository(self.session)
        self.external_resources = ExternalResourceRepository(self.session)
        self.runs = RunRepository(self.session)
        self.events = EventRepository(self.session)
        self.idempotency = IdempotencyRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        await self.session.close()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
