from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import async_sessionmaker

from crucible.application.ports import (
    ApprovalStore,
    ArtifactMetadataStore,
    CompactionStore,
    ContextManifestStore,
    EventStore,
    ExternalResourceStore,
    IntegrationStore,
    MessageStore,
    RepositoryStore,
    ResultRevisionStore,
    RunStore,
    StepStore,
    TaskStore,
    ToolCallStore,
    ToolResultStore,
    UnitOfWork,
    ValidationAttemptStore,
    ValidationCommandResultStore,
)
from crucible.application.ports import IdempotencyStore as IdempotencyStorePort
from crucible.storage.database import Database
from crucible.storage.repositories import (
    ApprovalRepository,
    ArtifactRepository,
    CompactionRepository,
    ContextManifestRepository,
    EventRepository,
    ExternalResourceRepository,
    IdempotencyRepository,
    IntegrationRepository,
    MessageRepository,
    RepositoryRepository,
    ResultRevisionRepository,
    RunRepository,
    StepRepository,
    TaskRepository,
    ToolCallRepository,
    ToolResultRepository,
    ValidationAttemptRepository,
    ValidationCommandResultRepository,
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
    compactions: CompactionStore
    validation_attempts: ValidationAttemptStore
    validation_command_results: ValidationCommandResultStore
    result_revisions: ResultRevisionStore
    integrations: IntegrationStore

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
        self.compactions = CompactionRepository(self.session)
        self.validation_attempts = ValidationAttemptRepository(self.session)
        self.validation_command_results = ValidationCommandResultRepository(
            self.session
        )
        self.result_revisions = ResultRevisionRepository(self.session)
        self.integrations = IntegrationRepository(self.session)
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
