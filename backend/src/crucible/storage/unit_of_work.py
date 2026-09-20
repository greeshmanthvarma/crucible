from types import TracebackType
from typing import Protocol, Self

from sqlalchemy.ext.asyncio import async_sessionmaker

from crucible.storage.database import Database
from crucible.storage.repositories import (
    EventRepository,
    EventWriter,
    IdempotencyRepository,
    IdempotencyStore,
    MessageRepository,
    MessageWriter,
    RepositoryReader,
    RepositoryRepository,
    RunRepository,
    RunWriter,
    TaskRepository,
    TaskWriter,
)


class UnitOfWork(Protocol):
    repositories: RepositoryReader
    tasks: TaskWriter
    messages: MessageWriter
    runs: RunWriter
    events: EventWriter
    idempotency: IdempotencyStore

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc_info: object) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class SqlAlchemyUnitOfWork:
    def __init__(self, database: Database) -> None:
        self._session_factory = async_sessionmaker(
            database.engine, expire_on_commit=False
        )

    async def __aenter__(self) -> Self:
        self.session = self._session_factory()
        self.repositories = RepositoryRepository(self.session)
        self.tasks = TaskRepository(self.session)
        self.messages = MessageRepository(self.session)
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
