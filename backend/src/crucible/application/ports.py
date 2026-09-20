from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from crucible.domain.conversation import Message
from crucible.domain.events import Event
from crucible.domain.repository import Repository
from crucible.domain.run import Run
from crucible.domain.task import Task


class RepositoryStore(Protocol):
    async def add_if_absent(self, repository: Repository) -> bool: ...
    async def get_by_root(self, root: Path) -> Repository | None: ...
    async def list(self) -> tuple[Repository, ...]: ...


class TaskStore(Protocol):
    async def add(self, task: Task) -> None: ...


class MessageStore(Protocol):
    async def add(self, message: Message) -> Message: ...


class RunStore(Protocol):
    async def add(self, run: Run) -> None: ...
    async def claim_queued(
        self,
        run_id: UUID,
        execution_id: UUID,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool: ...
    async def set_triggering_message(self, run_id: UUID, message_id: UUID) -> None: ...


class EventStore(Protocol):
    async def append(self, event: Event) -> Event: ...


class IdempotencyStore(Protocol):
    """Port for durable idempotency records, completed in Task 7."""


class UnitOfWork(Protocol):
    repositories: RepositoryStore
    tasks: TaskStore
    messages: MessageStore
    runs: RunStore
    events: EventStore
    idempotency: IdempotencyStore

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
