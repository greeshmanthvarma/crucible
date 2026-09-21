from collections.abc import AsyncIterator, Awaitable, Callable
from uuid import UUID

from crucible.application.errors import (
    EventCursorNotFound,
    EventCursorTaskMismatch,
    TaskNotFound,
)
from crucible.application.ports import UnitOfWork
from crucible.domain.events import Event
from crucible.engine.notifier import TaskEventNotifier


class TaskEventSource:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        notifier: TaskEventNotifier,
        heartbeat_seconds: float = 15,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._notifier = notifier
        self._heartbeat_seconds = heartbeat_seconds

    async def resolve_cursor(self, task_id: UUID, cursor: UUID | None) -> int:
        async with self._unit_of_work() as uow:
            if await uow.tasks.get(task_id) is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            if cursor is None:
                return 0
            event = await uow.events.get(cursor)
        if event is None:
            raise EventCursorNotFound(f"Event cursor not found: {cursor}")
        if event.task_id != task_id:
            raise EventCursorTaskMismatch("Event cursor belongs to another Task")
        return event.task_sequence

    async def list_after(self, task_id: UUID, sequence: int) -> tuple[Event, ...]:
        async with self._unit_of_work() as uow:
            return await uow.events.list_after(task_id, sequence)

    async def follow(
        self,
        is_disconnected: Callable[[], Awaitable[bool]],
        task_id: UUID,
        sequence: int,
    ) -> AsyncIterator[Event | None]:
        generation = self._notifier.generation(task_id)
        while not await is_disconnected():
            events = await self.list_after(task_id, sequence)
            for event in events:
                yield event
                sequence = event.task_sequence
            if len(events) == 100:
                continue
            changed = await self._notifier.wait(
                task_id, generation, self._heartbeat_seconds
            )
            generation = self._notifier.generation(task_id)
            if not changed:
                yield None
