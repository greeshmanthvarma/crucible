from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Mapping

from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import Message
from crucible.domain.events import Event, EventFactory, EventType
from crucible.domain.ids import ExecutionId, RunId, TaskId
from crucible.domain.run import Run

MutationAction = Callable[[UnitOfWork], Awaitable[None]]


class JournalMutationRejected(Exception):
    """The durable precondition for a journal mutation was not met."""


@dataclass(frozen=True)
class EventSpec:
    type: EventType
    payload: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class JournalMutation:
    task_id: TaskId
    run_id: RunId
    apply: MutationAction
    events: tuple[EventSpec, ...]


class RunJournal:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
        event_factory: EventFactory | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._events = event_factory or EventFactory()

    async def record(self, mutation: JournalMutation) -> tuple[Event, ...]:
        async with self._unit_of_work() as uow:
            await mutation.apply(uow)
            created_at = self._clock.now()
            stored = tuple(
                [
                    await uow.events.append(
                        self._events.create(
                            task_id=mutation.task_id,
                            run_id=mutation.run_id,
                            type=spec.type,
                            created_at=created_at,
                            payload=spec.payload,
                        )
                    )
                    for spec in mutation.events
                ]
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(mutation.task_id)
        return stored


def claim_run_mutation(
    run: Run,
    process_execution_id: ExecutionId,
    now: datetime,
    lease_duration: timedelta = timedelta(minutes=5),
) -> JournalMutation:
    async def apply(uow: UnitOfWork) -> None:
        claimed = await uow.runs.claim_queued(
            run.id, process_execution_id, now, now + lease_duration
        )
        if not claimed:
            raise JournalMutationRejected(f"Run cannot be claimed: {run.id}")

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(EventSpec(EventType.RUN_STARTED),),
    )


def completed_message_mutation(
    run: Run, message: Message, now: datetime
) -> JournalMutation:
    async def apply(uow: UnitOfWork) -> None:
        await uow.messages.add(message)
        await uow.runs.update(run.complete(now=now))

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(
            EventSpec(EventType.MESSAGE_COMPLETED, {"message_id": str(message.id)}),
            EventSpec(EventType.RUN_COMPLETED),
        ),
    )


def message_completed_mutation(run: Run, message: Message) -> JournalMutation:
    async def apply(uow: UnitOfWork) -> None:
        await uow.messages.add(message)

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(
            EventSpec(EventType.MESSAGE_COMPLETED, {"message_id": str(message.id)}),
        ),
    )


def terminal_run_mutation(
    run: Run,
    *,
    type: EventType,
    code: str,
    detail: str,
    now: datetime,
) -> JournalMutation:
    if type is EventType.RUN_FAILED:
        terminal = run.fail(code, detail, now=now)
    elif type is EventType.RUN_INTERRUPTED:
        terminal = run.interrupt(code, detail, now=now)
    else:
        raise ValueError(f"Unsupported terminal Run Event: {type}")

    async def apply(uow: UnitOfWork) -> None:
        await uow.runs.update(terminal)

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(EventSpec(type, {"outcome_code": code}),),
    )


def recovery_mutation(run: Run, *, now: datetime) -> JournalMutation:
    return terminal_run_mutation(
        run,
        type=EventType.RUN_INTERRUPTED,
        code="process_restarted",
        detail="Run was owned by a prior application process",
        now=now,
    )
