from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Mapping

from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import Message
from crucible.domain.events import Event, EventFactory, EventType
from crucible.domain.ids import ExecutionId, RunId, TaskId, new_id
from crucible.domain.run import Run
from crucible.domain.steps import Step
from crucible.domain.tools import ToolCall, ToolCallStatus, ToolResult, ToolResultStatus

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
            if any(
                spec.type
                in (
                    EventType.RUN_COMPLETED,
                    EventType.RUN_FAILED,
                    EventType.RUN_INTERRUPTED,
                    EventType.RUN_CANCELLED,
                )
                for spec in mutation.events
            ):
                await uow.evals.rebuild_summary(mutation.run_id, created_at)
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


def complete_run_mutation(run: Run, now: datetime) -> JournalMutation:
    async def apply(uow: UnitOfWork) -> None:
        await uow.runs.update(run.complete(now=now))

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(EventSpec(EventType.RUN_COMPLETED),),
    )


def terminal_run_mutation(
    run: Run,
    *,
    type: EventType,
    code: str,
    detail: str,
    now: datetime,
    step: Step | None = None,
) -> JournalMutation:
    if type is EventType.RUN_FAILED:
        terminal = run.fail(code, detail, now=now)
    elif type is EventType.RUN_INTERRUPTED:
        terminal = run.interrupt(code, detail, now=now)
    elif type is EventType.RUN_CANCELLED:
        terminal = run.cancel(code, detail, now=now)
    else:
        raise ValueError(f"Unsupported terminal Run Event: {type}")

    async def apply(uow: UnitOfWork) -> None:
        if step is not None:
            await uow.steps.update(
                step.fail(now) if type is EventType.RUN_FAILED else step.interrupt(now)
            )
        await uow.runs.update(terminal)

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(
            *(
                (
                    EventSpec(
                        EventType.STEP_FAILED
                        if type is EventType.RUN_FAILED
                        else EventType.STEP_INTERRUPTED,
                        {"step_id": str(step.id)},
                    ),
                )
                if step is not None
                else ()
            ),
            EventSpec(type, {"outcome_code": code}),
        ),
    )


def recovery_mutation(
    run: Run,
    *,
    now: datetime,
    orphaned_calls: tuple[ToolCall, ...] = (),
    active_step: Step | None = None,
) -> JournalMutation:
    terminal = run.interrupt(
        "process_restarted", "Run was owned by a prior application process", now=now
    )

    async def apply(uow: UnitOfWork) -> None:
        completion_by_step: dict[object, int] = {}
        for call in orphaned_calls:
            if call.step_id not in completion_by_step:
                existing = await uow.tool_results.list_for_step(call.step_id)
                completion_by_step[call.step_id] = max(
                    (result.completion_sequence for result in existing), default=0
                )
            completion_by_step[call.step_id] += 1
            await uow.tool_results.add(
                ToolResult(
                    id=new_id(),
                    task_id=call.task_id,
                    run_id=call.run_id,
                    step_id=call.step_id,
                    tool_call_id=call.id,
                    status=ToolResultStatus.INTERRUPTED,
                    result={"truncated": False},
                    schema_version=1,
                    display_text="Interrupted by process restart",
                    error_code="process_restarted",
                    completion_sequence=completion_by_step[call.step_id],
                    created_at=now,
                    completed_at=now,
                )
            )
            if call.status is not ToolCallStatus.REJECTED:
                await uow.tool_calls.update(
                    replace(call, status=ToolCallStatus.COMPLETED)
                )
        if active_step is not None:
            await uow.steps.update(active_step.interrupt(now))
        await uow.runs.update(terminal)

    return JournalMutation(
        task_id=run.task_id,
        run_id=run.id,
        apply=apply,
        events=(
            *(
                (
                    EventSpec(
                        EventType.STEP_INTERRUPTED,
                        {"step_id": str(active_step.id)},
                    ),
                )
                if active_step is not None
                else ()
            ),
            EventSpec(EventType.RUN_INTERRUPTED, {"outcome_code": "process_restarted"}),
        ),
    )


def cancellation_mutation(
    run: Run,
    *,
    now: datetime,
    outstanding_calls: tuple[ToolCall, ...] = (),
    active_step: Step | None = None,
) -> JournalMutation:
    terminal = run.cancel("cancelled", "Run cancelled by user", now=now)

    async def apply(uow: UnitOfWork) -> None:
        completion_by_step: dict[object, int] = {}
        for call in outstanding_calls:
            if call.step_id not in completion_by_step:
                existing = await uow.tool_results.list_for_step(call.step_id)
                completion_by_step[call.step_id] = max(
                    (result.completion_sequence for result in existing), default=0
                )
            completion_by_step[call.step_id] += 1
            await uow.tool_results.add(
                ToolResult(
                    new_id(),
                    call.task_id,
                    call.run_id,
                    call.step_id,
                    call.id,
                    ToolResultStatus.CANCELLED,
                    {"truncated": False},
                    1,
                    "Cancelled",
                    "cancelled",
                    completion_by_step[call.step_id],
                    now,
                    now,
                )
            )
            if call.status is not ToolCallStatus.REJECTED:
                await uow.tool_calls.update(
                    replace(call, status=ToolCallStatus.COMPLETED)
                )
        if active_step is not None:
            await uow.steps.update(active_step.interrupt(now))
        await uow.runs.update(terminal)

    return JournalMutation(
        run.task_id,
        run.id,
        apply,
        (
            *(
                (
                    EventSpec(
                        EventType.STEP_INTERRUPTED,
                        {"step_id": str(active_step.id)},
                    ),
                )
                if active_step is not None
                else ()
            ),
            EventSpec(EventType.RUN_CANCELLED, {"outcome_code": "cancelled"}),
        ),
    )
