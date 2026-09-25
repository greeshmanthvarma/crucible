import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path

from pydantic import ValidationError

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import Message
from crucible.domain.events import EventType
from crucible.domain.ids import MessageId, RunId, StepId, TaskId, new_id
from crucible.domain.tools import (
    ToolCall,
    ToolCallStatus,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.engine.gateway import CompleteToolCall
from crucible.engine.journal import EventSpec, JournalMutation, RunJournal
from crucible.tools.definitions import Tool, ToolContext, ToolOutcome
from crucible.tools.registry import ToolRegistry


@dataclass(frozen=True)
class DispatchContext:
    task_id: TaskId
    run_id: RunId
    step_id: StepId
    assistant_message_id: MessageId
    workspace: Path


@dataclass(frozen=True)
class AdmittedCall:
    source: CompleteToolCall
    record: ToolCall
    tool: Tool | None
    arguments: object
    rejection: str | None = None
    rejection_code: str = "rejected"


class ToolDispatcher:
    def __init__(
        self,
        registry: ToolRegistry,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        *,
        max_calls: int = 100,
        journal: RunJournal | None = None,
    ) -> None:
        self._registry = registry
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._max_calls = max_calls
        self._journal = journal

    async def execute_batch(
        self,
        context: DispatchContext,
        calls: tuple[CompleteToolCall, ...],
        *,
        call_budget: int | None = None,
        assistant_message: Message | None = None,
    ) -> tuple[ToolResult, ...]:
        admitted = await self._preflight(
            context,
            calls,
            call_budget=call_budget,
            assistant_message=assistant_message,
        )
        completion_lock = asyncio.Lock()
        completion_sequence = 0
        results: dict[object, ToolResult] = {}

        async def record_result(
            item: AdmittedCall, outcome: ToolOutcome, status: ToolResultStatus
        ) -> None:
            nonlocal completion_sequence
            async with completion_lock:
                completion_sequence += 1
                result = ToolResult(
                    id=new_id(),
                    task_id=context.task_id,
                    run_id=context.run_id,
                    step_id=context.step_id,
                    tool_call_id=item.record.id,
                    status=status,
                    result={**dict(outcome.data), "truncated": outcome.truncated},
                    schema_version=1,
                    display_text=outcome.display_text[:100_000],
                    error_code=outcome.error_code,
                    completion_sequence=completion_sequence,
                    created_at=self._clock.now(),
                    completed_at=self._clock.now(),
                )

                async def persist(uow: UnitOfWork) -> None:
                    await uow.tool_results.add(result)
                    if item.rejection is None:
                        await uow.tool_calls.update(
                            replace(item.record, status=ToolCallStatus.COMPLETED)
                        )

                await self._persist(
                    context,
                    persist,
                    EventSpec(
                        EventType.TOOL_CALL_COMPLETED,
                        {
                            "tool_call_id": str(item.record.id),
                            "tool_result_id": str(result.id),
                            "status": result.status.value,
                        },
                    ),
                )
                results[item.record.id] = result

        async def execute(item: AdmittedCall) -> None:
            if item.rejection is not None:
                outcome = ToolOutcome(
                    {}, item.rejection, error_code=item.rejection_code
                )
                status = ToolResultStatus.REJECTED
            else:
                assert item.tool is not None
                running = replace(item.record, status=ToolCallStatus.RUNNING)

                async def start(uow: UnitOfWork) -> None:
                    await uow.tool_calls.update(running)

                await self._persist(
                    context,
                    start,
                    EventSpec(
                        EventType.TOOL_CALL_STARTED,
                        {"tool_call_id": str(item.record.id)},
                    ),
                )
                try:
                    outcome = await item.tool.invoke(
                        ToolContext(context.workspace), item.arguments
                    )
                    status = (
                        ToolResultStatus.FAILED
                        if outcome.error_code is not None
                        else ToolResultStatus.SUCCEEDED
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    outcome = ToolOutcome(
                        {}, str(error)[:4000], error_code="tool_exception"
                    )
                    status = ToolResultStatus.FAILED
            await record_result(item, outcome, status)

        all_parallel = bool(admitted) and all(
            item.rejection is None and item.tool is not None and item.tool.parallel_safe
            for item in admitted
        )
        try:
            if all_parallel:
                async with asyncio.TaskGroup() as group:
                    for item in admitted:
                        group.create_task(execute(item))
            else:
                for item in admitted:
                    await execute(item)
        except asyncio.CancelledError:

            async def terminalize() -> None:
                async with self._unit_of_work() as uow:
                    persisted = {
                        result.tool_call_id
                        for result in await uow.tool_results.list_for_step(
                            context.step_id
                        )
                    }
                for item in admitted:
                    if item.record.id not in persisted:
                        await record_result(
                            item,
                            ToolOutcome({}, "Cancelled", error_code="cancelled"),
                            ToolResultStatus.CANCELLED,
                        )

            cleanup = asyncio.create_task(terminalize())
            await asyncio.shield(cleanup)
            raise
        return tuple(results[item.record.id] for item in admitted)

    async def _preflight(
        self,
        context: DispatchContext,
        calls: tuple[CompleteToolCall, ...],
        *,
        call_budget: int | None,
        assistant_message: Message | None,
    ) -> tuple[AdmittedCall, ...]:
        admitted: list[AdmittedCall] = []
        limit = (
            self._max_calls
            if call_budget is None
            else min(self._max_calls, call_budget)
        )
        for sequence, source in enumerate(calls, 1):
            tool = self._registry.get(source.name)
            rejection: str | None = None
            rejection_code = "rejected"
            arguments: object = source.arguments
            if sequence > limit:
                rejection = "Tool Call budget exhausted"
                rejection_code = "budget_exhausted"
            elif tool is None:
                rejection = f"Unknown tool: {source.name}"
            else:
                try:
                    arguments = tool.argument_model.model_validate(
                        source.arguments
                    ).model_dump()
                except ValidationError as error:
                    rejection = str(error)
            mode = (
                ToolExecutionMode.PARALLEL
                if tool is not None and tool.parallel_safe
                else ToolExecutionMode.SEQUENTIAL
            )
            record = ToolCall(
                id=source.id,
                task_id=context.task_id,
                run_id=context.run_id,
                step_id=context.step_id,
                assistant_message_id=context.assistant_message_id,
                call_sequence=sequence,
                name=source.name,
                arguments=dict(source.arguments),
                schema_version=1,
                provider_correlation_id=source.provider_correlation_id,
                execution_mode=mode,
                created_at=self._clock.now(),
                status=(
                    ToolCallStatus.REJECTED
                    if rejection is not None
                    else ToolCallStatus.ADMITTED
                ),
            )
            admitted.append(
                AdmittedCall(source, record, tool, arguments, rejection, rejection_code)
            )

        async def persist(uow: UnitOfWork) -> None:
            if assistant_message is not None:
                await uow.messages.add(assistant_message)
            for item in admitted:
                await uow.tool_calls.add(item.record)

        await self._persist(
            context,
            persist,
            *(
                (
                    EventSpec(
                        EventType.MESSAGE_COMPLETED,
                        {"message_id": str(assistant_message.id)},
                    ),
                )
                if assistant_message is not None
                else ()
            ),
            *(
                EventSpec(
                    EventType.TOOL_CALL_ADMITTED,
                    {
                        "tool_call_id": str(item.record.id),
                        "status": item.record.status.value,
                    },
                )
                for item in admitted
            ),
        )
        return tuple(admitted)

    async def _persist(
        self,
        context: DispatchContext,
        apply: Callable[[UnitOfWork], Awaitable[None]],
        *events: EventSpec,
    ) -> None:
        async def mutation(uow: UnitOfWork) -> None:
            await apply(uow)

        if self._journal is not None:
            await self._journal.record(
                JournalMutation(
                    context.task_id, context.run_id, mutation, tuple(events)
                )
            )
            return
        async with self._unit_of_work() as uow:
            await mutation(uow)
            await uow.commit()
