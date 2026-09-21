import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.ids import MessageId, RunId, StepId, TaskId, new_id
from crucible.domain.tools import (
    ToolCall,
    ToolCallStatus,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.engine.gateway import CompleteToolCall
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


class ToolDispatcher:
    def __init__(
        self,
        registry: ToolRegistry,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        *,
        max_calls: int = 100,
    ) -> None:
        self._registry = registry
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._max_calls = max_calls

    async def execute_batch(
        self, context: DispatchContext, calls: tuple[CompleteToolCall, ...]
    ) -> tuple[ToolResult, ...]:
        admitted = await self._preflight(context, calls)
        completion_lock = asyncio.Lock()
        completion_sequence = 0
        results: dict[object, ToolResult] = {}

        async def execute(item: AdmittedCall) -> None:
            nonlocal completion_sequence
            if item.rejection is not None:
                outcome = ToolOutcome({}, item.rejection, error_code="rejected")
                status = ToolResultStatus.REJECTED
            else:
                assert item.tool is not None
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
                    outcome = ToolOutcome({}, "Cancelled", error_code="cancelled")
                    status = ToolResultStatus.CANCELLED
                except Exception as error:
                    outcome = ToolOutcome(
                        {}, str(error)[:4000], error_code="tool_exception"
                    )
                    status = ToolResultStatus.FAILED
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
                async with self._unit_of_work() as uow:
                    await uow.tool_results.add(result)
                    await uow.commit()
                results[item.record.id] = result

        executable = [item for item in admitted if item.rejection is None]
        all_parallel = bool(executable) and all(
            item.tool is not None and item.tool.parallel_safe for item in executable
        )
        if all_parallel:
            async with asyncio.TaskGroup() as group:
                for item in admitted:
                    group.create_task(execute(item))
        else:
            for item in admitted:
                await execute(item)
        return tuple(results[item.record.id] for item in admitted)

    async def _preflight(
        self, context: DispatchContext, calls: tuple[CompleteToolCall, ...]
    ) -> tuple[AdmittedCall, ...]:
        admitted: list[AdmittedCall] = []
        for sequence, source in enumerate(calls, 1):
            tool = self._registry.get(source.name)
            rejection: str | None = None
            arguments: object = source.arguments
            if sequence > self._max_calls:
                rejection = "Tool Call budget exhausted"
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
            admitted.append(AdmittedCall(source, record, tool, arguments, rejection))
        async with self._unit_of_work() as uow:
            for item in admitted:
                await uow.tool_calls.add(item.record)
            await uow.commit()
        return tuple(admitted)
