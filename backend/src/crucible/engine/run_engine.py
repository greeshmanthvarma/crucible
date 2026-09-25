import asyncio
import time
from collections.abc import Callable
from uuid import UUID

from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.context.manager import (
    ContextLimitExceeded,
    ContextManager,
    SimpleTokenEstimator,
)
from crucible.domain.clock import Clock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import EventType
from crucible.domain.ids import new_id
from crucible.domain.run import Run
from crucible.domain.steps import Step
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelGateway,
    ModelStop,
    ModelStopReason,
    ModelUsage,
    ReasoningDelta,
    TextDelta,
)
from crucible.engine.journal import (
    EventSpec,
    JournalMutation,
    JournalMutationRejected,
    RunJournal,
    claim_run_mutation,
    completed_message_mutation,
    message_completed_mutation,
    terminal_run_mutation,
)
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.registry import default_registry


class RunBudgetExceeded(Exception):
    pass


class RunEngine:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        gateway: ModelGateway,
        notifier: EventNotifier | None = None,
        process_execution_id: UUID | None = None,
        journal: RunJournal | None = None,
        context_manager: ContextManager | None = None,
        dispatcher: ToolDispatcher | None = None,
        max_steps: int = 20,
        max_tool_calls: int = 100,
        max_model_tokens: int = 200_000,
        max_active_seconds: float = 600,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._gateway = gateway
        self._notifier = notifier
        self.process_execution_id = process_execution_id or new_id()
        self._journal = journal or RunJournal(unit_of_work, clock, notifier)
        registry = default_registry()
        self._dispatcher = dispatcher or ToolDispatcher(
            registry, unit_of_work, clock, journal=self._journal
        )
        self._max_steps = max_steps
        self._max_tool_calls = max_tool_calls
        self._max_model_tokens = max_model_tokens
        self._max_active_seconds = max_active_seconds
        self._context_manager = context_manager or ContextManager(
            unit_of_work,
            clock,
            SimpleTokenEstimator(),
            harness_policy="Follow harness safety and execution policy.",
            tool_contract="Use only the structured tools supplied by the harness.",
            model="fake",
            input_limit=100_000,
            output_reserve=1024,
            tools=registry.definitions,
            journal=self._journal,
        )

    async def execute(self, run_id: UUID) -> bool:
        now = self._clock.now()
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return False
        try:
            await self._journal.record(
                claim_run_mutation(run, self.process_execution_id, now)
            )
        except JournalMutationRejected:
            return False
        async with self._unit_of_work() as uow:
            claimed_run = await uow.runs.get(run_id)
        if claimed_run is None:
            return False
        run = claimed_run

        async with self._unit_of_work() as uow:
            steps = await uow.steps.list_for_run(run.id)
            existing_calls = 0
            for step in steps:
                existing_calls += len(await uow.tool_calls.list_for_step(step.id))
        used_calls = existing_calls
        used_tokens = 0
        started = time.monotonic()
        for step_sequence in range(1, self._max_steps + 1):
            try:
                if time.monotonic() - started > self._max_active_seconds:
                    raise RunBudgetExceeded(
                        "Run exceeded configured active-time budget"
                    )
                remaining_seconds = self._max_active_seconds - (
                    time.monotonic() - started
                )
                async with asyncio.timeout(remaining_seconds):
                    should_continue, step_calls, step_tokens = await self._execute_step(
                        run,
                        step_sequence,
                        call_budget=max(0, self._max_tool_calls - used_calls),
                        token_budget=max(0, self._max_model_tokens - used_tokens),
                    )
                used_calls += step_calls
                used_tokens += step_tokens
            except ContextLimitExceeded as error:
                await self._persist_failure(run_id, error, code="context_limit")
                return True
            except RunBudgetExceeded as error:
                await self._persist_failure(run_id, error, code="budget_exhausted")
                return True
            except TimeoutError as error:
                await self._persist_failure(
                    run_id,
                    error,
                    code="budget_exhausted",
                )
                return True
            except asyncio.CancelledError as error:
                await self._persist_failure(
                    run_id,
                    error,
                    code="cancelled",
                    event_type=EventType.RUN_INTERRUPTED,
                )
                raise
            except Exception as error:
                await self._persist_failure(run_id, error)
                return True
            if not should_continue:
                return True
        await self._persist_failure(
            run_id,
            RuntimeError("Run exceeded configured Step budget"),
            code="budget_exhausted",
        )
        return True

    async def _execute_step(
        self,
        run: Run,
        step_sequence: int,
        *,
        call_budget: int,
        token_budget: int,
    ) -> tuple[bool, int, int]:
        step = Step.preparing(
            new_id(), run.task_id, run.id, step_sequence, self._clock.now()
        )
        await self._record_step(step, EventType.STEP_PREPARING, add=True)
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(run.task_id)
        if task is None:
            raise ValueError(f"Task not found: {run.task_id}")

        prepared = await self._context_manager.prepare(run, step)
        active_step = step.activate_model(self._clock.now())
        await self._record_step(active_step, EventType.STEP_MODEL_ACTIVE)

        text: list[str] = []
        reasoning: list[str] = []
        calls: list[CompleteToolCall] = []
        stop: ModelStop | None = None
        model_tokens = 0
        usage_reported = False
        async for item in self._gateway.stream(prepared.request):
            if isinstance(item, TextDelta):
                text.append(item.text)
            elif isinstance(item, ReasoningDelta):
                reasoning.append(item.text)
            elif isinstance(item, CompleteToolCall):
                calls.append(item)
            elif isinstance(item, ModelStop):
                stop = item
            elif isinstance(item, ModelError):
                raise RuntimeError(f"{item.code}: {item.detail}")
            elif isinstance(item, ModelUsage):
                usage_reported = True
                model_tokens += (item.input_tokens or 0) + (item.output_tokens or 0)

        if stop is None:
            raise RuntimeError("Model stream ended without a terminal stop item")
        if not usage_reported:
            output_characters = sum(map(len, text)) + sum(map(len, reasoning))
            model_tokens = prepared.manifest.estimated_tokens + max(
                1, (output_characters + 3) // 4
            )
        if model_tokens > token_budget:
            raise RunBudgetExceeded("Run exceeded configured model-token budget")

        now = self._clock.now()
        message_id = new_id()
        parts: list[MessagePart] = []
        if text:
            parts.append(
                MessagePart(
                    new_id(), len(parts) + 1, MessagePartKind.TEXT, "".join(text)
                )
            )
        if reasoning:
            parts.append(
                MessagePart(
                    new_id(),
                    len(parts) + 1,
                    MessagePartKind.REASONING,
                    None,
                    reasoning_content="".join(reasoning),
                )
            )
        for call in calls:
            parts.append(
                MessagePart(
                    new_id(),
                    len(parts) + 1,
                    MessagePartKind.TOOL_CALL,
                    None,
                    tool_call_id=call.id,
                )
            )
        assistant = Message(
            id=message_id,
            task_id=run.task_id,
            run_id=run.id,
            step_id=step.id,
            conversation_sequence=0,
            role=MessageRole.ASSISTANT,
            status=MessageStatus.COMPLETED,
            parts=tuple(parts),
            created_at=now,
            completed_at=now,
        )
        if not calls:
            if stop is not None and stop.reason is ModelStopReason.TOOL_CALLS:
                raise RuntimeError(
                    "Model stopped for Tool Calls without emitting a call"
                )
            await self._record_step(active_step.complete(now), EventType.STEP_COMPLETED)
            await self._journal.record(completed_message_mutation(run, assistant, now))
            return False, 0, model_tokens

        tools_step = active_step.activate_tools(now)
        await self._record_step(tools_step, EventType.STEP_TOOLS_ACTIVE)
        results = await self._dispatcher.execute_batch(
            DispatchContext(
                run.task_id,
                run.id,
                step.id,
                assistant.id,
                task.workspace_path,
            ),
            tuple(calls),
            call_budget=call_budget,
            assistant_message=assistant,
        )
        tool_message = Message(
            id=new_id(),
            task_id=run.task_id,
            run_id=run.id,
            step_id=step.id,
            conversation_sequence=0,
            role=MessageRole.TOOL,
            status=MessageStatus.COMPLETED,
            parts=tuple(
                MessagePart(
                    new_id(),
                    sequence,
                    MessagePartKind.TOOL_RESULT,
                    result.display_text,
                    tool_result_id=result.id,
                )
                for sequence, result in enumerate(results, 1)
            ),
            created_at=self._clock.now(),
            completed_at=self._clock.now(),
        )
        await self._journal.record(message_completed_mutation(run, tool_message))
        await self._record_step(
            tools_step.complete(self._clock.now()), EventType.STEP_COMPLETED
        )
        return True, len(calls), model_tokens

    async def _record_step(
        self, step: Step, event_type: EventType, *, add: bool = False
    ) -> None:
        async def apply(uow: UnitOfWork) -> None:
            if add:
                await uow.steps.add(step)
            else:
                await uow.steps.update(step)

        await self._journal.record(
            JournalMutation(
                step.task_id,
                step.run_id,
                apply,
                (EventSpec(event_type, {"step_id": str(step.id)}),),
            )
        )

    async def _persist_failure(
        self,
        run_id: UUID,
        error: BaseException,
        *,
        code: str = "model_gateway_error",
        event_type: EventType = EventType.RUN_FAILED,
    ) -> None:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return
            now = self._clock.now()
            steps = await uow.steps.list_for_run(run_id)
            active_step = next(
                (step for step in reversed(steps) if step.completed_at is None), None
            )
        await self._journal.record(
            terminal_run_mutation(
                run,
                type=event_type,
                code=code,
                detail=str(error)[:1000],
                now=now,
                step=active_step,
            )
        )
