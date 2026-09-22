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
    TextDelta,
)
from crucible.engine.journal import (
    JournalMutationRejected,
    RunJournal,
    claim_run_mutation,
    completed_message_mutation,
    message_completed_mutation,
    terminal_run_mutation,
)
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.registry import default_registry


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
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._gateway = gateway
        self._notifier = notifier
        self.process_execution_id = process_execution_id or new_id()
        self._journal = journal or RunJournal(unit_of_work, clock, notifier)
        registry = default_registry()
        self._dispatcher = dispatcher or ToolDispatcher(registry, unit_of_work, clock)
        self._max_steps = max_steps
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

        for step_sequence in range(1, self._max_steps + 1):
            try:
                should_continue = await self._execute_step(run, step_sequence)
            except ContextLimitExceeded as error:
                await self._persist_failure(run_id, error, code="context_limit")
                return True
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

    async def _execute_step(self, run: Run, step_sequence: int) -> bool:
        step = Step.preparing(
            new_id(), run.task_id, run.id, step_sequence, self._clock.now()
        )
        async with self._unit_of_work() as uow:
            await uow.steps.add(step)
            task = await uow.tasks.get(run.task_id)
            await uow.commit()
        if task is None:
            raise ValueError(f"Task not found: {run.task_id}")

        prepared = await self._context_manager.prepare(run, step)
        active_step = step.activate_model(self._clock.now())
        async with self._unit_of_work() as uow:
            await uow.steps.update(active_step)
            await uow.commit()

        text: list[str] = []
        calls: list[CompleteToolCall] = []
        stop: ModelStop | None = None
        async for item in self._gateway.stream(prepared.request):
            if isinstance(item, TextDelta):
                text.append(item.text)
            elif isinstance(item, CompleteToolCall):
                calls.append(item)
            elif isinstance(item, ModelStop):
                stop = item
            elif isinstance(item, ModelError):
                raise RuntimeError(f"{item.code}: {item.detail}")

        now = self._clock.now()
        message_id = new_id()
        parts: list[MessagePart] = []
        if text:
            parts.append(
                MessagePart(
                    new_id(), len(parts) + 1, MessagePartKind.TEXT, "".join(text)
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
            await self._journal.record(completed_message_mutation(run, assistant, now))
            async with self._unit_of_work() as uow:
                await uow.steps.update(active_step.complete(now))
                await uow.commit()
            return False

        await self._journal.record(message_completed_mutation(run, assistant))
        tools_step = active_step.activate_tools(now)
        async with self._unit_of_work() as uow:
            await uow.steps.update(tools_step)
            await uow.commit()
        results = await self._dispatcher.execute_batch(
            DispatchContext(
                run.task_id,
                run.id,
                step.id,
                assistant.id,
                task.workspace_path,
            ),
            tuple(calls),
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
        async with self._unit_of_work() as uow:
            await uow.steps.update(tools_step.complete(self._clock.now()))
            await uow.commit()
        return True

    async def _persist_failure(
        self, run_id: UUID, error: Exception, *, code: str = "model_gateway_error"
    ) -> None:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return
            now = self._clock.now()
        await self._journal.record(
            terminal_run_mutation(
                run,
                type=EventType.RUN_FAILED,
                code=code,
                detail=str(error)[:1000],
                now=now,
            )
        )
