from collections.abc import Callable
from uuid import UUID

from crucible.application.ports import EventNotifier, UnitOfWork
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
from crucible.engine.gateway import ModelGateway, ModelRequest
from crucible.engine.journal import (
    JournalMutationRejected,
    RunJournal,
    claim_run_mutation,
    completed_message_mutation,
    terminal_run_mutation,
)


class RunEngine:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        gateway: ModelGateway,
        notifier: EventNotifier | None = None,
        process_execution_id: UUID | None = None,
        journal: RunJournal | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._gateway = gateway
        self._notifier = notifier
        self.process_execution_id = process_execution_id or new_id()
        self._journal = journal or RunJournal(unit_of_work, clock, notifier)

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
            messages = await uow.messages.list_for_task(run.task_id)

        try:
            response = await self._gateway.complete(ModelRequest(messages))
        except Exception as error:
            await self._persist_failure(run_id, error)
            return True

        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return False
            completed_at = self._clock.now()
            message = Message(
                id=new_id(),
                task_id=run.task_id,
                run_id=run.id,
                step_id=None,
                conversation_sequence=0,
                role=MessageRole.ASSISTANT,
                status=MessageStatus.COMPLETED,
                parts=(MessagePart(new_id(), 1, MessagePartKind.TEXT, response),),
                created_at=completed_at,
                completed_at=completed_at,
            )
        await self._journal.record(
            completed_message_mutation(run, message, completed_at)
        )
        return True

    async def _persist_failure(self, run_id: UUID, error: Exception) -> None:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return
            now = self._clock.now()
        await self._journal.record(
            terminal_run_mutation(
                run,
                type=EventType.RUN_FAILED,
                code="model_gateway_error",
                detail=str(error)[:1000],
                now=now,
            )
        )
