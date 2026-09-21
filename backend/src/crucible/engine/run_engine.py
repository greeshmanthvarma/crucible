from collections.abc import Callable
from datetime import timedelta
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id
from crucible.engine.gateway import ModelGateway, ModelRequest


class RunEngine:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        gateway: ModelGateway,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._gateway = gateway

    async def execute(self, run_id: UUID) -> bool:
        now = self._clock.now()
        execution_id = new_id()
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return False
            claimed = await uow.runs.claim_queued(
                run_id, execution_id, now, now + timedelta(minutes=5)
            )
            if not claimed:
                return False
            await uow.events.append(
                self._event(run.task_id, run.id, EventType.RUN_STARTED, now)
            )
            await uow.commit()

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
            message = await uow.messages.add(
                Message(
                    id=new_id(),
                    task_id=run.task_id,
                    run_id=run.id,
                    conversation_sequence=0,
                    role=MessageRole.ASSISTANT,
                    status=MessageStatus.COMPLETED,
                    parts=(MessagePart(new_id(), 1, MessagePartKind.TEXT, response),),
                    created_at=completed_at,
                    completed_at=completed_at,
                )
            )
            await uow.events.append(
                self._event(
                    run.task_id,
                    run.id,
                    EventType.MESSAGE_COMPLETED,
                    completed_at,
                    message_id=str(message.id),
                )
            )
            await uow.runs.update(run.complete(now=completed_at))
            await uow.events.append(
                self._event(run.task_id, run.id, EventType.RUN_COMPLETED, completed_at)
            )
            await uow.commit()
        return True

    async def _persist_failure(self, run_id: UUID, error: Exception) -> None:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                return
            now = self._clock.now()
            await uow.runs.update(
                run.fail("model_gateway_error", str(error)[:1000], now=now)
            )
            await uow.events.append(
                self._event(
                    run.task_id,
                    run.id,
                    EventType.RUN_FAILED,
                    now,
                    outcome_code="model_gateway_error",
                )
            )
            await uow.commit()

    @staticmethod
    def _event(
        task_id: UUID,
        run_id: UUID,
        event_type: EventType,
        created_at: object,
        **payload: object,
    ) -> Event:
        from datetime import datetime

        assert isinstance(created_at, datetime)
        return Event(
            id=new_id(),
            task_id=task_id,
            run_id=run_id,
            task_sequence=0,
            run_sequence=0,
            type=event_type,
            schema_version=1,
            payload={"schema_version": 1, **payload},
            created_at=created_at,
        )
