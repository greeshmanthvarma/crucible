import logging
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from crucible.application.errors import (
    IdempotencyConflict,
    TaskNotActive,
    TaskNotFound,
)
from crucible.application.idempotency import (
    IdempotencyRecord,
    canonical_request_hash,
)
from crucible.application.ports import EventNotifier, RunSupervisor, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.run import Run, RunStatus
from crucible.domain.task import TaskStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubmittedRun:
    message_id: UUID
    run_id: UUID
    run_status: RunStatus
    response_status: int = 202

    def response_json(self) -> dict[str, object]:
        return {
            "messageId": str(self.message_id),
            "runId": str(self.run_id),
            "runStatus": self.run_status,
        }


class MessageService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        supervisor: RunSupervisor,
        notifier: EventNotifier | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._supervisor = supervisor
        self._notifier = notifier
        self._events = EventFactory()

    async def submit(
        self, task_id: UUID, text: str, idempotency_key: str
    ) -> SubmittedRun:
        scope = f"POST:/api/tasks/{task_id}/messages"
        request_hash = canonical_request_hash({"text": text})
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                return SubmittedRun(
                    message_id=UUID(str(previous.response_json["messageId"])),
                    run_id=UUID(str(previous.response_json["runId"])),
                    run_status=RunStatus(str(previous.response_json["runStatus"])),
                    response_status=previous.response_status,
                )
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            if task.status is not TaskStatus.ACTIVE:
                raise TaskNotActive(f"Task is not active: {task_id}")

            now = self._clock.now()
            message_id, run_id = new_id(), new_id()
            run = Run.queued(
                run_id=run_id,
                task_id=task_id,
                triggering_message_id=None,
                created_at=now,
            )
            await uow.runs.add(run)
            message = await uow.messages.add(
                Message(
                    id=message_id,
                    task_id=task_id,
                    run_id=run_id,
                    conversation_sequence=0,
                    role=MessageRole.USER,
                    status=MessageStatus.COMPLETED,
                    parts=(
                        MessagePart(
                            id=new_id(),
                            part_sequence=1,
                            kind=MessagePartKind.TEXT,
                            text_content=text,
                        ),
                    ),
                    created_at=now,
                    completed_at=now,
                )
            )
            await uow.runs.set_triggering_message(run_id, message.id)
            await uow.events.append(
                self._events.create(
                    task_id=task_id,
                    run_id=run_id,
                    type=EventType.RUN_QUEUED,
                    payload={
                        "run_id": str(run_id),
                        "message_id": str(message_id),
                    },
                    created_at=now,
                )
            )
            result = SubmittedRun(message_id, run_id, RunStatus.QUEUED)
            await uow.idempotency.add(
                IdempotencyRecord(
                    id=new_id(),
                    scope=scope,
                    key=idempotency_key,
                    request_hash=request_hash,
                    response_status=result.response_status,
                    response_json=result.response_json(),
                    created_at=now,
                )
            )
            await uow.commit()

        if self._notifier is not None:
            await self._notifier.notify(task_id)
        try:
            await self._supervisor.submit(run_id)
        except Exception:
            logger.exception(
                "Run submission failed after durable commit",
                extra={"run_id": str(run_id)},
            )
        return result

    async def list(self, task_id: UUID) -> tuple[Message, ...]:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            return await uow.messages.list_for_task(task_id)
