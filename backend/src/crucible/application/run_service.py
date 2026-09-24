from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from crucible.application.errors import IdempotencyConflict, RunNotFound
from crucible.application.idempotency import IdempotencyRecord, canonical_request_hash
from crucible.application.ports import EventNotifier, RunSupervisor, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.run import Run, RunStatus


@dataclass(frozen=True)
class CancelledRun:
    run: Run


class RunService:
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

    async def cancel(self, run_id: UUID, idempotency_key: str) -> CancelledRun:
        scope = f"run:{run_id}:cancel"
        request_hash = canonical_request_hash({"code": "cancelled"})
        task_id = None
        replay = False
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                run = await uow.runs.get(run_id)
                if run is None:
                    raise RuntimeError("Idempotency record references a missing Run")
                if run.status not in (RunStatus.QUEUED, RunStatus.RUNNING):
                    return CancelledRun(run)
                task_id = run.task_id
                replay = True
            if not replay:
                run = await uow.runs.get(run_id)
                if run is None:
                    raise RunNotFound(f"Run not found: {run_id}")
                task_id = run.task_id
                now = self._clock.now()
                requested = run.request_cancel("cancelled", now)
                await uow.runs.update(requested)
                if run.status in (RunStatus.QUEUED, RunStatus.RUNNING):
                    await uow.events.append(
                        self._events.create(
                            task_id=run.task_id,
                            run_id=run.id,
                            type=EventType.RUN_CANCEL_REQUESTED,
                            payload={"run_id": str(run.id), "code": "cancelled"},
                            created_at=now,
                        )
                    )
                await uow.idempotency.add(
                    IdempotencyRecord(
                        new_id(),
                        scope,
                        idempotency_key,
                        request_hash,
                        200,
                        {"runId": str(run.id)},
                        now,
                    )
                )
                await uow.commit()
        if self._notifier is not None and task_id is not None:
            await self._notifier.notify(task_id)
        await self._supervisor.cancel(run_id)
        async with self._unit_of_work() as uow:
            terminal = await uow.runs.get(run_id)
        if terminal is None:
            raise RunNotFound(f"Run not found: {run_id}")
        return CancelledRun(terminal)
