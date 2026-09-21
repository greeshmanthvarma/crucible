import asyncio
from collections.abc import Callable
from functools import partial
from uuid import UUID

from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.engine.journal import RunJournal, recovery_mutation
from crucible.engine.run_engine import RunEngine


class LocalRunSupervisor:
    def __init__(
        self,
        engine: RunEngine,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
        journal: RunJournal | None = None,
    ) -> None:
        self._engine = engine
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._journal = journal or RunJournal(unit_of_work, clock, notifier)
        self._semaphore = asyncio.Semaphore(1)
        self._tasks: dict[UUID, asyncio.Task[None]] = {}

    async def submit(self, run_id: UUID) -> None:
        if run_id in self._tasks:
            return
        task = asyncio.create_task(self._execute(run_id))
        self._tasks[run_id] = task
        task.add_done_callback(partial(self._finished, run_id))

    async def _execute(self, run_id: UUID) -> None:
        async with self._semaphore:
            await self._engine.execute(run_id)

    def _finished(self, run_id: UUID, task: asyncio.Task[None]) -> None:
        self._tasks.pop(run_id, None)
        if not task.cancelled():
            task.exception()

    async def reconcile(self) -> None:
        async with self._unit_of_work() as uow:
            now = self._clock.now()
            prior_process_runs = await uow.runs.list_running_not_owned_by(
                self._engine.process_execution_id
            )
            queued = await uow.runs.list_queued()
        for run in prior_process_runs:
            await self._journal.record(recovery_mutation(run, now=now))
        for run in queued:
            await self.submit(run.id)

    async def close(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
