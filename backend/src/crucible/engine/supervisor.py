import asyncio
from collections.abc import Callable
from functools import partial
from uuid import UUID

from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.run import RunStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.journal import RunJournal, cancellation_mutation, recovery_mutation
from crucible.engine.run_engine import RunEngine


class LocalRunSupervisor:
    def __init__(
        self,
        engine: RunEngine,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
        journal: RunJournal | None = None,
        approval_broker: InMemoryApprovalBroker | None = None,
    ) -> None:
        self._engine = engine
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._journal = journal or RunJournal(unit_of_work, clock, notifier)
        self._semaphore = asyncio.Semaphore(1)
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._approval_broker = approval_broker
        self._events = EventFactory()

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
            async with self._unit_of_work() as uow:
                orphaned = await uow.tool_calls.list_without_result_for_run(run.id)
                steps = await uow.steps.list_for_run(run.id)
                active_step = next(
                    (step for step in reversed(steps) if step.completed_at is None),
                    None,
                )
            await self._journal.record(
                recovery_mutation(
                    run,
                    now=now,
                    orphaned_calls=orphaned,
                    active_step=active_step,
                )
            )
        for run in queued:
            await self.submit(run.id)

    async def cancel(self, run_id: UUID) -> None:
        pending = await self._cancel_pending_approvals(run_id)
        if self._approval_broker is not None:
            for approval in pending:
                await self._approval_broker.publish(approval.id, approval.status)
        task = self._tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None or run.status not in (RunStatus.QUEUED, RunStatus.RUNNING):
                return
            outstanding = await uow.tool_calls.list_without_result_for_run(run_id)
            steps = await uow.steps.list_for_run(run_id)
            active_step = next(
                (step for step in reversed(steps) if step.completed_at is None), None
            )
        await self._journal.record(
            cancellation_mutation(
                run,
                now=self._clock.now(),
                outstanding_calls=outstanding,
                active_step=active_step,
            )
        )

    async def _cancel_pending_approvals(self, run_id: UUID) -> tuple[Approval, ...]:
        cancelled = []
        async with self._unit_of_work() as uow:
            for approval in await uow.approvals.list_pending_for_run(run_id):
                decided = approval.cancel("cancelled", self._clock.now())
                await uow.approvals.update(decided)
                await uow.events.append(
                    self._events.create(
                        task_id=decided.task_id,
                        run_id=decided.run_id,
                        type=EventType.APPROVAL_CANCELLED,
                        payload={
                            "approval_id": str(decided.id),
                            "status": ApprovalStatus.CANCELLED.value,
                        },
                        created_at=self._clock.now(),
                    )
                )
                cancelled.append(decided)
            await uow.commit()
        if self._notifier is not None:
            for task_id in {approval.task_id for approval in cancelled}:
                await self._notifier.notify(task_id)
        return tuple(cancelled)

    async def close(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
