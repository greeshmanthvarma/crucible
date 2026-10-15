"""Durable manual commands. Scheduling follows the admission transaction."""

import asyncio
import logging
import os
import socket
from collections.abc import Callable
from dataclasses import asdict, replace
from uuid import UUID, uuid4

from pydantic import TypeAdapter

from crucible.application.errors import (
    ApplicationError,
    IdempotencyConflict,
    TaskNotFound,
)
from crucible.application.idempotency import IdempotencyRecord, canonical_request_hash
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.repair import (
    TERMINAL_STATUSES,
    RepairCheckpoint,
    RepairCommandResult,
    RepairRun,
    RepairStage,
    RepairStatus,
)
from crucible.domain.run import RunStatus
from crucible.domain.task import TaskStatus
from crucible.repair.events import append_repair_event
from crucible.repair.identity import RuntimeIdentity
from crucible.repair.ports import (
    RepairAdmissionGitHubPort,
    RepairAdmissionGitPort,
    RepairScheduler,
    RepairWorkerRecovery,
)

logger = logging.getLogger(__name__)


class RepairNotFound(ApplicationError):
    code = "repair_not_found"
    status_code = 404


class RepairStopFailed(ApplicationError):
    code = "repair_stop_failed"
    status_code = 503


class RepairStopInProgress(ApplicationError):
    code = "repair_stop_in_progress"
    status_code = 409


class InvalidRepairCommand(ApplicationError, ValueError):
    code = "invalid_repair_command"


class RepairService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        git: RepairAdmissionGitPort,
        github: RepairAdmissionGitHubPort,
        runtime_identity: RuntimeIdentity,
        *,
        scheduler: RepairScheduler | None = None,
        notifier: EventNotifier | None = None,
        worker_recovery: RepairWorkerRecovery | None = None,
    ) -> None:
        self._uow, self._clock, self._git, self._github = (
            unit_of_work,
            clock,
            git,
            github,
        )
        self._runtime, self._scheduler, self._notifier = (
            runtime_identity,
            scheduler,
            notifier,
        )
        self._worker_recovery = worker_recovery
        self._heartbeat_task: asyncio.Task[None] | None = None
        self.execution_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"

    def bind_scheduler(self, scheduler: RepairScheduler) -> None:
        if self._scheduler is not None:
            raise RuntimeError("Repair scheduler already bound")
        self._scheduler = scheduler

    async def start(self) -> None:
        await self.reconcile()
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(20)
            try:
                async with self._uow() as uow:
                    await uow.repairs.lock()
                    owner = await uow.repairs.owner()
                    if (
                        owner.repair_id
                        and owner.owner_execution_id == self.execution_id
                    ):
                        await uow.repairs.heartbeat(
                            owner.repair_id, self.execution_id, self._clock.now()
                        )
                    await uow.commit()
            except Exception:
                logger.warning("Repair ownership heartbeat failed")

    async def close(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        async with self._uow() as uow:
            owner = await uow.repairs.owner()
        if owner.repair_id and owner.owner_execution_id == self.execution_id:
            if self._scheduler:
                await self._scheduler.stop(owner.repair_id)
            await self.reconcile(lambda execution: execution == self.execution_id)

    async def _replay(
        self, uow: UnitOfWork, scope: str, key: str, request_hash: str
    ) -> RepairCommandResult | None:
        previous = await uow.idempotency.get(scope, key)
        if previous is None:
            return None
        if previous.request_hash != request_hash:
            raise IdempotencyConflict(
                "Idempotency key was already used for a different request"
            )
        return TypeAdapter(RepairCommandResult).validate_python(previous.response_json)

    async def _save_response(
        self,
        uow: UnitOfWork,
        scope: str,
        key: str,
        request_hash: str,
        result: RepairCommandResult,
    ) -> None:
        data = asdict(result)
        data["repair_id"] = str(result.repair_id) if result.repair_id else None
        await uow.idempotency.add(
            IdempotencyRecord(
                uuid4(), scope, key, request_hash, 202, data, self._clock.now()
            )
        )

    async def _event(self, uow: UnitOfWork, run: RepairRun) -> None:
        await append_repair_event(uow, self._clock, run)

    async def _notify(self, task_id: UUID) -> None:
        if self._notifier:
            await self._notifier.notify(task_id)

    async def command(
        self, task_id: UUID, text: str, idempotency_key: str
    ) -> RepairCommandResult:
        command = text.strip()
        if command not in {"/repair", "/repair resume", "/repair close"}:
            raise InvalidRepairCommand(
                "Supported commands: /repair, /repair resume, /repair close"
            )
        scope = f"POST:/api/tasks/{task_id}/messages"
        request_hash = canonical_request_hash({"text": text})
        async with self._uow() as uow:
            previous = await self._replay(uow, scope, idempotency_key, request_hash)
            if previous:
                return previous
            source_task = await uow.tasks.get(task_id)
            if source_task is None or source_task.internal_repair_id is not None:
                raise TaskNotFound(f"Task not found: {task_id}")
        # External reads do not hold the database write lock. Eligibility is rechecked
        # atomically afterward, including same-key replay from another process.
        admission_error = None
        if command != "/repair close":
            try:
                prs = await self._github.list_open_repairs()
                if prs:
                    admission_error = RepairCommandResult(
                        "blocked",
                        None,
                        "open_repair_pr",
                        "Merge or close the existing repair PR first.",
                        tuple(pr.url for pr in prs),
                    )
            except Exception:
                admission_error = RepairCommandResult(
                    "blocked",
                    None,
                    "github_unavailable",
                    "GitHub admission check failed; retry with a new request key.",
                )
            if admission_error is None:
                try:
                    check = await self._git.check_fresh_main()
                    identity = await self._git.main_identity() if check.ok else None
                    if not check.ok:
                        admission_error = RepairCommandResult(
                            "blocked", None, check.code, check.detail
                        )
                    elif identity != self._runtime:
                        admission_error = RepairCommandResult(
                            "blocked",
                            None,
                            "runtime_mismatch",
                            "Update main and restart Crucible before repairing.",
                        )
                except Exception:
                    admission_error = RepairCommandResult(
                        "blocked",
                        None,
                        "freshness_unavailable",
                        "Main freshness unavailable; retry with a new request key.",
                    )
        dispatch = False
        async with self._uow() as uow:
            await uow.repairs.lock()
            previous = await self._replay(uow, scope, idempotency_key, request_hash)
            if previous:
                return previous
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            unfinished = [
                run
                for run in await uow.repairs.list_for_task(task_id)
                if run.status not in TERMINAL_STATUSES
            ]
            current = unfinished[-1] if unfinished else None
            active = await uow.runs.get_nonterminal_for_task(task_id)
            owner = await uow.repairs.owner()
            result: RepairCommandResult
            if active is not None:
                result = RepairCommandResult(
                    "blocked",
                    current.id if current else None,
                    "active_run",
                    "Wait for the coding run to finish.",
                )
            elif command == "/repair close":
                if current is None:
                    result = RepairCommandResult(
                        "blocked",
                        None,
                        "no_resumable_repair",
                        "There is no repair to close.",
                    )
                elif owner.repair_id == current.id or current.status in {
                    RepairStatus.QUEUED,
                    RepairStatus.RUNNING,
                }:
                    result = RepairCommandResult(
                        "blocked",
                        current.id,
                        "stop_first",
                        "Stop the repair before closing it.",
                    )
                else:
                    current = replace(
                        current,
                        status=RepairStatus.CLOSED,
                        updated_at=self._clock.now(),
                        outcome_code="closed",
                        outcome_detail="Repair closed; worktree and evidence retained.",
                    )
                    await uow.repairs.update(current)
                    await self._event(uow, current)
                    result = RepairCommandResult(
                        "closed", current.id, None, current.outcome_detail or "Closed"
                    )
            elif command == "/repair" and current:
                result = RepairCommandResult(
                    "blocked",
                    current.id,
                    "resume_or_close",
                    "Resume or close the existing repair first.",
                )
            elif command == "/repair resume" and current is None:
                result = RepairCommandResult(
                    "blocked",
                    None,
                    "no_resumable_repair",
                    "There is no repair to resume.",
                )
            elif owner.repair_id is not None:
                result = RepairCommandResult(
                    "blocked",
                    current.id if current else None,
                    "repair_busy",
                    "Another repair owns execution; stop it or retry later.",
                )
            elif command == "/repair" and not any(
                run.status is RunStatus.FAILED
                for run in await uow.runs.list_for_task(task_id)
            ):
                result = RepairCommandResult(
                    "nothing_eligible",
                    None,
                    None,
                    "No failed runs are eligible for repair.",
                )
            elif task.status not in {
                TaskStatus.ACTIVE,
                TaskStatus.ACCEPTED,
                TaskStatus.INTEGRATED,
            }:
                result = RepairCommandResult(
                    "blocked", None, "task_not_ready", "Task is not ready for repair."
                )
            elif admission_error is not None:
                result = admission_error
            else:
                now = self._clock.now()
                if current is None:
                    current = RepairRun(
                        uuid4(),
                        task_id,
                        RepairStatus.QUEUED,
                        RepairStage.INTAKE,
                        asdict(self._runtime),
                        None,
                        None,
                        self._runtime.commit,
                        None,
                        now,
                        now,
                    )
                    await uow.repairs.add(current)
                else:
                    # Full checkpoint/source compatibility is enforced by the next
                    # stage coordinator; intake identity is checked here.
                    if current.initial_identity_json != asdict(self._runtime):
                        current = replace(
                            current,
                            status=RepairStatus.INVALIDATED,
                            updated_at=now,
                            outcome_code="source_drift",
                            outcome_detail="Source changed; start a new repair.",
                        )
                        await uow.repairs.update(current)
                        await self._event(uow, current)
                        result = RepairCommandResult(
                            "blocked",
                            current.id,
                            "source_drift",
                            current.outcome_detail or "Source changed",
                        )
                        await self._save_response(
                            uow, scope, idempotency_key, request_hash, result
                        )
                        await uow.commit()
                        await self._notify(task_id)
                        return result
                    current = replace(
                        current,
                        status=RepairStatus.QUEUED,
                        updated_at=now,
                        outcome_code=None,
                        outcome_detail=None,
                    )
                    await uow.repairs.update(current)
                if not await uow.repairs.claim_global(
                    current.id, self.execution_id, now
                ):
                    raise RuntimeError("Repair ownership changed under write lock")
                await uow.repairs.add_checkpoint(
                    RepairCheckpoint(
                        uuid4(),
                        current.id,
                        "intake",
                        None,
                        None,
                        self._runtime.manifest_digest,
                        current.last_accepted_commit,
                        {},
                        {
                            "worktree_provision": "pending"
                            if current.worktree_path is None
                            else "bound"
                        },
                        now,
                    )
                )
                await self._event(uow, current)
                result = RepairCommandResult(
                    "admitted" if command == "/repair" else "resumed",
                    current.id,
                    None,
                    "Repair queued.",
                )
                dispatch = True
            await self._save_response(uow, scope, idempotency_key, request_hash, result)
            await uow.commit()
        await self._notify(task_id)
        if dispatch and result.repair_id:
            await self._provision_and_schedule(result.repair_id)
        return result

    async def _provision_and_schedule(self, repair_id: UUID) -> None:
        run = await self.get(repair_id)
        if run.worktree_path is None:
            try:
                worktree = await self._git.create_worktree(
                    run.id, run.last_accepted_commit
                )
            except (Exception, asyncio.CancelledError) as error:
                async with self._uow() as uow:
                    await uow.repairs.lock()
                    current = await uow.repairs.get(repair_id)
                    if current is not None and current.status in {
                        RepairStatus.QUEUED,
                        RepairStatus.STOPPED,
                    }:
                        current = replace(
                            current,
                            status=RepairStatus.STOPPED
                            if current.status is RepairStatus.STOPPED
                            else RepairStatus.RETRYABLE_ERROR,
                            updated_at=self._clock.now(),
                            outcome_code="worktree_unavailable",
                            outcome_detail="Worktree unavailable; resume to retry.",
                        )
                        await uow.repairs.update(current)
                        await uow.repairs.release_global(repair_id)
                        await self._event(uow, current)
                        await uow.commit()
                await self._notify(run.source_task_id)
                if isinstance(error, asyncio.CancelledError):
                    raise
                return
            async with self._uow() as uow:
                await uow.repairs.lock()
                current = await uow.repairs.get(repair_id)
                if current is None:
                    return
                current = replace(
                    current,
                    branch=worktree.branch,
                    worktree_path=worktree.path,
                    updated_at=self._clock.now(),
                )
                await uow.repairs.update(current)
                if current.status is RepairStatus.STOPPED:
                    await uow.repairs.release_global(repair_id)
                await self._event(uow, current)
                await uow.commit()
            await self._notify(run.source_task_id)
        current = await self.get(repair_id)
        if self._scheduler and current.status is RepairStatus.QUEUED:
            try:
                await self._scheduler.submit(repair_id)
            except Exception:
                # Durable queued ownership must survive an in-process scheduler
                # failure. Never compensate by admitting a second execution.
                logger.warning(
                    "Repair scheduling failed after durable admission",
                    extra={"repair_id": str(repair_id)},
                )

    async def get(self, repair_id: UUID) -> RepairRun:
        async with self._uow() as uow:
            run = await uow.repairs.get(repair_id)
        if run is None:
            raise RepairNotFound("Repair not found")
        return run

    async def public_status(self, run: RepairRun) -> dict[str, object]:
        async with self._uow() as uow:
            attempts = await uow.repairs.list_attempts(run.id)
            publication = await uow.repairs.get_publication(run.id)
        return {
            "repair_id": run.id,
            "status": run.status,
            "stage": run.stage,
            "attempts": {
                role: sum(a.charged for a in attempts if a.role == role)
                for role in {a.role for a in attempts}
            },
            "outcome_code": run.outcome_code,
            "detail": run.outcome_detail,
            "pr_url": publication.pr_url if publication else None,
        }

    async def list_for_task(self, task_id: UUID) -> tuple[RepairRun, ...]:
        async with self._uow() as uow:
            source_task = await uow.tasks.get(task_id)
            if source_task is None or source_task.internal_repair_id is not None:
                raise TaskNotFound("Task not found")
            return await uow.repairs.list_for_task(task_id)

    async def stop(self, repair_id: UUID, idempotency_key: str) -> RepairRun:
        scope = f"POST:/api/repairs/{repair_id}/stop"
        async with self._uow() as uow:
            await uow.repairs.lock()
            previous = await uow.idempotency.get(scope, idempotency_key)
            run = await uow.repairs.get(repair_id)
            if run is None:
                raise RepairNotFound("Repair not found")
            if previous is not None:
                return TypeAdapter(RepairRun).validate_python(previous.response_json)
            if run.outcome_code == "stop_requested":
                raise RepairStopInProgress(
                    "Stop is in progress; retry after it finishes."
                )
            owner = await uow.repairs.owner()
            if owner.repair_id != repair_id and run.status not in {
                RepairStatus.QUEUED,
                RepairStatus.RUNNING,
            }:
                if run.status not in TERMINAL_STATUSES:
                    run = replace(
                        run,
                        status=RepairStatus.STOPPED,
                        updated_at=self._clock.now(),
                        outcome_code="user_stopped",
                        outcome_detail="Repair stopped; resume or close explicitly.",
                    )
                    await uow.repairs.update(run)
                    await self._event(uow, run)
                await uow.idempotency.add(
                    IdempotencyRecord(
                        uuid4(),
                        scope,
                        idempotency_key,
                        canonical_request_hash({}),
                        200,
                        TypeAdapter(RepairRun).dump_python(run, mode="json"),
                        self._clock.now(),
                    )
                )
                await uow.commit()
                await self._notify(run.source_task_id)
                return run
            if run.status not in TERMINAL_STATUSES:
                # Keep global ownership until cancellation completes. Persisting
                # intent first prevents a restart from scheduling this repair.
                run = replace(
                    run,
                    status=RepairStatus.STOPPED,
                    updated_at=self._clock.now(),
                    outcome_code="stop_requested",
                    outcome_detail="Stopping repair execution.",
                )
                await uow.repairs.update(run)
                await self._event(uow, run)
            await uow.commit()
        await self._notify(run.source_task_id)
        # Cancellation may journal role/worker shutdown using another UoW. Never
        # hold SQLite's writer lock while waiting for it.
        if run.status not in TERMINAL_STATUSES:
            try:
                if self._scheduler:
                    await self._scheduler.stop(repair_id)
                await self._stop_trial_jobs(repair_id)
            except (Exception, asyncio.CancelledError) as error:
                async with self._uow() as uow:
                    await uow.repairs.lock()
                    current = await uow.repairs.get(repair_id)
                    if current is not None and current.outcome_code == "stop_requested":
                        current = replace(
                            current,
                            updated_at=self._clock.now(),
                            outcome_code="stop_failed",
                            outcome_detail="Stop failed; retry Stop before resuming.",
                        )
                        await uow.repairs.update(current)
                        await self._event(uow, current)
                    await uow.commit()
                await self._notify(run.source_task_id)
                if isinstance(error, asyncio.CancelledError):
                    raise
                raise RepairStopFailed(
                    "Stop failed; retry Stop before resuming."
                ) from None
        async with self._uow() as uow:
            await uow.repairs.lock()
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                return TypeAdapter(RepairRun).validate_python(previous.response_json)
            run = await uow.repairs.get(repair_id)
            if run is None:
                raise RepairNotFound("Repair not found")
            if run.status not in TERMINAL_STATUSES:
                run = replace(
                    run,
                    status=RepairStatus.STOPPED,
                    updated_at=self._clock.now(),
                    outcome_code="user_stopped",
                    outcome_detail="Repair stopped; resume or close explicitly.",
                )
                await uow.repairs.update(run)
                # An in-flight provision effect releases after binding/failure.
                if run.worktree_path is not None:
                    await uow.repairs.release_global(repair_id)
                await self._event(uow, run)
            await uow.idempotency.add(
                IdempotencyRecord(
                    uuid4(),
                    scope,
                    idempotency_key,
                    canonical_request_hash({}),
                    200,
                    TypeAdapter(RepairRun).dump_python(run, mode="json"),
                    self._clock.now(),
                )
            )
            await uow.commit()
        await self._notify(run.source_task_id)
        return run

    async def _stop_trial_jobs(self, repair_id: UUID) -> None:
        async with self._uow() as uow:
            jobs = await uow.repairs.list_running_trials(repair_id)
        if not jobs:
            return
        if self._worker_recovery is None:
            raise RuntimeError("Worker recovery is unavailable")
        await self._worker_recovery.stop_jobs(tuple(job.id for job in jobs))
        async with self._uow() as uow:
            for job in jobs:
                current = await uow.repair_trials.get_job(job.id)
                if current is not None and current.status == "running":
                    await uow.repair_trials.update_job(
                        replace(
                            current,
                            status="interrupted",
                            completed_at=self._clock.now(),
                        )
                    )
            await uow.commit()

    async def reconcile(
        self, owner_is_dead: Callable[[str], bool] | None = None
    ) -> None:
        """No scheduling. Keep ownership until orphan-worker termination is verified."""
        probe = owner_is_dead or execution_owner_is_dead
        async with self._uow() as uow:
            await uow.repairs.lock()
            owner = await uow.repairs.owner()
            if (
                owner.repair_id is None
                or owner.owner_execution_id is None
                or not probe(owner.owner_execution_id)
            ):
                return
            run = await uow.repairs.get(owner.repair_id)
            if run is None:
                return
            run = replace(
                run,
                status=RepairStatus.INTERRUPTED,
                updated_at=self._clock.now(),
                outcome_code="owner_exited",
                outcome_detail="Previous execution exited; resume explicitly.",
            )
            await uow.repairs.update(run)
            await self._event(uow, run)
            await uow.commit()
        await self._notify(run.source_task_id)
        try:
            await self._stop_trial_jobs(run.id)
        except Exception:
            logger.warning(
                "Repair worker recovery failed; ownership retained",
                extra={"repair_id": str(run.id)},
            )
            return
        async with self._uow() as uow:
            await uow.repairs.lock()
            current_owner = await uow.repairs.owner()
            if (
                current_owner.repair_id != owner.repair_id
                or current_owner.owner_execution_id != owner.owner_execution_id
            ):
                return
            await uow.repairs.release_global(run.id)
            await self._event(uow, run)
            await uow.commit()
        await self._notify(run.source_task_id)


def execution_owner_is_dead(execution_id: str) -> bool:
    try:
        host, pid, _nonce = execution_id.split(":", 2)
        if host != socket.gethostname() or int(pid) <= 0:
            return False
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except (ValueError, PermissionError):
        return False
    return False
