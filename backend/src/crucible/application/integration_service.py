import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from crucible.application.errors import (
    IdempotencyConflict,
    RepositoryNotFound,
    ResultRevisionNotFound,
    TaskNotFound,
)
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.results import Integration, IntegrationStatus
from crucible.domain.task import Task
from crucible.workspaces.git import GitClient, TargetSnapshot


@dataclass(frozen=True)
class IntegrationTarget:
    repository_id: UUID
    target_ref: str
    expected_revision: str


class IntegrationTargetLocks:
    """Process-local serialization for mutations of a repository checkout/ref."""

    def __init__(self) -> None:
        self._locks: dict[tuple[UUID, str], asyncio.Lock] = {}

    def for_target(self, repository_id: UUID, target_ref: str) -> asyncio.Lock:
        return self._locks.setdefault((repository_id, target_ref), asyncio.Lock())


_TARGET_LOCKS = IntegrationTargetLocks()


class IntegrationService:
    def __init__(
        self,
        git: GitClient,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
        target_locks: IntegrationTargetLocks = _TARGET_LOCKS,
    ) -> None:
        self._git = git
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._target_locks = target_locks
        self._events = EventFactory()

    async def integrate(
        self,
        result_revision_id: UUID,
        target: IntegrationTarget,
        idempotency_key: str,
    ) -> Integration:
        async with self._target_locks.for_target(
            target.repository_id, target.target_ref
        ):
            return await self._integrate_locked(
                result_revision_id, target, idempotency_key
            )

    async def _integrate_locked(
        self,
        result_revision_id: UUID,
        target: IntegrationTarget,
        idempotency_key: str,
    ) -> Integration:
        async with self._unit_of_work() as uow:
            existing = await uow.integrations.get_by_key(
                result_revision_id, idempotency_key
            )
            if existing is not None:
                if (
                    existing.repository_id != target.repository_id
                    or existing.target_ref != target.target_ref
                    or existing.expected_target_revision != target.expected_revision
                ):
                    raise IdempotencyConflict(
                        "Idempotency key was already used for another target"
                    )
                if existing.status is not IntegrationStatus.PENDING:
                    return existing
            result = await uow.result_revisions.get(result_revision_id)
            repository = await uow.repositories.get(target.repository_id)
            prior_integrations = await uow.integrations.list_for_result(
                result_revision_id
            )
        if result is None:
            raise ResultRevisionNotFound(
                f"Result Revision not found: {result_revision_id}"
            )
        if repository is None:
            raise RepositoryNotFound(f"Repository not found: {target.repository_id}")
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(result.task_id)
        if task is None:
            raise TaskNotFound(f"Task not found: {result.task_id}")
        integration = existing or Integration.pending(
            new_id(),
            result.id,
            target.repository_id,
            target.target_ref,
            target.expected_revision,
            idempotency_key,
            self._clock.now(),
        )
        if existing is None:
            async with self._unit_of_work() as uow:
                await uow.integrations.add(integration)
                await uow.commit()
        if any(
            prior.status is IntegrationStatus.COMPLETED for prior in prior_integrations
        ):
            return await self._fail(
                integration,
                task.id,
                "already_integrated",
                "Result Revision was already integrated",
            )
        if task.repository_id != target.repository_id:
            return await self._fail(
                integration,
                task.id,
                "wrong_repository",
                "Result belongs to another Repository",
            )
        resolved = await self._git.resolve_repository(repository.root_path)
        if resolved.root != repository.root_path.resolve():
            return await self._fail(
                integration,
                task.id,
                "repository_root_mismatch",
                "Registered Repository root no longer resolves canonically",
            )
        snapshot = await self._git.target_snapshot(resolved.root)
        if existing is not None and await self._git.is_applied_cherry_pick(
            resolved.root,
            snapshot.head_revision,
            result.commit_sha,
            target.expected_revision,
        ):
            return await self._complete(
                integration, task, target.expected_revision, snapshot.head_revision
            )
        preflight = await self._preflight(
            resolved.root, snapshot, result.commit_sha, result.parent_revision, target
        )
        if preflight is not None:
            code, detail = preflight
            return await self._fail(
                integration,
                task.id,
                code,
                detail,
                observed_before=snapshot.head_revision,
            )
        applied = await self._git.cherry_pick(resolved.root, result.commit_sha)
        if applied.returncode != 0:
            aborted = await self._git.abort_cherry_pick(resolved.root)
            restored = None
            if aborted.returncode == 0:
                try:
                    restored = await self._git.target_snapshot(resolved.root)
                except Exception:
                    restored = None
            if restored != snapshot:
                return await self._fail(
                    integration,
                    task.id,
                    "integration_recovery_required",
                    (applied.stderr or aborted.stderr)[:4000],
                    status=IntegrationStatus.RECOVERY_REQUIRED,
                    observed_before=snapshot.head_revision,
                )
            return await self._fail(
                integration,
                task.id,
                "integration_conflict",
                applied.stderr[:4000],
                status=IntegrationStatus.CONFLICT,
                observed_before=snapshot.head_revision,
            )
        after = await self._git.target_snapshot(resolved.root)
        applied_exactly = await self._git.is_applied_cherry_pick(
            resolved.root,
            after.head_revision,
            result.commit_sha,
            snapshot.head_revision,
        )
        if (
            not applied_exactly
            or after.current_ref != snapshot.current_ref
            or bool(after.status)
        ):
            return await self._fail(
                integration,
                task.id,
                "integration_recovery_required",
                "Applied Integration could not be verified as a clean exact "
                "cherry-pick",
                status=IntegrationStatus.RECOVERY_REQUIRED,
                observed_before=snapshot.head_revision,
            )
        return await self._complete(
            integration, task, snapshot.head_revision, after.head_revision
        )

    async def _complete(
        self, integration: Integration, task: Task, before: str, after: str
    ) -> Integration:
        completed = integration.succeed(before, after, self._clock.now())
        async with self._unit_of_work() as uow:
            await uow.integrations.update(completed)
            await uow.tasks.update(task.integrate(self._clock))
            await uow.events.append(
                self._events.create(
                    task_id=task.id,
                    run_id=None,
                    type=EventType.TASK_INTEGRATED,
                    payload={"integration_id": str(completed.id)},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(task.id)
        return completed

    async def _preflight(
        self,
        root: Path,
        snapshot: TargetSnapshot,
        commit_sha: str,
        parent_revision: str,
        target: IntegrationTarget,
    ) -> tuple[str, str] | None:
        if snapshot.current_ref != target.target_ref:
            return "target_ref_mismatch", "Target checkout is on a different ref"
        if snapshot.head_revision != target.expected_revision:
            return "target_revision_drift", "Target revision differs from expected"
        if snapshot.status:
            return (
                "target_dirty",
                "Target checkout contains tracked or untracked changes",
            )
        if not await self._git.has_commit(root, commit_sha):
            return "result_unavailable", "Result Revision commit is unavailable"
        if not await self._git.is_ancestor(
            root, parent_revision, snapshot.head_revision
        ):
            return "result_ancestry_mismatch", "Result parent is not target ancestry"
        return None

    async def _fail(
        self,
        integration: Integration,
        task_id: UUID,
        code: str,
        detail: str,
        *,
        status: IntegrationStatus = IntegrationStatus.FAILED,
        observed_before: str | None = None,
    ) -> Integration:
        failed = integration.fail(
            status,
            code,
            detail,
            self._clock.now(),
            observed_before_revision=observed_before,
        )
        event_type = {
            IntegrationStatus.FAILED: EventType.INTEGRATION_FAILED,
            IntegrationStatus.CONFLICT: EventType.INTEGRATION_CONFLICT,
            IntegrationStatus.RECOVERY_REQUIRED: (
                EventType.INTEGRATION_RECOVERY_REQUIRED
            ),
        }[status]
        async with self._unit_of_work() as uow:
            await uow.integrations.update(failed)
            await uow.events.append(
                self._events.create(
                    task_id=task_id,
                    run_id=None,
                    type=event_type,
                    payload={"integration_id": str(failed.id), "failure_code": code},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(task_id)
        return failed
