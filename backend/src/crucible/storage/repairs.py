"""Repair records and a SQLite write-serialized global ownership invariant."""

from dataclasses import asdict, fields
from datetime import datetime, timedelta
from typing import Any, TypeVar
from uuid import UUID

from sqlalchemy import Table, insert, literal_column, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.domain.repair import (
    TERMINAL_STATUSES,
    RepairAcceptance,
    RepairAttempt,
    RepairCheckpoint,
    RepairFix,
    RepairOwnership,
    RepairPublication,
    RepairRun,
    RepairStage,
    RepairStatus,
)
from crucible.repair.trial_contracts import TrialJob
from crucible.storage import models
from crucible.storage.repair_trials import SqlAlchemyTrialStore

T = TypeVar(
    "T",
    RepairRun,
    RepairAttempt,
    RepairCheckpoint,
    RepairFix,
    RepairAcceptance,
    RepairPublication,
)
_UUID_FIELDS = {
    "id",
    "repair_id",
    "source_task_id",
    "current_fix_id",
    "predecessor_repair_id",
    "fix_id",
    "trial_job_id",
    "harness_run_id",
    "output_artifact_id",
}


def _values(record: T) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, UUID) else value
        for key, value in asdict(record).items()
    }


def _record(cls: type[T], row: Any) -> T:
    values = {field.name: row[field.name] for field in fields(cls)}
    for key in _UUID_FIELDS & values.keys():
        if values[key] is not None:
            values[key] = UUID(values[key])
    if cls is RepairRun:
        values["status"] = RepairStatus(values["status"])
        values["stage"] = RepairStage(values["stage"])
    return cls(**values)


class SqlAlchemyRepairStore:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_running_trials(self, repair_id: UUID) -> tuple[TrialJob, ...]:
        ids = (
            await self.session.execute(
                select(models.repair_trial_jobs.c.id).where(
                    models.repair_trial_jobs.c.repair_id == str(repair_id),
                    models.repair_trial_jobs.c.status == "running",
                )
            )
        ).scalars()
        store = SqlAlchemyTrialStore(self.session)
        jobs = []
        for job_id in ids:
            job = await store.get_job(UUID(job_id))
            if job is not None:
                jobs.append(job)
        return tuple(jobs)

    async def attempt_for_run(self, run_id: UUID) -> RepairAttempt | None:
        row = (
            (
                await self.session.execute(
                    select(models.repair_attempts).where(
                        models.repair_attempts.c.harness_run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return _record(RepairAttempt, row) if row is not None else None

    async def lock(self) -> None:
        # This must be the first statement in any admission mutation transaction.
        # A write, rather than SELECT then UPDATE, avoids WAL read-upgrade races.
        await self.session.execute(
            update(models.repair_ownership)
            .where(models.repair_ownership.c.singleton_id == 1)
            .values(singleton_id=1)
        )

    async def _add(self, table: Table, record: T) -> None:
        await self.session.execute(insert(table).values(**_values(record)))

    async def _get(
        self, table: Table, cls: type[T], key: UUID, column: str = "id"
    ) -> T | None:
        row = (
            (
                await self.session.execute(
                    select(table).where(table.c[column] == str(key))
                )
            )
            .mappings()
            .one_or_none()
        )
        return _record(cls, row) if row is not None else None

    async def _update(self, table: Table, record: T, column: str = "id") -> None:
        values = _values(record)
        await self.session.execute(
            update(table).where(table.c[column] == values[column]).values(**values)
        )

    async def _list(
        self, table: Table, cls: type[T], column: str, key: UUID, order: str
    ) -> tuple[T, ...]:
        rows = (
            await self.session.execute(
                select(table)
                .where(table.c[column] == str(key))
                .order_by(
                    literal_column("rowid") if order == "rowid" else table.c[order],
                    literal_column("rowid"),
                )
            )
        ).mappings()
        return tuple(_record(cls, row) for row in rows)

    async def add(self, run: RepairRun) -> None:
        await self._add(models.repair_runs, run)

    async def get(self, repair_id: UUID) -> RepairRun | None:
        return await self._get(models.repair_runs, RepairRun, repair_id)

    async def update(self, run: RepairRun) -> None:
        await self._update(models.repair_runs, run)

    async def list_for_task(self, task_id: UUID) -> tuple[RepairRun, ...]:
        return await self._list(
            models.repair_runs, RepairRun, "source_task_id", task_id, "created_at"
        )

    async def list_unfinished(self) -> tuple[RepairRun, ...]:
        rows = (
            await self.session.execute(
                select(models.repair_runs)
                .where(models.repair_runs.c.status.not_in(tuple(TERMINAL_STATUSES)))
                .order_by(models.repair_runs.c.created_at)
            )
        ).mappings()
        return tuple(_record(RepairRun, row) for row in rows)

    async def owner(self) -> RepairOwnership:
        row = (
            (
                await self.session.execute(
                    select(models.repair_ownership).where(
                        models.repair_ownership.c.singleton_id == 1
                    )
                )
            )
            .mappings()
            .one()
        )
        return RepairOwnership(
            UUID(row["repair_id"]) if row["repair_id"] else None,
            row["owner_execution_id"],
            row["heartbeat_at"],
            row["lease_expires_at"],
        )

    async def claim_global(
        self, repair_id: UUID, execution_id: str, now: datetime
    ) -> bool:
        result = await self.session.execute(
            update(models.repair_ownership)
            .where(
                models.repair_ownership.c.singleton_id == 1,
                models.repair_ownership.c.repair_id.is_(None),
            )
            .values(
                repair_id=str(repair_id),
                owner_execution_id=execution_id,
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=60),
            )
            .returning(models.repair_ownership.c.singleton_id)
        )
        return result.scalar_one_or_none() is not None

    async def heartbeat(
        self, repair_id: UUID, execution_id: str, now: datetime
    ) -> bool:
        result = await self.session.execute(
            update(models.repair_ownership)
            .where(
                models.repair_ownership.c.repair_id == str(repair_id),
                models.repair_ownership.c.owner_execution_id == execution_id,
            )
            .values(heartbeat_at=now, lease_expires_at=now + timedelta(seconds=60))
            .returning(models.repair_ownership.c.singleton_id)
        )
        return result.scalar_one_or_none() is not None

    async def release_global(self, repair_id: UUID) -> None:
        await self.session.execute(
            update(models.repair_ownership)
            .where(models.repair_ownership.c.repair_id == str(repair_id))
            .values(
                repair_id=None,
                owner_execution_id=None,
                heartbeat_at=None,
                lease_expires_at=None,
            )
        )

    async def add_checkpoint(self, record: RepairCheckpoint) -> None:
        await self._add(models.repair_checkpoints, record)

    async def list_checkpoints(self, repair_id: UUID) -> tuple[RepairCheckpoint, ...]:
        return await self._list(
            models.repair_checkpoints,
            RepairCheckpoint,
            "repair_id",
            repair_id,
            "created_at",
        )

    async def add_attempt(self, record: RepairAttempt) -> None:
        await self._add(models.repair_attempts, record)

    async def update_attempt(self, record: RepairAttempt) -> None:
        await self._update(models.repair_attempts, record)

    async def list_attempts(self, repair_id: UUID) -> tuple[RepairAttempt, ...]:
        return await self._list(
            models.repair_attempts, RepairAttempt, "repair_id", repair_id, "started_at"
        )

    async def add_fix(self, record: RepairFix) -> None:
        await self._add(models.repair_fixes, record)

    async def update_fix(self, record: RepairFix) -> None:
        await self._update(models.repair_fixes, record)

    async def list_fixes(self, repair_id: UUID) -> tuple[RepairFix, ...]:
        return await self._list(
            models.repair_fixes, RepairFix, "repair_id", repair_id, "ordinal"
        )

    async def list_acceptances(self, repair_id: UUID) -> tuple[RepairAcceptance, ...]:
        return await self._list(
            models.repair_acceptances, RepairAcceptance, "repair_id", repair_id, "rowid"
        )

    async def add_acceptance(self, record: RepairAcceptance) -> None:
        await self._add(models.repair_acceptances, record)

    async def get_acceptance(self, acceptance_id: UUID) -> RepairAcceptance | None:
        return await self._get(
            models.repair_acceptances, RepairAcceptance, acceptance_id
        )

    async def update_acceptance(self, record: RepairAcceptance) -> None:
        existing = await self.get_acceptance(record.id)
        if existing is None or {
            k: v
            for k, v in _values(existing).items()
            if k not in {"status", "commit_sha"}
        } != {
            k: v
            for k, v in _values(record).items()
            if k not in {"status", "commit_sha"}
        }:
            raise ValueError("Acceptance intent is immutable")
        await self._update(models.repair_acceptances, record)

    async def get_publication(self, repair_id: UUID) -> RepairPublication | None:
        return await self._get(
            models.repair_publications, RepairPublication, repair_id, "repair_id"
        )

    async def update_publication(self, record: RepairPublication) -> None:
        if await self.get_publication(record.repair_id) is None:
            await self._add(models.repair_publications, record)
        else:
            await self._update(models.repair_publications, record, "repair_id")
