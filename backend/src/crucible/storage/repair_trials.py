"""Transactional immutable evidence storage; checkpoints only advance once."""

from __future__ import annotations

from dataclasses import asdict, replace
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.repair.trials import TrialEvidence, TrialJob
from crucible.storage import models

if TYPE_CHECKING:
    from crucible.repair.incident_suite import AcceptedIncident


class SqlAlchemyTrialStore:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add_job(self, job: TrialJob) -> None:
        values = asdict(job)
        for key in ("id", "invocation_id", "repair_id", "observation_artifact_id"):
            values[key] = str(values[key]) if values[key] is not None else None
        await self.session.execute(insert(models.repair_trial_jobs).values(**values))

    async def get_job(self, job_id: UUID) -> TrialJob | None:
        row = (
            (
                await self.session.execute(
                    select(models.repair_trial_jobs).where(
                        models.repair_trial_jobs.c.id == str(job_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        values = dict(row)
        for key in ("id", "invocation_id", "repair_id", "observation_artifact_id"):
            values[key] = UUID(values[key]) if values[key] else None
        return TrialJob(**values)

    async def list_jobs(self, invocation_id: UUID) -> tuple[TrialJob, ...]:
        ids = (
            await self.session.execute(
                select(models.repair_trial_jobs.c.id)
                .where(models.repair_trial_jobs.c.invocation_id == str(invocation_id))
                .order_by(
                    models.repair_trial_jobs.c.started_at, models.repair_trial_jobs.c.id
                )
            )
        ).scalars()
        jobs = []
        for value in ids:
            job = await self.get_job(UUID(value))
            if job is not None:
                jobs.append(job)
        return tuple(jobs)

    async def update_job(self, job: TrialJob) -> None:
        current = await self.get_job(job.id)
        if (
            current is None
            or current.status != "running"
            or job.status not in {"completed", "interrupted"}
        ):
            raise ValueError("Trial checkpoint is immutable after termination")
        if (
            replace(
                job,
                status=current.status,
                completed_at=current.completed_at,
                observation_artifact_id=current.observation_artifact_id,
            )
            != current
        ):
            raise ValueError("Trial identity is immutable")
        await self.session.execute(
            update(models.repair_trial_jobs)
            .where(
                models.repair_trial_jobs.c.id == str(job.id),
                models.repair_trial_jobs.c.status == "running",
            )
            .values(
                status=job.status,
                completed_at=job.completed_at,
                observation_artifact_id=str(job.observation_artifact_id)
                if job.observation_artifact_id
                else None,
            )
        )

    async def add_result(
        self, result: TrialEvidence, termination: str = "completed"
    ) -> None:
        job = await self.get_job(result.id)
        if (
            job is None
            or job.status != "running"
            or (
                result.fingerprint,
                result.case_id,
                result.repeat_index,
                result.side,
                result.source_digest,
            )
            != (
                job.fingerprint,
                job.case_id,
                job.repeat_index,
                job.side,
                job.source_digest,
            )
        ):
            raise ValueError("Result does not match a running trial")
        await self.session.execute(
            insert(models.repair_trial_results).values(
                job_id=str(result.id),
                verdict=result.verdict,
                assertions_json=list(result.assertion_results),
                usage_json=result.usage,
                termination=termination,
                created_at=result.created_at,
            )
        )

    async def list_results(
        self, fingerprint: str, case_id: str, side: str
    ) -> tuple[TrialEvidence, ...]:
        j, r = models.repair_trial_jobs, models.repair_trial_results
        rows = (
            await self.session.execute(
                select(
                    j,
                    r.c.verdict,
                    r.c.assertions_json,
                    r.c.usage_json,
                    r.c.created_at.label("result_created_at"),
                )
                .join(r, r.c.job_id == j.c.id)
                .where(
                    j.c.fingerprint == fingerprint,
                    j.c.case_id == case_id,
                    j.c.side == side,
                )
                .order_by(r.c.created_at, j.c.id)
            )
        ).mappings()
        return tuple(
            TrialEvidence(
                UUID(row["id"]),
                row["fingerprint"],
                row["case_id"],
                row["repeat_index"],
                row["side"],
                row["source_digest"],
                UUID(row["observation_artifact_id"])
                if row["observation_artifact_id"]
                else None,
                row["verdict"],
                tuple(row["assertions_json"]),
                row["usage_json"],
                row["result_created_at"],
            )
            for row in rows
        )

    async def add_incident(self, record: "AcceptedIncident") -> None:
        values = asdict(record)
        for key in ("definition_artifact_id", "fixture_manifest_artifact_id"):
            values[key] = str(values[key])
        await self.session.execute(
            insert(models.repair_incident_cases).values(**values)
        )

    async def list_incidents(self) -> tuple["AcceptedIncident", ...]:
        from crucible.repair.incident_suite import AcceptedIncident

        rows = (
            await self.session.execute(
                select(models.repair_incident_cases).order_by(
                    models.repair_incident_cases.c.case_digest
                )
            )
        ).mappings()
        return tuple(
            AcceptedIncident(
                row["case_digest"],
                UUID(row["definition_artifact_id"]),
                UUID(row["fixture_manifest_artifact_id"]),
                row["accepted_commit"],
                row["created_at"],
            )
            for row in rows
        )
