"""Task-scoped immutable evidence metadata and atomic duplicate handling."""

from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.repair.evidence import EvidenceCapture
from crucible.storage import models


class SqlAlchemyEvidenceStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def _authorize(self, run_id: UUID, task_id: UUID) -> None:
        owner = await self._session.scalar(
            select(models.runs.c.task_id).where(models.runs.c.id == str(run_id))
        )
        if owner != str(task_id):
            raise ValueError("Evidence Run does not belong to Task")

    async def add(self, capture: EvidenceCapture) -> EvidenceCapture:
        await self._authorize(capture.run_id, capture.task_id)
        if capture.step_id is not None:
            run = await self._session.scalar(
                select(models.steps.c.run_id).where(
                    models.steps.c.id == str(capture.step_id)
                )
            )
            if run != str(capture.run_id):
                raise ValueError("Evidence Step does not belong to Run")
        if capture.tool_call_id is not None:
            run = await self._session.scalar(
                select(models.tool_calls.c.run_id).where(
                    models.tool_calls.c.id == str(capture.tool_call_id)
                )
            )
            if run != str(capture.run_id):
                raise ValueError("Evidence Tool Call does not belong to Run")
        if capture.artifact_id is not None:
            owner = await self._session.scalar(
                select(models.artifacts.c.task_id).where(
                    models.artifacts.c.id == str(capture.artifact_id)
                )
            )
            if owner != str(capture.task_id):
                raise ValueError("Evidence artifact does not belong to Task")
        await self._session.execute(
            insert(models.repair_evidence)
            .values(
                id=str(capture.id),
                task_id=str(capture.task_id),
                run_id=str(capture.run_id),
                kind=capture.kind,
                artifact_id=str(capture.artifact_id) if capture.artifact_id else None,
                step_id=str(capture.step_id) if capture.step_id else None,
                tool_call_id=str(capture.tool_call_id)
                if capture.tool_call_id
                else None,
                source_key=capture.source_key,
                attempt_index=capture.attempt_index,
                source_ids_json=dict(capture.source_ids),
                manifest_digest=capture.manifest_digest,
                complete=capture.complete,
                unsupported_code=capture.unsupported_code,
                created_at=capture.created_at,
            )
            .on_conflict_do_nothing(
                index_elements=["run_id", "kind", "source_key", "attempt_index"]
            )
        )
        result = await self.get_by_source(
            capture.run_id,
            capture.kind,
            capture.source_key,
            capture.attempt_index,
            task_id=capture.task_id,
        )
        assert result is not None
        return result

    async def get(self, capture_id: UUID, *, task_id: UUID) -> EvidenceCapture | None:
        row = (
            (
                await self._session.execute(
                    select(models.repair_evidence).where(
                        models.repair_evidence.c.id == str(capture_id),
                        models.repair_evidence.c.task_id == str(task_id),
                    )
                )
            )
            .mappings()
            .first()
        )
        return self._capture(dict(row)) if row is not None else None

    async def get_by_source(
        self,
        run_id: UUID,
        kind: str,
        source_key: str,
        attempt_index: int,
        *,
        task_id: UUID,
    ) -> EvidenceCapture | None:
        await self._authorize(run_id, task_id)
        row = (
            (
                await self._session.execute(
                    select(models.repair_evidence).where(
                        models.repair_evidence.c.run_id == str(run_id),
                        models.repair_evidence.c.kind == kind,
                        models.repair_evidence.c.source_key == source_key,
                        models.repair_evidence.c.attempt_index == attempt_index,
                    )
                )
            )
            .mappings()
            .first()
        )
        return self._capture(dict(row)) if row is not None else None

    async def list_for_run(
        self, run_id: UUID, *, task_id: UUID
    ) -> tuple[EvidenceCapture, ...]:
        await self._authorize(run_id, task_id)
        rows = (
            await self._session.execute(
                select(models.repair_evidence)
                .where(models.repair_evidence.c.run_id == str(run_id))
                .order_by(
                    models.repair_evidence.c.created_at, models.repair_evidence.c.id
                )
            )
        ).mappings()
        return tuple(self._capture(dict(row)) for row in rows)

    @staticmethod
    def _capture(row: Mapping[str, Any]) -> EvidenceCapture:
        return EvidenceCapture(
            id=UUID(row["id"]),
            task_id=UUID(row["task_id"]),
            run_id=UUID(row["run_id"]),
            kind=row["kind"],
            artifact_id=UUID(row["artifact_id"]) if row["artifact_id"] else None,
            step_id=UUID(row["step_id"]) if row["step_id"] else None,
            tool_call_id=UUID(row["tool_call_id"]) if row["tool_call_id"] else None,
            source_key=row["source_key"],
            attempt_index=row["attempt_index"],
            complete=bool(row["complete"]),
            unsupported_code=row["unsupported_code"],
            source_ids=row["source_ids_json"],
            manifest_digest=row["manifest_digest"],
            created_at=row["created_at"],
        )
