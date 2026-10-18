"""Dedicated WAL ledger. Transactions end before any external effect begins."""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Literal
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from crucible.improvement.contracts import ExperimentManifest, UsageAmounts
from crucible.improvement.manifest import ManifestDrift, manifest_digest

Status = Literal["ready", "running", "stopping", "interrupted", "error", "closed"]
Stage = Literal[
    "admission", "baseline", "awaiting_search", "candidate_checks", "candidate_trials"
]


class BudgetDenied(ValueError):
    pass


class RevisionConflict(ValueError):
    pass


@dataclass(frozen=True)
class ExperimentRecord:
    id: UUID
    manifest: ExperimentManifest
    manifest_hash: str
    initial_commit: str
    last_accepted_commit: str
    status: Status
    stage: Stage
    revision: int
    stop_requested: bool


_SCHEMA = (
    """CREATE TABLE experiment_runs (
id TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, manifest_hash TEXT NOT
NULL,
initial_commit TEXT NOT NULL, last_accepted_commit TEXT NOT NULL,
status TEXT NOT NULL, stage TEXT NOT NULL, revision INTEGER NOT NULL,
owner_execution_id TEXT, owner_heartbeat_at TEXT, stop_requested INTEGER
NOT NULL DEFAULT 0,
worktree_path TEXT, branch TEXT, error_code TEXT, created_at TEXT NOT NULL
DEFAULT CURRENT_TIMESTAMP,
updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, idempotency_key TEXT
NOT NULL UNIQUE)""",
    """CREATE TABLE experiment_snapshots (
id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES
experiment_runs(id),
parent_commit TEXT NOT NULL, kind TEXT NOT NULL, source_digest TEXT NOT
NULL,
export_digest TEXT NOT NULL, patch_digest TEXT, artifact_ref TEXT NOT
NULL, status TEXT NOT NULL,
UNIQUE(experiment, source_digest, export_digest))""",
    """CREATE TABLE experiment_trials (
id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES
experiment_runs(id),
snapshot TEXT NOT NULL REFERENCES experiment_snapshots(id), case_id TEXT
NOT NULL,
repeat_index INTEGER NOT NULL, key TEXT NOT NULL, status TEXT NOT NULL,
generation INTEGER NOT NULL,
worker_job_id TEXT, artifact_ref TEXT, grade_verdict TEXT,
solver_termination TEXT, error_code TEXT,
UNIQUE(experiment, snapshot, case_id, repeat_index, generation))""",
    """CREATE TABLE experiment_effects (
id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES
experiment_runs(id),
trial TEXT REFERENCES experiment_trials(id), kind TEXT NOT NULL,
intent_json TEXT NOT NULL,
ownership_labels_json TEXT NOT NULL, status TEXT NOT NULL, external_id
TEXT,
created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT
NULL DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE experiment_usage (
reservation_id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES
experiment_runs(id),
trial TEXT REFERENCES experiment_trials(id), kind TEXT NOT NULL,
requested_json TEXT NOT NULL, settled_json TEXT, status TEXT NOT NULL)""",
    """CREATE TABLE experiment_events (
experiment TEXT NOT NULL REFERENCES experiment_runs(id), sequence INTEGER
NOT NULL,
type TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL
DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY(experiment, sequence))""",
    """CREATE TABLE experiment_repository_lock (
repository_identity TEXT PRIMARY KEY, experiment_id TEXT NOT NULL UNIQUE
REFERENCES experiment_runs(id))""",
)


class ExperimentStore:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    @classmethod
    async def open(cls, path: Path) -> "ExperimentStore":
        if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError("invalid controller database path")
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_async_engine(
            "sqlite+aiosqlite:///" + str(path), connect_args={"timeout": 30}
        )
        store = cls(engine)
        try:
            async with engine.connect() as conn:
                await conn.execute(text("PRAGMA journal_mode=WAL"))
                await conn.commit()
            async with store._transaction() as conn:
                version = (await conn.execute(text("PRAGMA user_version"))).scalar_one()
                if version == 0:
                    existing = (
                        await conn.execute(
                            text("SELECT name FROM sqlite_master WHERE type='table'")
                        )
                    ).all()
                    if existing:
                        raise ValueError("unversioned existing database")
                    for sql in _SCHEMA:
                        await conn.execute(text(sql))
                    await conn.execute(text("PRAGMA user_version=1"))
                elif version != 1:
                    raise ValueError("unsupported experiment schema version")
            path.chmod(0o600)
            return store
        except BaseException:
            await engine.dispose()
            raise

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.connect() as conn:
            await conn.execute(text("PRAGMA foreign_keys=ON"))
            await conn.execute(text("BEGIN IMMEDIATE"))
            try:
                yield conn
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def close(self) -> None:
        await self.engine.dispose()

    async def _get(self, conn: AsyncConnection, id: UUID) -> ExperimentRecord:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM experiment_runs WHERE id=:id"), {"id": str(id)}
                )
            )
            .mappings()
            .one()
        )
        return ExperimentRecord(
            UUID(row["id"]),
            ExperimentManifest.model_validate_json(row["manifest_json"]),
            row["manifest_hash"],
            row["initial_commit"],
            row["last_accepted_commit"],
            row["status"],
            row["stage"],
            row["revision"],
            bool(row["stop_requested"]),
        )

    async def get(self, id: UUID) -> ExperimentRecord:
        async with self.engine.connect() as conn:
            return await self._get(conn, id)

    async def _event(
        self, conn: AsyncConnection, id: UUID, event: str, payload: dict[str, object]
    ) -> None:
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 65536 or len(event) > 200:
            raise ValueError("oversized event")
        await conn.execute(
            text("""INSERT INTO experiment_events(experiment,sequence,type,payload_json)
SELECT :id,COALESCE(MAX(sequence),0)+1,:type,:payload FROM
experiment_events WHERE experiment=:id"""),
            {"id": str(id), "type": event, "payload": encoded},
        )

    async def create(
        self,
        manifest: ExperimentManifest,
        repository_identity: str,
        idempotency_key: str,
    ) -> ExperimentRecord:
        if not repository_identity or not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("explicit repository and idempotency identities required")
        # Revalidate: model_copy is deliberately not a validation boundary.
        manifest = ExperimentManifest.model_validate_json(manifest.model_dump_json())
        hashed = manifest_digest(manifest)
        async with self._transaction() as conn:
            previous = (
                await conn.execute(
                    text(
                        "SELECT id,manifest_hash FROM experiment_runs WHERE "
                        "idempotency_key=:key"
                    ),
                    {"key": idempotency_key},
                )
            ).first()
            if previous:
                lock = (
                    await conn.execute(
                        text(
                            "SELECT repository_identity FROM "
                            "experiment_repository_lock WHERE experiment_id=:id"
                        ),
                        {"id": previous.id},
                    )
                ).scalar_one_or_none()
                if previous.manifest_hash != hashed or lock != repository_identity:
                    raise ManifestDrift("idempotency identity changed")
                return await self._get(conn, UUID(previous.id))
            if (
                await conn.execute(
                    text(
                        "SELECT experiment_id FROM experiment_repository_lock "
                        "WHERE repository_identity=:repo"
                    ),
                    {"repo": repository_identity},
                )
            ).first():
                raise RevisionConflict("repository already owned")
            await conn.execute(
                text("""INSERT INTO experiment_runs
(id,manifest_json,manifest_hash,initial_commit,last_accepted_commit,status
,stage,revision,idempotency_key)
VALUES (:id,:manifest,:hash,:commit,:commit,'ready','admission',0,:key)"""),
                {
                    "id": str(manifest.experiment_id),
                    "manifest": manifest.model_dump_json(),
                    "hash": hashed,
                    "commit": manifest.initial_commit,
                    "key": idempotency_key,
                },
            )
            await conn.execute(
                text("INSERT INTO experiment_repository_lock VALUES (:repo,:id)"),
                {"repo": repository_identity, "id": str(manifest.experiment_id)},
            )
            await self._event(
                conn, manifest.experiment_id, "created", {"manifest_hash": hashed}
            )
            return await self._get(conn, manifest.experiment_id)

    async def transition(
        self, id: UUID, expected_revision: int, status: Status, stage: Stage, event: str
    ) -> ExperimentRecord:
        if status not in (
            "ready",
            "running",
            "stopping",
            "interrupted",
            "error",
            "closed",
        ) or stage not in (
            "admission",
            "baseline",
            "awaiting_search",
            "candidate_checks",
            "candidate_trials",
        ):
            raise ValueError("unknown state")
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.revision != expected_revision or record.status == "closed":
                raise RevisionConflict("stale revision or closed experiment")
            if status == "closed":
                active = (
                    await conn.execute(
                        text(
                            "SELECT id FROM experiment_effects WHERE experiment=:id "
                            "AND status!='stopped'"
                        ),
                        {"id": str(id)},
                    )
                ).first()
                if active:
                    raise RevisionConflict("owned effects not stopped")
                await conn.execute(
                    text(
                        "DELETE FROM experiment_repository_lock WHERE experiment_id=:id"
                    ),
                    {"id": str(id)},
                )
            await conn.execute(
                text("""UPDATE experiment_runs SET status=:status,stage=:stage,
revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=:id"""),
                {"id": str(id), "status": status, "stage": stage},
            )
            await self._event(conn, id, event, {"status": status, "stage": stage})
            return await self._get(conn, id)

    async def reserve_usage(
        self,
        id: UUID,
        trial_id: UUID | None,
        request: UsageAmounts,
        *,
        kind: Literal["model", "command", "worker"] = "model",
    ) -> UUID:
        request = UsageAmounts.model_validate_json(request.model_dump_json())
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.stop_requested or record.status not in ("ready", "running"):
                raise BudgetDenied("experiment is stopped")
            totals = {name: 0 for name in UsageAmounts.model_fields}
            rows = (
                await conn.execute(
                    text(
                        "SELECT requested_json,settled_json,status FROM "
                        "experiment_usage WHERE experiment=:id"
                    ),
                    {"id": str(id)},
                )
            ).all()
            for row in rows:
                if row.status == "uncertain":
                    raise BudgetDenied("uncertain usage requires explicit recovery")
                amounts = UsageAmounts.model_validate_json(
                    row.settled_json if row.status == "settled" else row.requested_json
                )
                for name in totals:
                    totals[name] += getattr(amounts, name)
            caps = dict(
                tokens=record.manifest.budget.max_reported_and_reserved_tokens,
                requests=record.manifest.budget.max_model_requests,
                active_seconds=record.manifest.budget.max_active_seconds,
                output_bytes=record.manifest.budget.max_total_output_bytes,
                trial_starts=record.manifest.budget.max_trial_starts,
                worker_wall_seconds=record.manifest.budget.max_worker_wall_seconds,
            )
            if any(totals[name] + getattr(request, name) > caps[name] for name in caps):
                raise BudgetDenied("frozen experiment budget exceeded")
            reservation = uuid4()
            await conn.execute(
                text("""INSERT INTO experiment_usage
(reservation_id,experiment,trial,kind,requested_json,status) VALUES
(:rid,:id,:trial,:kind,:request,'reserved')"""),
                {
                    "rid": str(reservation),
                    "id": str(id),
                    "trial": str(trial_id) if trial_id else None,
                    "kind": kind,
                    "request": request.model_dump_json(),
                },
            )
            await self._event(
                conn, id, "usage_reserved", {"reservation_id": str(reservation)}
            )
            return reservation

    async def settle_usage(self, reservation_id: UUID, actual: UsageAmounts) -> None:
        actual = UsageAmounts.model_validate_json(actual.model_dump_json())
        async with self._transaction() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_usage WHERE reservation_id=:rid"
                        ),
                        {"rid": str(reservation_id)},
                    )
                )
                .mappings()
                .one()
            )
            if row["status"] == "uncertain":
                raise RevisionConflict("uncertain usage cannot be rewritten")
            if row["status"] == "settled":
                if row["settled_json"] != actual.model_dump_json():
                    raise RevisionConflict("usage already settled differently")
                return
            reserved = UsageAmounts.model_validate_json(row["requested_json"])
            exceeded = any(
                getattr(actual, n) > getattr(reserved, n)
                for n in UsageAmounts.model_fields
            )
            await conn.execute(
                text(
                    "UPDATE experiment_usage SET "
                    "settled_json=:actual,status=:status WHERE "
                    "reservation_id=:rid"
                ),
                {
                    "actual": actual.model_dump_json(),
                    "status": "uncertain" if exceeded else "settled",
                    "rid": str(reservation_id),
                },
            )
            if exceeded:
                # Preserve actual overage; future admission stays fail-closed.
                await conn.execute(
                    text(
                        "UPDATE experiment_runs SET "
                        "status='error',stop_requested=1,revision=revision+1 "
                        "WHERE id=:id"
                    ),
                    {"id": row["experiment"]},
                )
            await self._event(
                conn,
                UUID(row["experiment"]),
                "usage_settled",
                {"reservation_id": str(reservation_id), "uncertain": exceeded},
            )

    async def request_stop(self, id: UUID) -> ExperimentRecord:
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.status != "closed":
                await conn.execute(
                    text(
                        "UPDATE experiment_runs SET "
                        "stop_requested=1,status='stopping',revision=revision+1 "
                        "WHERE id=:id"
                    ),
                    {"id": str(id)},
                )
                await self._event(conn, id, "stop_requested", {})
            return await self._get(conn, id)
