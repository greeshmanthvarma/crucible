"""Dedicated WAL ledger. Transactions end before any external effect begins."""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Generic, Literal, Self, cast
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from typing_extensions import TypeVar

from crucible.improvement.contracts import (
    CalibrationManifest,
    ExecutionPolicy,
    ExperimentManifest,
    HarnessSnapshot,
    UsageAmounts,
)
from crucible.improvement.manifest import ManifestDrift, manifest_digest
from crucible.improvement.roles import RoleExecution, SelectionDecision
from crucible.repair.identity import digest

if TYPE_CHECKING:
    from crucible.improvement.publication import PublicationRecord

Status = Literal["ready", "running", "stopping", "interrupted", "error", "closed"]
Stage = Literal[
    "admission",
    "baseline",
    "awaiting_search",
    "candidate_checks",
    "candidate_trials",
    "mining",
    "proposal",
    "promotion",
    "final_evaluation",
    "report",
    "finished",
]


class BudgetDenied(ValueError):
    pass


class QuotaExhausted(BudgetDenied):
    """A known aggregate limit denied a reservation before any effect."""


class RevisionConflict(ValueError):
    pass


Policy = TypeVar("Policy", bound=ExecutionPolicy, default=ExperimentManifest)


@dataclass(frozen=True)
class ExperimentRecord(Generic[Policy]):
    id: UUID
    manifest: Policy
    manifest_hash: str
    initial_commit: str
    last_accepted_commit: str
    status: Status
    stage: Stage
    revision: int
    stop_requested: bool


@dataclass(frozen=True)
class RoundRecord:
    id: UUID
    experiment_id: UUID
    ordinal: int
    baseline_snapshot_id: UUID
    baseline_commit: str
    status: str
    mining_attempts: int
    candidate_snapshot_id: UUID | None
    decision_ref: str | None
    decision_digest: str | None


@dataclass(frozen=True)
class RoleRecord:
    execution: RoleExecution
    attempt: int


@dataclass(frozen=True)
class PromotionRecord:
    id: UUID
    experiment_id: UUID
    round_id: UUID
    expected_parent: str
    candidate_snapshot_id: UUID
    expected_tree: str
    patch_digest: str
    decision_digest: str
    state: Literal["intent", "committed", "complete", "uncertain"]
    commit_sha: str | None


@dataclass(frozen=True)
class SearchSeal:
    experiment_id: UUID
    initial_snapshot_id: UUID
    selected_final_snapshot_id: UUID
    selected_final_commit: str
    accepted_lineage_digest: str
    search_history_digest: str
    stop_reason: str
    created_at: str


_SEARCH_SCHEMA = (
    """CREATE TABLE experiment_rounds (
    id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES experiment_runs(id),
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 3),
    baseline_snapshot TEXT NOT NULL REFERENCES experiment_snapshots(id),
    baseline_commit TEXT NOT NULL, status TEXT NOT NULL,
    mining_attempts INTEGER NOT NULL DEFAULT 0 CHECK(mining_attempts BETWEEN 0 AND 2),
    candidate_snapshot TEXT REFERENCES experiment_snapshots(id),
    decision_ref TEXT, decision_digest TEXT, UNIQUE(experiment,ordinal))""",
    """CREATE TABLE experiment_roles (
    id TEXT PRIMARY KEY, round TEXT NOT NULL REFERENCES experiment_rounds(id),
    kind TEXT NOT NULL, attempt INTEGER NOT NULL CHECK(attempt BETWEEN 0 AND 1),
    execution_json TEXT NOT NULL, UNIQUE(round,kind,attempt))""",
    """CREATE TABLE experiment_promotions (
    id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES experiment_runs(id),
    round TEXT NOT NULL UNIQUE REFERENCES experiment_rounds(id),
    expected_parent TEXT NOT NULL,
    candidate_snapshot TEXT NOT NULL REFERENCES experiment_snapshots(id),
    expected_tree TEXT NOT NULL, patch_digest TEXT NOT NULL,
    decision_digest TEXT NOT NULL,
    state TEXT NOT NULL, commit_sha TEXT)""",
    """CREATE TABLE experiment_search_seals (
    experiment TEXT PRIMARY KEY REFERENCES experiment_runs(id),
    initial_snapshot TEXT NOT NULL REFERENCES experiment_snapshots(id),
    selected_final_snapshot TEXT NOT NULL REFERENCES experiment_snapshots(id),
    selected_final_commit TEXT NOT NULL, accepted_lineage_digest TEXT NOT NULL,
    search_history_digest TEXT NOT NULL, stop_reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE experiment_publications (
    experiment TEXT PRIMARY KEY REFERENCES experiment_runs(id),
    seal_digest TEXT NOT NULL, report_digest TEXT NOT NULL, branch TEXT NOT NULL,
    expected_head TEXT NOT NULL, base_branch TEXT NOT NULL, remote_url TEXT NOT NULL,
    pr_url TEXT, pr_number INTEGER, status TEXT NOT NULL, last_error TEXT)""",
)


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


class ExperimentStore(Generic[Policy]):
    manifest_type: type[ExecutionPolicy] = ExperimentManifest

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    @classmethod
    async def open(cls, path: Path) -> Self:
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
                    version = 1
                if version == 1:
                    for sql in _SEARCH_SCHEMA:
                        await conn.execute(text(sql))
                    await conn.execute(text("PRAGMA user_version=2"))
                    version = 2
                if version == 2:
                    # Semantic accounting version: old binaries must refuse new
                    # conservative/partial rows rather than misprice them.
                    # No historical rows or events are rewritten.
                    await conn.execute(text("PRAGMA user_version=3"))
                elif version != 3:
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

    async def _get(self, conn: AsyncConnection, id: UUID) -> ExperimentRecord[Policy]:
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
            cast(Policy, self.manifest_type.model_validate_json(row["manifest_json"])),
            row["manifest_hash"],
            row["initial_commit"],
            row["last_accepted_commit"],
            row["status"],
            row["stage"],
            row["revision"],
            bool(row["stop_requested"]),
        )

    async def get(self, id: UUID) -> ExperimentRecord[Policy]:
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
        manifest: Policy,
        repository_identity: str,
        idempotency_key: str,
    ) -> ExperimentRecord[Policy]:
        if not repository_identity or not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("explicit repository and idempotency identities required")
        # Revalidate: model_copy is deliberately not a validation boundary.
        manifest = cast(
            Policy, self.manifest_type.model_validate_json(manifest.model_dump_json())
        )
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
                if lock is None:
                    created = (
                        await conn.execute(
                            text(
                                "SELECT payload_json FROM experiment_events "
                                "WHERE experiment=:id AND type='created' "
                                "ORDER BY sequence LIMIT 1"
                            ),
                            {"id": previous.id},
                        )
                    ).scalar_one_or_none()
                    lock = (
                        json.loads(created).get("repository_identity")
                        if created
                        else None
                    )
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
(id,manifest_json,manifest_hash,initial_commit,
last_accepted_commit,status,stage,revision,idempotency_key)
VALUES
(:id,:manifest,:hash,:commit,:commit,'ready','admission',0,:key)"""),
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
                conn,
                manifest.experiment_id,
                "created",
                {"manifest_hash": hashed, "repository_identity": repository_identity},
            )
            return await self._get(conn, manifest.experiment_id)

    async def transition(
        self, id: UUID, expected_revision: int, status: Status, stage: Stage, event: str
    ) -> ExperimentRecord[Policy]:
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
            "mining",
            "proposal",
            "promotion",
            "final_evaluation",
            "report",
            "finished",
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
        job_id: UUID | None = None,
    ) -> UUID:
        request = UsageAmounts.model_validate_json(request.model_dump_json())
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.stop_requested or record.status not in ("ready", "running"):
                raise BudgetDenied("experiment is stopped")
            if trial_id is not None:
                trial = (
                    await conn.execute(
                        text(
                            "SELECT experiment,status FROM experiment_trials "
                            "WHERE id=:trial"
                        ),
                        {"trial": str(trial_id)},
                    )
                ).first()
                if (
                    trial is None
                    or trial.experiment != str(id)
                    or trial.status in ("complete", "error", "interrupted")
                ):
                    raise BudgetDenied("trial generation is no longer active")
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
                    row.settled_json
                    if row.status in ("settled", "conservative")
                    else row.requested_json
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
                raise QuotaExhausted("frozen experiment budget exceeded")
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
                conn,
                id,
                "usage_reserved",
                {
                    "reservation_id": str(reservation),
                    **({"job_id": str(job_id)} if job_id else {}),
                },
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
            if row["status"] in ("uncertain", "conservative"):
                raise RevisionConflict(
                    "uncertain or conservative usage cannot be rewritten"
                )
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

    async def record_partial_usage(
        self, reservation_id: UUID, observed: UsageAmounts, unknown: tuple[str, ...]
    ) -> None:
        """Settle known dimensions while retaining only unknown dimensions' caps."""
        if not set(unknown) <= UsageAmounts.model_fields.keys():
            raise ValueError("unknown usage dimension")
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
            if row["status"] != "reserved":
                raise RevisionConflict("usage already recorded")
            reserved = UsageAmounts.model_validate_json(row["requested_json"])
            charged = UsageAmounts(
                **{
                    name: max(getattr(observed, name), getattr(reserved, name))
                    if name in unknown
                    else getattr(observed, name)
                    for name in UsageAmounts.model_fields
                }
            )
            await conn.execute(
                text(
                    "UPDATE experiment_usage SET status='uncertain',"
                    "settled_json=:charge "
                    "WHERE reservation_id=:rid"
                ),
                {"rid": str(reservation_id), "charge": charged.model_dump_json()},
            )
            await self._event(
                conn,
                UUID(row["experiment"]),
                "usage_partial",
                {
                    "reservation_id": str(reservation_id),
                    "observed": observed.model_dump(),
                    "unknown_dimensions": list(unknown),
                    "charged": charged.model_dump(),
                },
            )

    async def request_stop(self, id: UUID) -> ExperimentRecord[Policy]:
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

    async def mark_uncertain(
        self, reservation_id: UUID, observed: UsageAmounts | None = None
    ) -> None:
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT experiment,status,settled_json FROM experiment_usage "
                        "WHERE "
                        "reservation_id=:id"
                    ),
                    {"id": str(reservation_id)},
                )
            ).one()
            if row.status in ("settled", "conservative"):
                raise RevisionConflict("finalized usage cannot become uncertain")
            previous = UsageAmounts.model_validate_json(row.settled_json or "{}")
            observed = UsageAmounts(
                **{
                    name: max(
                        getattr(previous, name),
                        getattr(observed or UsageAmounts(), name),
                    )
                    for name in UsageAmounts.model_fields
                }
            )
            await conn.execute(
                text(
                    "UPDATE experiment_usage SET "
                    "status='uncertain',settled_json=:observed WHERE "
                    "reservation_id=:id"
                ),
                {"id": str(reservation_id), "observed": observed.model_dump_json()},
            )
            await self._event(
                conn,
                UUID(row.experiment),
                "usage_uncertain",
                {"reservation_id": str(reservation_id)},
            )

    async def begin_effect(
        self,
        id: UUID,
        trial: UUID | None,
        kind: str,
        intent: dict[str, object],
        labels: dict[str, str],
    ) -> UUID:
        if kind not in (
            "worktree",
            "network",
            "relay",
            "worker",
            "command",
            "grading",
            "role",
            "promotion",
            "publication",
        ):
            raise ValueError("unknown effect kind")
        encoded = json.dumps(intent, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 65536:
            raise ValueError("oversized effect intent")
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.stop_requested or record.status not in ("ready", "running"):
                raise RevisionConflict("experiment stopped before external effect")
            effect = uuid4()
            await conn.execute(
                text("""INSERT INTO experiment_effects
(id,experiment,trial,kind,intent_json,ownership_labels_json,status)
VALUES (:effect,:id,:trial,:kind,:intent,:labels,'intent')"""),
                dict(
                    effect=str(effect),
                    id=str(id),
                    trial=str(trial) if trial else None,
                    kind=kind,
                    intent=encoded,
                    labels=json.dumps(labels, sort_keys=True),
                ),
            )
            await self._event(
                conn, id, "effect_intent", {"effect_id": str(effect), "kind": kind}
            )
            return effect

    async def effect_state(
        self, effect_id: UUID, status: str, external_id: str | None = None
    ) -> None:
        if status not in ("active", "stopped", "uncertain"):
            raise ValueError("invalid effect state")
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT experiment,status,external_id FROM "
                        "experiment_effects WHERE id=:id"
                    ),
                    {"id": str(effect_id)},
                )
            ).one()
            if row.status == "stopped" and status == "active":
                raise RevisionConflict("effect already stopped")
            if external_id and row.external_id and row.external_id != external_id:
                raise RevisionConflict("external effect identity drift")
            await conn.execute(
                text("""UPDATE experiment_effects SET status=:status,
external_id=COALESCE(external_id,:external),
updated_at=CURRENT_TIMESTAMP WHERE id=:id"""),
                {"id": str(effect_id), "status": status, "external": external_id},
            )
            await self._event(
                conn,
                UUID(row.experiment),
                "effect_" + status,
                {"effect_id": str(effect_id)},
            )

    async def effects(self, id: UUID) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text("SELECT * FROM experiment_effects WHERE experiment=:id"),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    async def register_snapshot(
        self, id: UUID, snapshot: "HarnessSnapshot", artifact_ref: str
    ) -> None:
        from crucible.improvement.contracts import HarnessSnapshot

        snapshot = HarnessSnapshot.model_validate_json(snapshot.model_dump_json())
        async with self._transaction() as conn:
            run = await self._get(conn, id)
            if run.manifest.schema_version == 2:
                await self._search_revision(conn, id, run.revision)
            await conn.execute(
                text("""INSERT INTO experiment_snapshots
(id,experiment,parent_commit,kind,source_digest,
export_digest,patch_digest,artifact_ref,status)
VALUES
(:sid,:id,:parent,:kind,:source,:export,:patch,:artifact,'verified')"""),
                dict(
                    sid=str(snapshot.snapshot_id),
                    id=str(id),
                    parent=snapshot.parent_commit,
                    kind=snapshot.kind,
                    source=snapshot.source_digest,
                    export=snapshot.export_digest,
                    patch=snapshot.patch_digest,
                    artifact=artifact_ref,
                ),
            )
            await self._event(
                conn,
                id,
                "snapshot_verified",
                {"snapshot_id": str(snapshot.snapshot_id)},
            )

    async def create_trial(
        self,
        id: UUID,
        snapshot_id: UUID,
        case_id: str,
        repeat_index: int,
        key: str,
        *,
        final_authorization: str | None = None,
    ) -> UUID:
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.stop_requested or record.status not in ("ready", "running"):
                raise RevisionConflict("experiment not ready for trial")
            if not isinstance(record.manifest, ExperimentManifest):
                raise ValueError("measured trials require an experiment manifest")
            if final_authorization is not None:
                await self._authorize_final(
                    conn, id, snapshot_id, case_id, repeat_index, final_authorization
                )
            else:
                if record.manifest.schema_version == 2:
                    await self._search_revision(conn, id, record.revision)
                if repeat_index not in range(3) or case_id not in {
                    c.case_id for c in record.manifest.cases if c.split != "final-test"
                }:
                    raise ValueError("trial outside selection schedule")
            snapshot = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_snapshots WHERE id=:sid AND "
                        "experiment=:id AND status='verified'"
                    ),
                    {"sid": str(snapshot_id), "id": str(id)},
                )
            ).first()
            if not snapshot:
                raise ValueError("snapshot outside experiment")
            generation = (
                await conn.execute(
                    text("""SELECT COALESCE(MAX(generation),-1)+1 FROM experiment_trials
WHERE experiment=:id AND snapshot=:sid AND case_id=:case AND
repeat_index=:repeat"""),
                    dict(
                        id=str(id),
                        sid=str(snapshot_id),
                        case=case_id,
                        repeat=repeat_index,
                    ),
                )
            ).scalar_one()
            trial_id = uuid4()
            await conn.execute(
                text("""INSERT INTO experiment_trials
(id,experiment,snapshot,case_id,repeat_index,key,status,generation)
VALUES (:trial,:id,:sid,:case,:repeat,:key,'pending',:generation)"""),
                dict(
                    trial=str(trial_id),
                    id=str(id),
                    sid=str(snapshot_id),
                    case=case_id,
                    repeat=repeat_index,
                    key=key,
                    generation=generation,
                ),
            )
            if generation and record.manifest.schema_version == 2:
                previous = (
                    await conn.execute(
                        text(
                            "SELECT id,key,status FROM experiment_trials WHERE "
                            "experiment=:id AND snapshot=:sid AND case_id=:case AND "
                            "repeat_index=:repeat AND generation=:generation"
                        ),
                        {
                            "id": str(id),
                            "sid": str(snapshot_id),
                            "case": case_id,
                            "repeat": repeat_index,
                            "generation": generation - 1,
                        },
                    )
                ).one()
                if (
                    previous.status not in ("error", "interrupted")
                    or previous.key != key
                ):
                    raise RevisionConflict("previous trial is not retry eligible")
                await self._consume_trial_retry(conn, id, previous.id, str(trial_id))
                await self._event(
                    conn,
                    id,
                    "trial_superseded",
                    {
                        "original_trial_id": previous.id,
                        "replacement_trial_id": str(trial_id),
                    },
                )
            await self._event(
                conn,
                id,
                "trial_created",
                {"trial_id": str(trial_id), "generation": generation},
            )
            return trial_id

    async def snapshot(self, id: UUID, snapshot_id: UUID) -> dict[str, object]:
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_snapshots WHERE id=:sid "
                            "AND experiment=:id"
                        ),
                        {"sid": str(snapshot_id), "id": str(id)},
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)

    async def trials(self, id: UUID) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_trials WHERE "
                            "experiment=:id ORDER BY rowid"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    async def trial_state(
        self,
        trial: UUID,
        status: str,
        *,
        artifact: str | None = None,
        verdict: str | None = None,
        termination: str | None = None,
        error: str | None = None,
    ) -> None:
        if status not in (
            "starting",
            "solving",
            "snapshotting",
            "grading",
            "complete",
            "error",
            "interrupted",
        ):
            raise ValueError("invalid trial status")
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT experiment,status FROM experiment_trials "
                        "WHERE id=:trial"
                    ),
                    {"trial": str(trial)},
                )
            ).one()
            if row.status in ("complete", "error", "interrupted"):
                raise RevisionConflict("terminal trial cannot be overwritten")
            if status == "complete" and (
                not artifact or verdict not in ("pass", "fail")
            ):
                raise ValueError("complete trial requires authoritative result")
            await conn.execute(
                text("""UPDATE experiment_trials SET status=:status,
                artifact_ref=:artifact,grade_verdict=:verdict,solver_termination=:termination,
                error_code=:error WHERE id=:trial"""),
                dict(
                    trial=str(trial),
                    status=status,
                    artifact=artifact,
                    verdict=verdict,
                    termination=termination,
                    error=error,
                ),
            )
            await self._event(
                conn, UUID(row.experiment), "trial_" + status, {"trial_id": str(trial)}
            )

    async def record_event(
        self, id: UUID, event: str, payload: dict[str, object]
    ) -> None:
        async with self._transaction() as conn:
            await self._get(conn, id)
            await self._event(conn, id, event, payload)

    async def events(self, id: UUID, event: str) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT payload_json FROM experiment_events WHERE "
                        "experiment=:id AND type=:event ORDER BY sequence"
                    ),
                    {"id": str(id), "event": event},
                )
            ).all()
            return tuple(json.loads(row.payload_json) for row in rows)

    async def fail_run(self, id: UUID, code: str, *, interrupted: bool = False) -> None:
        async with self._transaction() as conn:
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET status=:status, "
                    "stop_requested=1, error_code=COALESCE(error_code,:code), "
                    "revision=revision+1 WHERE id=:id AND "
                    "status!='closed'"
                ),
                {
                    "id": str(id),
                    "status": "interrupted" if interrupted else "error",
                    "code": code,
                },
            )
            await self._event(
                conn,
                id,
                "execution_stopped",
                {"code": code, "interrupted": interrupted},
            )

    async def usage_summary(self, id: UUID) -> dict[str, object]:
        run = await self.get(id)
        confirmed = dict.fromkeys(UsageAmounts.model_fields, 0)
        conservative = dict.fromkeys(UsageAmounts.model_fields, 0)
        retained = dict.fromkeys(UsageAmounts.model_fields, 0)
        partials = {
            e["reservation_id"]: e for e in await self.events(id, "usage_partial")
        }
        reported_overages = {
            e["reservation_id"]
            for e in await self.events(id, "usage_settled")
            if e.get("uncertain") is True
        }
        acknowledgments = {
            e["reservation_id"]: e for e in await self.events(id, "usage_acknowledged")
        }
        for row in await self.usage_records(id):
            reserved = UsageAmounts.model_validate_json(str(row["requested_json"]))
            observed = UsageAmounts.model_validate_json(
                str(row["settled_json"] or "{}")
            )
            target = (
                confirmed
                if row["status"] == "settled"
                else conservative
                if row["status"] == "conservative"
                else retained
            )
            partial = partials.get(row["reservation_id"])
            for name in target:
                unknown = partial.get("unknown_dimensions") if partial else None
                if row["reservation_id"] in reported_overages:
                    # An overage is known usage, even while its allowance needs
                    # recovery. Acknowledgment never changes that provenance.
                    receipt = acknowledgments.get(row["reservation_id"])
                    reported = (
                        UsageAmounts.model_validate(receipt["observed"])
                        if receipt
                        else observed
                    )
                    confirmed[name] += getattr(reported, name)
                    target[name] += max(
                        0, getattr(reserved, name) - getattr(reported, name)
                    )
                elif partial and isinstance(unknown, list) and name not in unknown:
                    confirmed[name] += getattr(observed, name)
                else:
                    target[name] += (
                        getattr(observed, name)
                        if row["status"] == "settled"
                        else max(getattr(reserved, name), getattr(observed, name))
                    )
        caps = dict(
            tokens=run.manifest.budget.max_reported_and_reserved_tokens,
            requests=run.manifest.budget.max_model_requests,
            active_seconds=run.manifest.budget.max_active_seconds,
            output_bytes=run.manifest.budget.max_total_output_bytes,
            trial_starts=run.manifest.budget.max_trial_starts,
            worker_wall_seconds=run.manifest.budget.max_worker_wall_seconds,
        )
        return {
            "confirmed": confirmed,
            "conservative": conservative,
            "retained": retained,
            "remaining": {
                name: max(
                    0,
                    caps[name] - confirmed[name] - conservative[name] - retained[name],
                )
                for name in caps
            },
            "dollars": None,
            "price_basis": None,
        }

    async def unsettled_usage(self, id: UUID) -> bool:
        async with self.engine.connect() as conn:
            return bool(
                (
                    await conn.execute(
                        text(
                            "SELECT reservation_id FROM experiment_usage "
                            "WHERE experiment=:id AND status NOT IN "
                            "('settled','conservative') LIMIT"
                            " 1"
                        ),
                        {"id": str(id)},
                    )
                ).first()
            )

    async def claim_execution(
        self,
        id: UUID,
        stage: Stage,
        *,
        process_id: int | None = None,
        process_start: str | None = None,
    ) -> UUID:
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT owner_execution_id,status,stop_requested FROM"
                        " experiment_runs WHERE id=:id"
                    ),
                    {"id": str(id)},
                )
            ).one()
            if row.owner_execution_id or row.status != "ready" or row.stop_requested:
                raise RevisionConflict(
                    "experiment already running or requires explicit recovery"
                )
            owner = uuid4()
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    " "
                    "owner_execution_id=:owner,owner_heartbeat_at=CURRENT_TIMESTA"
                    "MP,status='running',stage=:stage,revision=revision+1"
                    " WHERE id=:id"
                ),
                {"id": str(id), "owner": str(owner), "stage": stage},
            )
            await self._event(
                conn,
                id,
                "execution_claimed",
                {
                    "owner": str(owner),
                    "pid": process_id,
                    "process_start": process_start,
                },
            )
            return owner

    async def release_execution(self, id: UUID, owner: UUID, stage: Stage) -> None:
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT owner_execution_id,status FROM "
                        "experiment_runs WHERE id=:id"
                    ),
                    {"id": str(id)},
                )
            ).one()
            if row.owner_execution_id != str(owner):
                raise RevisionConflict("execution ownership changed")
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    "owner_execution_id=NULL,owner_heartbeat_at=NULL,status=CASE"
                    " WHEN status='running' THEN 'ready' ELSE status "
                    "END,stage=:stage,revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id), "stage": stage},
            )
            await self._event(conn, id, "execution_released", {"owner": str(owner)})

    async def heartbeat(self, id: UUID, owner: UUID) -> None:
        async with self._transaction() as conn:
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    "owner_heartbeat_at=CURRENT_TIMESTAMP WHERE id=:id AND "
                    "owner_execution_id=:owner"
                ),
                {"id": str(id), "owner": str(owner)},
            )

    async def acknowledge_usage(
        self, id: UUID, owner: UUID, reservation_ids: tuple[UUID, ...]
    ) -> None:
        """Retain conservative charges after effects are positively reconciled."""
        async with self._transaction() as conn:
            await self._require_owner(conn, id, owner)
            if (await self._get(conn, id)).status == "closed":
                raise RevisionConflict("closed experiment")
            effects = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_effects WHERE experiment=:id "
                        "AND status!='stopped' LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).first()
            if effects:
                raise RevisionConflict(
                    "owned effects must be stopped before acknowledgment"
                )
            for rid in dict.fromkeys(reservation_ids):
                row = (
                    (
                        await conn.execute(
                            text(
                                "SELECT * FROM experiment_usage WHERE experiment=:id "
                                "AND reservation_id=:rid"
                            ),
                            {"id": str(id), "rid": str(rid)},
                        )
                    )
                    .mappings()
                    .one()
                )
                if row["status"] == "conservative":
                    continue
                if row["status"] not in ("reserved", "uncertain"):
                    raise RevisionConflict(
                        "only unresolved reservations can be acknowledged"
                    )
                reserved = UsageAmounts.model_validate_json(row["requested_json"])
                observed = UsageAmounts.model_validate_json(row["settled_json"] or "{}")
                partial = (
                    await conn.execute(
                        text(
                            "SELECT payload_json FROM experiment_events "
                            "WHERE experiment=:id "
                            "AND type='usage_partial' AND "
                            "json_extract(payload_json,'$.reservation_id')=:rid"
                        ),
                        {"id": str(id), "rid": str(rid)},
                    )
                ).scalar_one_or_none()
                unknown = (
                    json.loads(partial)["unknown_dimensions"]
                    if partial
                    else list(UsageAmounts.model_fields)
                )
                charged = UsageAmounts(
                    **{
                        name: max(getattr(reserved, name), getattr(observed, name))
                        if name in unknown
                        else getattr(observed, name)
                        for name in UsageAmounts.model_fields
                    }
                )
                await conn.execute(
                    text(
                        "UPDATE experiment_usage SET "
                        "status='conservative',settled_json=:charge "
                        "WHERE reservation_id=:rid"
                    ),
                    {"rid": str(rid), "charge": charged.model_dump_json()},
                )
                await self._event(
                    conn,
                    id,
                    "usage_acknowledged",
                    {
                        "reservation_id": str(rid),
                        "basis": "conservative",
                        "reserved": reserved.model_dump(),
                        "observed": observed.model_dump(),
                        "charged": charged.model_dump(),
                    },
                )

    async def authorize_trial_retries(
        self, id: UUID, trial_ids: tuple[UUID, ...]
    ) -> None:
        """Authorize named failed attempts; this operation executes no work."""
        async with self._transaction() as conn:
            run = await self._get(conn, id)
            owner = (
                await conn.execute(
                    text("SELECT owner_execution_id FROM experiment_runs WHERE id=:id"),
                    {"id": str(id)},
                )
            ).scalar_one()
            if run.status != "ready" or run.stop_requested or owner is not None:
                raise RevisionConflict("recover before authorizing retries")
            blocked = (
                await conn.execute(
                    text(
                        "SELECT 1 FROM experiment_usage WHERE experiment=:id AND "
                        "status NOT IN ('settled','conservative') UNION ALL "
                        "SELECT 1 FROM experiment_effects WHERE experiment=:id "
                        "AND status!='stopped'"
                    ),
                    {"id": str(id)},
                )
            ).first()
            if blocked:
                raise RevisionConflict("effects or usage require recovery")
            for trial in dict.fromkeys(trial_ids):
                row = (
                    (
                        await conn.execute(
                            text(
                                "SELECT * FROM experiment_trials WHERE id=:trial AND "
                                "experiment=:id"
                            ),
                            {"id": str(id), "trial": str(trial)},
                        )
                    )
                    .mappings()
                    .one()
                )
                newer = (
                    await conn.execute(
                        text(
                            "SELECT id FROM experiment_trials WHERE experiment=:id AND "
                            "snapshot=:sid "
                            "AND case_id=:case AND repeat_index=:repeat AND "
                            "generation>:generation"
                        ),
                        {
                            "id": str(id),
                            "sid": row["snapshot"],
                            "case": row["case_id"],
                            "repeat": row["repeat_index"],
                            "generation": row["generation"],
                        },
                    )
                ).first()
                if newer or row["status"] not in ("error", "interrupted"):
                    raise RevisionConflict(
                        "only latest failed trial attempts can be retried"
                    )
                exists = (
                    await conn.execute(
                        text(
                            "SELECT 1 FROM experiment_events WHERE experiment=:id "
                            "AND type='trial_retry_authorized' "
                            "AND json_extract(payload_json,'$.trial_id')=:trial"
                        ),
                        {"id": str(id), "trial": str(trial)},
                    )
                ).first()
                if not exists:
                    await self._event(
                        conn, id, "trial_retry_authorized", {"trial_id": str(trial)}
                    )

    async def _consume_trial_retry(
        self, conn: AsyncConnection, id: UUID, trial: str, replacement: str
    ) -> None:
        events = (
            (
                await conn.execute(
                    text(
                        "SELECT type FROM experiment_events WHERE experiment=:id AND "
                        "type IN ('trial_retry_authorized','trial_retry_consumed') AND "
                        "json_extract(payload_json,'$.trial_id')=:trial"
                    ),
                    {"id": str(id), "trial": trial},
                )
            )
            .scalars()
            .all()
        )
        if "trial_retry_authorized" not in events or "trial_retry_consumed" in events:
            raise RevisionConflict("trial retry requires explicit authorization")
        await self._event(
            conn,
            id,
            "trial_retry_consumed",
            {
                "trial_id": trial,
                "replacement_trial_id": replacement,
            },
        )

    async def recover_ready(self, id: UUID, owner: UUID) -> None:
        async with self._transaction() as conn:
            await self._require_owner(conn, id, owner)
            unresolved = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_effects WHERE "
                        "experiment=:id AND status!='stopped' LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).first()
            usage = (
                await conn.execute(
                    text(
                        "SELECT reservation_id FROM experiment_usage WHERE "
                        "experiment=:id AND status NOT IN ('settled','conservative') "
                        "LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).first()
            if unresolved or usage:
                raise RevisionConflict("effects or usage require recovery")
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    " "
                    "owner_execution_id=NULL,owner_heartbeat_at=NULL,status='read"
                    "y',stop_requested=0,error_code=NULL,revision=revision+1"
                    " WHERE id=:id AND status!='closed'"
                ),
                {"id": str(id)},
            )
            await self._event(conn, id, "recovery_completed", {})

    async def _require_owner(
        self, conn: AsyncConnection, id: UUID, owner: UUID
    ) -> None:
        actual = (
            await conn.execute(
                text("SELECT owner_execution_id FROM experiment_runs WHERE id=:id"),
                {"id": str(id)},
            )
        ).scalar_one()
        if actual != str(owner):
            raise RevisionConflict("recovery ownership changed")

    async def claim_recovery(
        self,
        id: UUID,
        revision: int,
        previous_owner: str | None,
        process_id: int,
        process_start: str | None,
    ) -> UUID:
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            actual = (
                await conn.execute(
                    text("SELECT owner_execution_id FROM experiment_runs WHERE id=:id"),
                    {"id": str(id)},
                )
            ).scalar_one()
            if (
                record.revision != revision
                or actual != previous_owner
                or record.status != "stopping"
            ):
                raise RevisionConflict("recovery state changed")
            owner = uuid4()
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    "owner_execution_id=:owner,revision=revision+1 "
                    "WHERE id=:id"
                ),
                {"id": str(id), "owner": str(owner)},
            )
            await self._event(
                conn,
                id,
                "execution_claimed",
                {
                    "owner": str(owner),
                    "pid": process_id,
                    "process_start": process_start,
                },
            )
            return owner

    async def finish_close(self, id: UUID, owner: UUID) -> None:
        async with self._transaction() as conn:
            await self._require_owner(conn, id, owner)
            unresolved = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_effects WHERE experiment=:id "
                        "AND status!='stopped' LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).first()
            if unresolved:
                raise RevisionConflict("effects require recovery")
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET status='closed',"
                    "owner_execution_id=NULL,"
                    "owner_heartbeat_at=NULL,revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id)},
            )
            await conn.execute(
                text("DELETE FROM experiment_repository_lock WHERE experiment_id=:id"),
                {"id": str(id)},
            )
            await self._event(conn, id, "explicit_close", {})

    async def snapshots(self, id: UUID) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            return tuple(
                dict(r)
                for r in (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_snapshots WHERE "
                            "experiment=:id ORDER BY rowid"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )

    async def invalidate_interrupted(self, id: UUID) -> None:
        async with self._transaction() as conn:
            await conn.execute(
                text(
                    "UPDATE experiment_trials SET "
                    " "
                    "status='interrupted',error_code=COALESCE(error_code,'process"
                    "_interrupted')"
                    " WHERE experiment=:id AND (status NOT IN "
                    "('complete','error','interrupted') OR snapshot IN "
                    "(SELECT id FROM experiment_snapshots WHERE "
                    "experiment=:id AND kind='candidate' AND id NOT IN "
                    "(SELECT candidate_snapshot FROM experiment_promotions "
                    "WHERE experiment=:id)))"
                ),
                {"id": str(id)},
            )
            await conn.execute(
                text(
                    "UPDATE experiment_snapshots SET status='interrupted' "
                    "WHERE experiment=:id AND kind='candidate' AND id NOT IN "
                    "(SELECT candidate_snapshot FROM experiment_promotions "
                    "WHERE experiment=:id)"
                ),
                {"id": str(id)},
            )
            await self._event(conn, id, "interrupted_candidate_invalidated", {})

    async def reactivate_snapshot(
        self, id: UUID, snapshot_id: UUID, artifact: str
    ) -> None:
        async with self._transaction() as conn:
            await conn.execute(
                text(
                    "UPDATE experiment_snapshots SET "
                    "status='verified',artifact_ref=:artifact WHERE id=:sid "
                    "AND experiment=:id AND status='interrupted' AND "
                    "kind='candidate'"
                ),
                {"sid": str(snapshot_id), "id": str(id), "artifact": artifact},
            )
            await self._event(
                conn,
                id,
                "explicit_candidate_reintroduced",
                {"snapshot_id": str(snapshot_id)},
            )

    async def execution_owner(self, id: UUID) -> str | None:
        async with self.engine.connect() as conn:
            value = (
                await conn.execute(
                    text("SELECT owner_execution_id FROM experiment_runs WHERE id=:id"),
                    {"id": str(id)},
                )
            ).scalar_one()
            return str(value) if value is not None else None

    async def _search_revision(
        self, conn: AsyncConnection, id: UUID, revision: int
    ) -> ExperimentRecord:
        record = await self._get(conn, id)
        if not isinstance(record.manifest, ExperimentManifest):
            raise ValueError("search requires an experiment manifest")
        sealed = (
            await conn.execute(
                text(
                    "SELECT experiment FROM experiment_search_seals WHERE "
                    "experiment=:id"
                ),
                {"id": str(id)},
            )
        ).first()
        if record.manifest.schema_version != 2:
            raise RevisionConflict("historical experiment requires a new v2 identity")
        if (
            record.revision != revision
            or record.stop_requested
            or record.status not in ("ready", "running")
            or sealed
        ):
            raise RevisionConflict("stale, stopped or sealed search")
        return cast(ExperimentRecord, record)

    async def _search_event(
        self, conn: AsyncConnection, id: UUID, event: str, payload: dict[str, object]
    ) -> None:
        await conn.execute(
            text(
                "UPDATE experiment_runs SET "
                "revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=:id"
            ),
            {"id": str(id)},
        )
        await self._event(conn, id, event, payload)

    async def rounds(self, id: UUID) -> tuple[RoundRecord, ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_rounds WHERE experiment=:id "
                            "ORDER BY ordinal"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(
                RoundRecord(
                    UUID(r["id"]),
                    UUID(r["experiment"]),
                    r["ordinal"],
                    UUID(r["baseline_snapshot"]),
                    r["baseline_commit"],
                    r["status"],
                    r["mining_attempts"],
                    UUID(r["candidate_snapshot"]) if r["candidate_snapshot"] else None,
                    r["decision_ref"],
                    r["decision_digest"],
                )
                for r in rows
            )

    async def begin_round(
        self, experiment_id: UUID, baseline_snapshot: UUID, expected_revision: int
    ) -> RoundRecord:
        async with self._transaction() as conn:
            run = await self._search_revision(conn, experiment_id, expected_revision)
            snapshot = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_snapshots WHERE id=:sid AND "
                            "experiment=:id AND status='verified'"
                        ),
                        {"sid": str(baseline_snapshot), "id": str(experiment_id)},
                    )
                )
                .mappings()
                .one()
            )
            if UUID(str(snapshot["id"])) != await self._accepted_snapshot_id(conn, run):
                raise RevisionConflict("round baseline is not accepted")
            previous = (
                await conn.execute(
                    text(
                        "SELECT ordinal,status FROM experiment_rounds WHERE "
                        "experiment=:id ORDER BY ordinal DESC LIMIT 1"
                    ),
                    {"id": str(experiment_id)},
                )
            ).first()
            if previous and previous.status not in ("decided", "interrupted"):
                raise RevisionConflict("previous round unfinished")
            ordinal = previous.ordinal + 1 if previous else 1
            if ordinal > 3:
                raise RevisionConflict("round limit exhausted")
            rid = uuid4()
            await conn.execute(
                text(
                    "INSERT INTO "
                    "experiment_rounds(id,experiment,ordinal,baseline_snapshot,"
                    "baseline_commit,status) VALUES "
                    "(:rid,:id,:ordinal,:sid,:commit,'mining')"
                ),
                {
                    "rid": str(rid),
                    "id": str(experiment_id),
                    "ordinal": ordinal,
                    "sid": str(baseline_snapshot),
                    "commit": run.last_accepted_commit,
                },
            )
            await self._search_event(
                conn,
                experiment_id,
                "round_started",
                {"round_id": str(rid), "ordinal": ordinal},
            )
            return RoundRecord(
                rid,
                experiment_id,
                ordinal,
                baseline_snapshot,
                run.last_accepted_commit,
                "mining",
                0,
                None,
                None,
                None,
            )

    async def save_role(
        self, execution: RoleExecution, attempt: int, expected_revision: int
    ) -> RoleRecord:
        execution = RoleExecution.model_validate_json(execution.model_dump_json())
        if (
            attempt not in (0, 1)
            or (execution.kind != "format_correction" and attempt != 0)
            or (execution.kind == "format_correction" and attempt != 1)
        ):
            raise ValueError("invalid role attempt")
        async with self._transaction() as conn:
            if execution.status in ("error", "interrupted"):
                run = await self._get(conn, execution.experiment_id)
                if (
                    run.revision != expected_revision
                    or run.manifest.schema_version != 2
                ):
                    raise RevisionConflict("stale role interruption")
            else:
                await self._search_revision(
                    conn, execution.experiment_id, expected_revision
                )
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_rounds WHERE id=:rid AND "
                            "experiment=:id"
                        ),
                        {
                            "rid": str(execution.round_id),
                            "id": str(execution.experiment_id),
                        },
                    )
                )
                .mappings()
                .one()
            )
            snapshot = (
                await conn.execute(
                    text(
                        "SELECT source_digest FROM experiment_snapshots WHERE id=:sid"
                    ),
                    {"sid": row["baseline_snapshot"]},
                )
            ).scalar_one()
            if (
                execution.accepted_snapshot_id != UUID(row["baseline_snapshot"])
                or execution.source_digest != snapshot
                or row["status"] in ("decided", "interrupted", "error")
            ):
                raise RevisionConflict("role baseline or round changed")
            existing = (
                await conn.execute(
                    text("SELECT execution_json FROM experiment_roles WHERE id=:id"),
                    {"id": str(execution.role_id)},
                )
            ).scalar_one_or_none()
            if existing:
                old = RoleExecution.model_validate_json(existing)
                if old.status not in ("pending", "running") or old.model_dump(
                    exclude={
                        "status",
                        "output_digest",
                        "output_ref",
                        "termination",
                        "usage_ref",
                        "error_code",
                    }
                ) != execution.model_dump(
                    exclude={
                        "status",
                        "output_digest",
                        "output_ref",
                        "termination",
                        "usage_ref",
                        "error_code",
                    }
                ):
                    raise RevisionConflict(
                        "role completion immutable or identity changed"
                    )
                await conn.execute(
                    text(
                        "UPDATE experiment_roles SET execution_json=:data WHERE id=:id"
                    ),
                    {"id": str(execution.role_id), "data": execution.model_dump_json()},
                )
            else:
                await conn.execute(
                    text(
                        "INSERT INTO "
                        "experiment_roles(id,round,kind,attempt,execution_json) "
                        "VALUES (:id,:rid,:kind,:attempt,:data)"
                    ),
                    {
                        "id": str(execution.role_id),
                        "rid": str(execution.round_id),
                        "kind": execution.kind,
                        "attempt": attempt,
                        "data": execution.model_dump_json(),
                    },
                )
                if execution.kind in ("mining", "format_correction"):
                    await conn.execute(
                        text(
                            "UPDATE experiment_rounds SET "
                            "mining_attempts=mining_attempts+1 WHERE id=:id"
                        ),
                        {"id": str(execution.round_id)},
                    )
            await self._search_event(
                conn,
                execution.experiment_id,
                "role_saved",
                {"role_id": str(execution.role_id), "status": execution.status},
            )
        return RoleRecord(execution, attempt)

    async def recover_undispatched_role(
        self, execution: "RoleExecution"
    ) -> "RoleExecution":
        from crucible.improvement.roles import RoleExecution

        async with self._transaction() as conn:
            run = await self._get(conn, execution.experiment_id)
            await self._search_revision(conn, run.id, run.revision)
            saved = (
                await conn.execute(
                    text("SELECT execution_json FROM experiment_roles WHERE id=:id"),
                    {"id": str(execution.role_id)},
                )
            ).scalar_one()
            old = RoleExecution.model_validate_json(saved)
            round = (
                await conn.execute(
                    text(
                        "SELECT status,baseline_snapshot FROM experiment_rounds "
                        "WHERE id=:id"
                    ),
                    {"id": str(old.round_id)},
                )
            ).one()
            proposal = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_roles "
                        "WHERE round=:id AND kind='proposal'"
                    ),
                    {"id": str(old.round_id)},
                )
            ).first()
            if (
                round.status in ("decided", "interrupted", "error")
                or proposal
                or round.baseline_snapshot != str(old.accepted_snapshot_id)
                or await self._accepted_snapshot_id(conn, run)
                != old.accepted_snapshot_id
            ):
                raise RevisionConflict("mining round no longer recoverable")
            events = (
                await conn.execute(
                    text(
                        "SELECT type,payload_json FROM experiment_events "
                        "WHERE experiment=:id "
                        "ORDER BY sequence"
                    ),
                    {"id": str(run.id)},
                )
            ).all()
            resumed = False
            for event in events:
                payload = json.loads(event.payload_json)
                if event.type in ("explicit_resume", "recovery_completed"):
                    resumed = True
                if event.type == "role_saved" and payload.get("role_id") == str(
                    old.role_id
                ):
                    resumed = False
                if event.type == "usage_reserved" and payload.get("job_id") == str(
                    old.job_id
                ):
                    raise RevisionConflict("dispatched role cannot be re-executed")
            if (
                old != execution
                or old.kind not in ("mining", "format_correction")
                or old.status != "error"
                or old.termination is not None
                or old.output_ref is not None
                or not resumed
            ):
                raise RevisionConflict("mining requires explicit undispatched recovery")
            recovered = old.model_copy(update={"status": "running", "error_code": None})
            await conn.execute(
                text("UPDATE experiment_roles SET execution_json=:data WHERE id=:id"),
                {"id": str(old.role_id), "data": recovered.model_dump_json()},
            )
            await self._search_event(
                conn,
                run.id,
                "role_recovered",
                {
                    "original_execution": old.model_dump(mode="json"),
                    "role_id": str(old.role_id),
                },
            )
            await self._search_event(
                conn,
                run.id,
                "role_saved",
                {"role_id": str(old.role_id), "status": "running"},
            )
            return recovered

    async def roles(self, round_id: UUID) -> tuple[RoleRecord, ...]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT execution_json,attempt FROM experiment_roles WHERE "
                        "round=:id ORDER BY rowid"
                    ),
                    {"id": str(round_id)},
                )
            ).all()
            return tuple(
                RoleRecord(
                    RoleExecution.model_validate_json(r.execution_json), r.attempt
                )
                for r in rows
            )

    async def record_decision(
        self, round_id: UUID, decision: SelectionDecision, expected_revision: int
    ) -> None:
        decision = SelectionDecision.model_validate_json(decision.model_dump_json())
        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT experiment,status,baseline_snapshot FROM "
                        "experiment_rounds WHERE id=:id"
                    ),
                    {"id": str(round_id)},
                )
            ).one()
            id = UUID(row.experiment)
            await self._search_revision(conn, id, expected_revision)
            if row.status in ("decided", "interrupted"):
                raise RevisionConflict("round decision immutable")
            snapshot = (
                await conn.execute(
                    text(
                        "SELECT source_digest,export_digest FROM experiment_snapshots "
                        "WHERE id=:id"
                    ),
                    {"id": row.baseline_snapshot},
                )
            ).one()
            if (decision.baseline_source_digest, decision.baseline_export_digest) != (
                snapshot.source_digest,
                snapshot.export_digest,
            ):
                raise RevisionConflict("decision baseline mismatch")
            candidate_id = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_snapshots WHERE "
                        "experiment=:experiment AND source_digest=:source "
                        "AND export_digest=:export AND status='verified'"
                    ),
                    {
                        "experiment": str(id),
                        "source": decision.candidate_source_digest,
                        "export": decision.candidate_export_digest,
                    },
                )
            ).scalar_one_or_none()
            if decision.verdict == "promote" and candidate_id is None:
                raise RevisionConflict("promotion candidate snapshot unavailable")
            encoded = decision.model_dump_json()
            await conn.execute(
                text(
                    "UPDATE experiment_rounds SET "
                    "status='decided',decision_ref=:data,decision_digest=:digest,"
                    "candidate_snapshot=:candidate WHERE id=:id"
                ),
                {
                    "data": encoded,
                    "digest": digest(decision),
                    "id": str(round_id),
                    "candidate": candidate_id,
                },
            )
            await self._search_event(
                conn,
                id,
                "decision_recorded",
                {"round_id": str(round_id), "digest": digest(decision)},
            )

    async def decisions(self, experiment_id: UUID) -> tuple[SelectionDecision, ...]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT decision_ref,decision_digest FROM experiment_rounds "
                        "WHERE experiment=:id AND decision_ref IS NOT NULL ORDER BY "
                        "ordinal"
                    ),
                    {"id": str(experiment_id)},
                )
            ).all()
            results = []
            for row in rows:
                decision = SelectionDecision.model_validate_json(row.decision_ref)
                if digest(decision) != row.decision_digest:
                    raise ValueError("decision digest drift")
                results.append(decision)
            return tuple(results)

    async def round(self, round_id: UUID) -> RoundRecord:
        async with self.engine.connect() as conn:
            experiment = (
                await conn.execute(
                    text("SELECT experiment FROM experiment_rounds WHERE id=:id"),
                    {"id": str(round_id)},
                )
            ).scalar_one()
        return next(r for r in await self.rounds(UUID(experiment)) if r.id == round_id)

    async def require_search_open(self, id: UUID) -> ExperimentRecord:
        async with self._transaction() as conn:
            run = await self._get(conn, id)
            return await self._search_revision(conn, id, run.revision)

    @staticmethod
    def _promotion_record(row: RowMapping) -> PromotionRecord:
        return PromotionRecord(
            UUID(row["id"]),
            UUID(row["experiment"]),
            UUID(row["round"]),
            row["expected_parent"],
            UUID(row["candidate_snapshot"]),
            row["expected_tree"],
            row["patch_digest"],
            row["decision_digest"],
            row["state"],
            row["commit_sha"],
        )

    async def promotions(self, id: UUID) -> tuple[PromotionRecord, ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_promotions WHERE "
                            "experiment=:id ORDER BY rowid"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(self._promotion_record(r) for r in rows)

    async def promotion(self, id: UUID) -> PromotionRecord:
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM experiment_promotions WHERE id=:id"),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one()
            )
            return self._promotion_record(row)

    async def prepare_promotion(
        self,
        round_id: UUID,
        candidate_id: UUID,
        tree: str,
        patch_digest: str,
        decision_digest: str,
        timestamp: str,
    ) -> PromotionRecord:
        round = await self.round(round_id)
        async with self._transaction() as conn:
            run = await self._get(conn, round.experiment_id)
            await self._search_revision(conn, run.id, run.revision)
            row = (
                await conn.execute(
                    text(
                        "SELECT decision_digest, status FROM experiment_rounds "
                        "WHERE id=:id"
                    ),
                    {"id": str(round_id)},
                )
            ).one()
            if (
                row.status != "decided"
                or row.decision_digest != decision_digest
                or run.last_accepted_commit != round.baseline_commit
            ):
                raise RevisionConflict("promotion decision or parent drift")
            existing = (
                (
                    await conn.execute(
                        text("SELECT * FROM experiment_promotions WHERE round=:id"),
                        {"id": str(round_id)},
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                record = self._promotion_record(existing)
                if (
                    record.candidate_snapshot_id,
                    record.expected_tree,
                    record.patch_digest,
                    record.decision_digest,
                ) != (candidate_id, tree, patch_digest, decision_digest):
                    raise RevisionConflict("promotion intent drift")
                return record
            id = uuid4()
            await conn.execute(
                text(
                    "INSERT INTO experiment_promotions(id, experiment, round, "
                    "expected_parent, candidate_snapshot, expected_tree, "
                    "patch_digest, decision_digest, state) VALUES (:id, "
                    ":experiment, :round, :parent, :candidate, :tree, :patch, "
                    ":decision, 'intent')"
                ),
                {
                    "id": str(id),
                    "experiment": str(run.id),
                    "round": str(round_id),
                    "parent": round.baseline_commit,
                    "candidate": str(candidate_id),
                    "tree": tree,
                    "patch": patch_digest,
                    "decision": decision_digest,
                },
            )
            await self._search_event(
                conn,
                run.id,
                "promotion_intent",
                {"promotion_id": str(id), "timestamp": timestamp},
            )
        return await self.promotion(id)

    async def complete_promotion(self, id: UUID, commit: str) -> PromotionRecord:
        import re

        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise ValueError("invalid accepted commit")
        async with self._transaction() as conn:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM experiment_promotions WHERE id=:id"),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one()
            )
            record = self._promotion_record(row)
            if record.state == "complete":
                if record.commit_sha != commit:
                    raise RevisionConflict("accepted commit changed")
                return record
            run = await self._get(conn, record.experiment_id)
            if run.last_accepted_commit != record.expected_parent:
                raise RevisionConflict("accepted parent changed")
            await conn.execute(
                text(
                    "UPDATE experiment_promotions SET state='complete', "
                    "commit_sha=:commit WHERE id=:id"
                ),
                {"id": str(id), "commit": commit},
            )
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET last_accepted_commit=:commit, "
                    "revision=revision+1 WHERE id=:id"
                ),
                {"id": str(run.id), "commit": commit},
            )
            await self._event(
                conn,
                run.id,
                "promotion_complete",
                {
                    "promotion_id": str(id),
                    "commit": commit,
                    "accepted_snapshot_id": str(record.candidate_snapshot_id),
                },
            )
        return await self.promotion(id)

    async def _accepted_snapshot_id(
        self, conn: AsyncConnection, run: ExperimentRecord[Policy] | ExperimentRecord
    ) -> UUID:
        promoted = (
            await conn.execute(
                text(
                    "SELECT candidate_snapshot FROM experiment_promotions "
                    "WHERE experiment=:id AND state='complete' AND commit_sha=:commit"
                ),
                {"id": str(run.id), "commit": run.last_accepted_commit},
            )
        ).scalar_one_or_none()
        if promoted:
            return UUID(promoted)
        if run.last_accepted_commit != run.initial_commit:
            raise RevisionConflict("accepted lineage incomplete")
        rows = (
            await conn.execute(
                text(
                    "SELECT id FROM experiment_snapshots WHERE experiment=:id "
                    "AND kind='baseline' AND source_digest=:source AND "
                    "parent_commit=:parent AND status='verified'"
                ),
                {
                    "id": str(run.id),
                    "source": run.manifest.initial_source_digest,
                    "parent": run.initial_commit,
                },
            )
        ).all()
        if len(rows) != 1:
            raise RevisionConflict("accepted initial snapshot identity incomplete")
        return UUID(rows[0].id)

    async def accepted_snapshot_id(self, id: UUID) -> UUID:
        async with self.engine.connect() as conn:
            run = await self._get(conn, id)
            return await self._accepted_snapshot_id(conn, run)

    async def interrupt_round(self, round_id: UUID) -> None:
        """Consume a proposal opportunity after explicit reconciliation."""
        round = await self.round(round_id)
        async with self._transaction() as conn:
            run = await self._get(conn, round.experiment_id)
            await self._search_revision(conn, run.id, run.revision)
            if round.status in ("decided", "interrupted"):
                return
            await conn.execute(
                text("UPDATE experiment_rounds SET status='interrupted' WHERE id=:id"),
                {"id": str(round_id)},
            )
            await self._search_event(
                conn, run.id, "round_interrupted", {"round_id": str(round_id)}
            )

    async def selected_trials(self, id: UUID) -> tuple[dict[str, object], ...]:
        """Supersede only failed attempts explicitly authorized by recovery."""
        rows = await self.trials(id)
        by_id = {str(r["id"]): r for r in rows}
        removed: set[str] = set()
        for event in await self.events(id, "trial_superseded"):
            old_id, new_id = (
                str(event["original_trial_id"]),
                str(event["replacement_trial_id"]),
            )
            old, new = by_id[old_id], by_id[new_id]
            if (
                old_id in removed
                or old["status"] not in ("error", "interrupted")
                or any(
                    old[k] != new[k]
                    for k in ("snapshot", "case_id", "repeat_index", "key")
                )
                or int(str(new["generation"])) != int(str(old["generation"])) + 1
            ):
                raise RevisionConflict("invalid trial supersession")
            removed.add(old_id)
        return tuple(r for r in rows if str(r["id"]) not in removed)

    async def require_trial_retry(
        self, id: UUID, snapshot: UUID, case: str, repeat: int, key: str
    ) -> None:
        rows = [
            r
            for r in await self.selected_trials(id)
            if r["snapshot"] == str(snapshot)
            and r["case_id"] == case
            and r["repeat_index"] == repeat
        ]
        if not rows:
            return
        authorizations = await self.events(id, "trial_retry_authorized")
        consumed = await self.events(id, "trial_retry_consumed")
        authorized = {e["trial_id"] for e in authorizations} - {
            e["trial_id"] for e in consumed
        }
        if (
            len(rows) != 1
            or rows[0]["status"] not in ("error", "interrupted")
            or rows[0]["key"] != key
            or rows[0]["id"] not in authorized
        ):
            raise RevisionConflict(
                "trial retry requires explicit compatible authorization"
            )

    @staticmethod
    def _seal_record(row: RowMapping) -> SearchSeal:
        return SearchSeal(
            UUID(row["experiment"]),
            UUID(row["initial_snapshot"]),
            UUID(row["selected_final_snapshot"]),
            row["selected_final_commit"],
            row["accepted_lineage_digest"],
            row["search_history_digest"],
            row["stop_reason"],
            row["created_at"],
        )

    async def search_seal(self, id: UUID) -> SearchSeal | None:
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_search_seals WHERE experiment=:id"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one_or_none()
            )
            return self._seal_record(row) if row else None

    async def seal_search(
        self, id: UUID, expected_accepted_commit: str, stop_reason: str
    ) -> SearchSeal:
        from dataclasses import asdict

        async with self._transaction() as conn:
            run = await self._get(conn, id)
            existing = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_search_seals WHERE experiment=:id"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                seal = self._seal_record(existing)
                if (seal.selected_final_commit, seal.stop_reason) != (
                    expected_accepted_commit,
                    stop_reason,
                ):
                    raise RevisionConflict("immutable seal differs")
                return seal
            await self._search_revision(conn, id, run.revision)
            outcome = (
                await conn.execute(
                    text(
                        "SELECT payload_json FROM experiment_events WHERE "
                        "experiment=:id AND type='search_outcome' ORDER BY sequence "
                        "DESC LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).scalar_one_or_none()
            if (
                not outcome
                or not json.loads(outcome).get("ready_for_final")
                or json.loads(outcome).get("reason") != stop_reason
                or json.loads(outcome).get("accepted_commit")
                != run.last_accepted_commit
                or expected_accepted_commit != run.last_accepted_commit
            ):
                raise RevisionConflict("completed compatible search outcome required")
            pending = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_promotions WHERE experiment=:id "
                        "AND state!='complete'"
                    ),
                    {"id": str(id)},
                )
            ).first()
            effects = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_effects WHERE experiment=:id AND "
                        "status!='stopped'"
                    ),
                    {"id": str(id)},
                )
            ).first()
            usage = (
                await conn.execute(
                    text(
                        "SELECT reservation_id FROM experiment_usage WHERE "
                        "experiment=:id AND status!='settled'"
                    ),
                    {"id": str(id)},
                )
            ).first()
            if pending or effects or usage:
                raise RevisionConflict("unreconciled search cannot be sealed")
            selected = await self._accepted_snapshot_id(conn, run)
            initial = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_snapshots WHERE experiment=:id AND "
                        "kind='baseline' AND source_digest=:source AND "
                        "parent_commit=:commit AND status='verified'"
                    ),
                    {
                        "id": str(id),
                        "source": run.manifest.initial_source_digest,
                        "commit": run.initial_commit,
                    },
                )
            ).scalar_one()
            lineage = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_promotions WHERE experiment=:id "
                            "ORDER BY rowid"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            history = (
                await conn.execute(
                    text(
                        "SELECT type,payload_json FROM experiment_events WHERE "
                        "experiment=:id ORDER BY sequence"
                    ),
                    {"id": str(id)},
                )
            ).all()
            await conn.execute(
                text(
                    "INSERT INTO "
                    "experiment_search_seals(experiment,initial_snapshot,"
                    "selected_final_snapshot,selected_final_commit,"
                    "accepted_lineage_digest,search_history_digest,stop_reason) "
                    "VALUES "
                    "(:id,:initial,:selected,:commit,:lineage,:history,:reason)"
                ),
                {
                    "id": str(id),
                    "initial": initial,
                    "selected": str(selected),
                    "commit": run.last_accepted_commit,
                    "lineage": digest(
                        [asdict(self._promotion_record(p)) for p in lineage]
                    ),
                    "history": digest(
                        [(r.type, json.loads(r.payload_json)) for r in history]
                    ),
                    "reason": stop_reason,
                },
            )
            await self._search_event(
                conn,
                id,
                "search_sealed",
                {
                    "selected_snapshot_id": str(selected),
                    "selected_commit": run.last_accepted_commit,
                },
            )
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_search_seals WHERE experiment=:id"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one()
            )
            return self._seal_record(row)

    async def _authorize_final(
        self,
        conn: AsyncConnection,
        id: UUID,
        snapshot: UUID,
        case: str,
        repeat: int,
        seal_digest: str,
    ) -> None:
        from dataclasses import asdict

        run = await self._get(conn, id)
        if not isinstance(run.manifest, ExperimentManifest):
            raise ValueError("final evaluation requires an experiment manifest")
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM experiment_search_seals WHERE experiment=:id"),
                    {"id": str(id)},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RevisionConflict("final measurement requires immutable search seal")
        seal = self._seal_record(row)
        if (
            run.stop_requested
            or run.status not in ("ready", "running")
            or digest(asdict(seal)) != seal_digest
            or snapshot
            not in (seal.initial_snapshot_id, seal.selected_final_snapshot_id)
            or repeat not in range(3)
            or case
            not in {c.case_id for c in run.manifest.cases if c.split == "final-test"}
        ):
            raise RevisionConflict("final authorization differs from seal")

    async def authorize_final(
        self, id: UUID, snapshot: UUID, case: str, repeat: int, seal_digest: str
    ) -> None:
        async with self.engine.connect() as conn:
            await self._authorize_final(conn, id, snapshot, case, repeat, seal_digest)

    async def audit_events(self, id: UUID) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT sequence,type,payload_json,created_at FROM "
                            "experiment_events WHERE experiment=:id ORDER BY "
                            "sequence"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    async def usage_records(self, id: UUID) -> tuple[dict[str, object], ...]:
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT reservation_id,kind,requested_json,settled_js"
                            "on,status FROM experiment_usage WHERE experiment=:id"
                            " ORDER BY reservation_id"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    async def publication(self, id: UUID) -> "PublicationRecord | None":
        from crucible.improvement.publication import PublicationRecord
        from crucible.repair.identity import canonical_json

        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_publications WHERE experiment=:id"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            data = dict(row)
            data["experiment_id"] = data.pop("experiment")
            return PublicationRecord.model_validate_json(canonical_json(data))

    async def prepare_publication(
        self, record: "PublicationRecord", remote_before: str | None
    ) -> None:
        from dataclasses import asdict

        seal = await self.search_seal(record.experiment_id)
        if seal is None or digest(asdict(seal)) != record.seal_digest:
            raise RevisionConflict("publication seal differs")
        async with self._transaction() as conn:
            existing = (
                await conn.execute(
                    text(
                        "SELECT experiment FROM experiment_publications WHERE "
                        "experiment=:id"
                    ),
                    {"id": str(record.experiment_id)},
                )
            ).first()
            if existing:
                raise RevisionConflict("publication intent already exists")
            await conn.execute(
                text(
                    "INSERT INTO experiment_publications(experiment,seal_digest,r"
                    "eport_digest,branch,expected_head,base_branch,remote_url,pr_"
                    "url,pr_number,status,last_error) VALUES (:experiment_id,:sea"
                    "l_digest,:report_digest,:branch,:expected_head,:base_branch,"
                    ":remote_url,:pr_url,:pr_number,:status,:last_error)"
                ),
                record.model_dump(mode="json"),
            )
            await self._event(
                conn,
                record.experiment_id,
                "publication_intent",
                {
                    "remote_before": remote_before,
                    "expected_head": record.expected_head,
                    "report_digest": record.report_digest,
                },
            )

    async def save_publication(self, record: "PublicationRecord") -> None:
        from crucible.improvement.publication import PublicationRecord
        from crucible.repair.identity import canonical_json

        async with self._transaction() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM experiment_publications WHERE experiment=:id"
                        ),
                        {"id": str(record.experiment_id)},
                    )
                )
                .mappings()
                .one()
            )
            data = dict(row)
            data["experiment_id"] = data.pop("experiment")
            existing = PublicationRecord.model_validate_json(canonical_json(data))
            mutable = {"pr_url", "pr_number", "status", "last_error"}
            if existing.model_dump(exclude=mutable) != record.model_dump(
                exclude=mutable
            ) or (
                existing.pr_number is not None
                and (record.pr_number, record.pr_url)
                != (existing.pr_number, existing.pr_url)
            ):
                raise RevisionConflict("publication identity immutable")
            await conn.execute(
                text(
                    "UPDATE experiment_publications SET pr_url=:pr_url,pr_number="
                    ":pr_number,status=:status,last_error=:last_error WHERE "
                    "experiment=:experiment_id"
                ),
                record.model_dump(mode="json"),
            )
            await self._event(
                conn,
                record.experiment_id,
                "publication_checkpoint",
                {"status": record.status, "error": record.last_error},
            )


class CalibrationStore(ExperimentStore[CalibrationManifest]):
    """Separate calibration database using the same quota/effect transactions.

    Default experiment stores cannot decode these records. Search and measured
    trial admission explicitly require ExperimentManifest and reject this kind.
    """

    manifest_type = CalibrationManifest

    async def admit_trial(self, id: UUID, snapshot: UUID, case_id: str) -> UUID:
        """One durable launch identity; unlike measured trials, never superseded."""
        async with self._transaction() as conn:
            record = await self._get(conn, id)
            if record.stop_requested or record.status != "ready":
                raise RevisionConflict("calibration is not ready")
            verified = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_snapshots WHERE id=:snapshot "
                        "AND experiment=:id AND status='verified'"
                    ),
                    {"snapshot": str(snapshot), "id": str(id)},
                )
            ).first()
            existing = (
                await conn.execute(
                    text("SELECT id FROM experiment_trials WHERE experiment=:id"),
                    {"id": str(id)},
                )
            ).first()
            if not verified or existing:
                raise ValueError(
                    "calibration snapshot missing or launch already admitted"
                )
            trial = record.manifest.trial_id
            await conn.execute(
                text(
                    "INSERT INTO experiment_trials "
                    "(id,experiment,snapshot,case_id,repeat_index,key,status,"
                    "generation) "
                    "VALUES (:trial,:id,:snapshot,:case,0,:key,'pending',0)"
                ),
                {
                    "trial": str(trial),
                    "id": str(id),
                    "snapshot": str(snapshot),
                    "case": case_id,
                    "key": record.manifest_hash,
                },
            )
            await self._event(
                conn, id, "calibration_admitted", {"trial_id": str(trial)}
            )
            return trial


ExecutionStore = ExperimentStore | CalibrationStore
