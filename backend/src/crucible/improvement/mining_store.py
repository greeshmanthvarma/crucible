"""Private diagnostic ledger and atomic logical-slot admission.

Physical jobs never own new allowances. All charges use the base usage rows;
slot scopes are joins onto that same ledger, including failed attempts.
"""

import json
import sqlite3
import stat
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Literal, Self
from uuid import UUID, uuid4

from pydantic import TypeAdapter
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from crucible.benchmarks.contracts import Sha256, TrialLimits
from crucible.improvement.contracts import (
    MiningDiagnosticManifest,
    UsageAmounts,
)
from crucible.improvement.manifest import manifest_digest
from crucible.improvement.mining_contracts import (
    AnalystAssignment,
    ExecutingSourceIdentity,
    MiningAttemptStatus,
    MiningDiagnosticStatus,
    MiningSlotStatus,
    SlotStatus,
)
from crucible.improvement.store import (
    BudgetDenied,
    ExperimentRecord,
    ExperimentStore,
    QuotaExhausted,
    RevisionConflict,
)
from crucible.improvement.worker_protocol import RoleJob, validate_job_manifest

_SCHEMA = (
    "CREATE TABLE mining_metadata (schema_version INTEGER NOT NULL)",
    """
CREATE TABLE mining_runs (
id TEXT PRIMARY KEY REFERENCES experiment_runs(id), config_digest TEXT NOT
NULL,
package_digest TEXT NOT NULL, phase TEXT NOT NULL, status TEXT NOT NULL,
final_output_ref TEXT, final_output_sha TEXT, error_code TEXT, revision
INTEGER NOT NULL)
""",
    """
CREATE TABLE mining_slots (
id TEXT PRIMARY KEY, diagnostic TEXT NOT NULL REFERENCES mining_runs(id),
ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 3), kind TEXT NOT NULL,
trial_id TEXT, limits_json TEXT NOT NULL, status TEXT NOT NULL,
correction_used INTEGER NOT NULL DEFAULT 0 CHECK(correction_used IN (0,1)),
satisfied_attempt_id TEXT, UNIQUE(diagnostic,ordinal))
""",
    """
CREATE TABLE mining_attempts (
id TEXT PRIMARY KEY, slot TEXT NOT NULL REFERENCES mining_slots(id),
ordinal INTEGER NOT NULL, job_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
retry_of TEXT REFERENCES mining_attempts(id), correction_of TEXT
REFERENCES mining_attempts(id),
status TEXT NOT NULL, job_json TEXT NOT NULL, input_sha TEXT NOT NULL,
output_ref TEXT, output_sha TEXT, receipts_ref TEXT, tool_call_count
INTEGER,
validation_json TEXT, outcome_ref TEXT,
authorization_id TEXT REFERENCES mining_retry_authorizations(id),
UNIQUE(slot,ordinal))
""",
    """
CREATE TABLE mining_retry_authorizations (
id TEXT PRIMARY KEY, slot TEXT NOT NULL REFERENCES mining_slots(id),
failed_attempt TEXT NOT NULL REFERENCES mining_attempts(id), config_digest
TEXT NOT NULL,
status TEXT NOT NULL CHECK(status IN ('available','consumed','cancelled')))
""",
    """
CREATE UNIQUE INDEX mining_one_retry ON
mining_retry_authorizations(failed_attempt)
WHERE status='available'
""",
    """
CREATE TABLE mining_usage_scopes (
reservation_id TEXT PRIMARY KEY REFERENCES experiment_usage(reservation_id),
slot TEXT NOT NULL REFERENCES mining_slots(id), attempt TEXT NOT NULL
REFERENCES mining_attempts(id),
job_id TEXT NOT NULL)
""",
)


@dataclass(frozen=True)
class JobAdmission:
    """One-use local delivery of a durably registered intent, never reconstructed."""

    job: RoleJob
    manifest_json: str
    max_requests: int


class MiningDiagnosticStore(ExperimentStore[MiningDiagnosticManifest]):
    manifest_type = MiningDiagnosticManifest

    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self._admissions: dict[UUID, JobAdmission] = {}
        self._schema_ready = False

    @classmethod
    async def open(cls, path: Path) -> Self:
        if (
            not path.is_absolute()
            or path.name != "accounting.sqlite3"
            or any(p.is_symlink() for p in (path, *path.parents))
        ):
            raise ValueError(
                "diagnostic requires its own nonsymlink accounting.sqlite3"
            )
        # Inspect existing databases read-only BEFORE base open can migrate them.
        if path.exists():
            if not stat.S_ISREG(path.stat().st_mode):
                raise ValueError("diagnostic ledger must be a regular file")
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
                if conn.execute("PRAGMA user_version").fetchone()[0] != 3:
                    raise ValueError("unsupported diagnostic base ledger version")
                tables = {
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "mining_metadata" not in tables:
                    raise ValueError("not a dedicated diagnostic ledger")
                if conn.execute(
                    "SELECT schema_version FROM mining_metadata"
                ).fetchall() != [(1,)]:
                    raise ValueError("unsupported diagnostic schema version")
                for (raw,) in conn.execute("SELECT manifest_json FROM experiment_runs"):
                    MiningDiagnosticManifest.model_validate_json(raw)
        store = await super().open(path)
        try:
            async with store._transaction() as conn:
                exists = (
                    await conn.execute(
                        text(
                            "SELECT name FROM sqlite_master WHERE "
                            "name='mining_metadata'"
                        )
                    )
                ).first()
                if not exists:
                    for sql in _SCHEMA:
                        await conn.execute(text(sql))
                    await conn.execute(text("INSERT INTO mining_metadata VALUES (1)"))
                version = (
                    (
                        await conn.execute(
                            text("SELECT schema_version FROM mining_metadata")
                        )
                    )
                    .scalars()
                    .all()
                )
                if version != [1]:
                    raise ValueError("unsupported diagnostic schema version")
            store._schema_ready = True
            return store
        except BaseException:
            await store.close()
            raise

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        async with super()._transaction() as conn:
            yield conn
            if self._schema_ready:
                # Base cleanup/accounting may stop a run. Mirror that terminal
                # state in this same transaction, never after a separate commit.
                await conn.execute(
                    text("""
UPDATE mining_runs SET status=CASE
WHEN (SELECT status FROM experiment_runs WHERE
id=mining_runs.id)='interrupted'
THEN 'interrupted' ELSE 'blocked' END, revision=revision+1
WHERE status NOT IN ('complete','closed','blocked','interrupted')
AND id IN (SELECT id FROM experiment_runs WHERE status IN
('error','interrupted','stopping'))
""")
                )
                await conn.execute(
                    text("""
UPDATE mining_runs SET status='ready',revision=revision+1
WHERE status IN ('blocked','interrupted') AND id IN (
SELECT id FROM experiment_runs WHERE status='ready' AND
stop_requested=0)
""")
                )

    async def create(
        self,
        manifest: MiningDiagnosticManifest,
        repository_identity: str,
        idempotency_key: str,
    ) -> ExperimentRecord[MiningDiagnosticManifest]:
        raise ValueError("use create_diagnostic to register logical slots atomically")

    async def create_diagnostic(
        self,
        manifest: MiningDiagnosticManifest,
        config_digest: str,
        package_digest: str,
        assignments: tuple[AnalystAssignment, ...],
    ) -> MiningDiagnosticStatus:
        manifest = MiningDiagnosticManifest.model_validate_json(
            manifest.model_dump_json()
        )
        TypeAdapter(Sha256).validate_python(config_digest, strict=True)
        if package_digest != manifest.package_digest:
            raise ValueError("diagnostic package identity mismatch")
        assignments = tuple(
            AnalystAssignment.model_validate_json(a.model_dump_json())
            for a in assignments
        )
        if (
            tuple(a.trial_id for a in assignments) != manifest.selected_trial_ids
            or len({a.slot_id for a in assignments}) != 3
        ):
            raise ValueError(
                "diagnostic requires exactly three distinct assigned slots"
            )
        for assignment in assignments:
            assignment.validate_manifest(manifest)
        async with self._transaction() as conn:
            if (
                await conn.execute(text("SELECT id FROM experiment_runs LIMIT 1"))
            ).first():
                raise RevisionConflict("diagnostic ledger already initialized")
            await conn.execute(
                text("""
INSERT INTO experiment_runs
(id,manifest_json,manifest_hash,initial_commit,
last_accepted_commit,status,stage,revision,idempotency_key)
VALUES (:id,:manifest,:hash,:commit,:commit,'ready','admission',0,:config)
"""),
                dict(
                    id=str(manifest.experiment_id),
                    manifest=manifest.model_dump_json(),
                    hash=manifest_digest(manifest),
                    commit=manifest.initial_commit,
                    config=config_digest,
                ),
            )
            await conn.execute(
                text("""
INSERT INTO mining_runs
(id,config_digest,package_digest,phase,status,revision)
VALUES (:id,:config,:package,'prepared','ready',0)
"""),
                dict(
                    id=str(manifest.experiment_id),
                    config=config_digest,
                    package=package_digest,
                ),
            )
            for ordinal in range(4):
                await conn.execute(
                    text("""
INSERT INTO mining_slots
(id,diagnostic,ordinal,kind,trial_id,limits_json,status)
VALUES (:slot,:id,:ordinal,:kind,:trial,:limits,'pending')
"""),
                    dict(
                        slot=str(
                            assignments[ordinal].slot_id if ordinal < 3 else uuid4()
                        ),
                        id=str(manifest.experiment_id),
                        ordinal=ordinal,
                        kind="trial_analysis" if ordinal < 3 else "mining_synthesis",
                        trial=str(assignments[ordinal].trial_id)
                        if ordinal < 3
                        else None,
                        limits=(
                            manifest.analyst_limits
                            if ordinal < 3
                            else manifest.synthesis_limits
                        ).model_dump_json(),
                    ),
                )
            await self._event(
                conn,
                manifest.experiment_id,
                "diagnostic_created",
                {"config_digest": config_digest},
            )
        return await self.diagnostic_status(manifest.experiment_id)

    async def _slot(self, conn: AsyncConnection, slot_id: UUID) -> RowMapping:
        return (
            (
                await conn.execute(
                    text("SELECT * FROM mining_slots WHERE id=:slot"),
                    {"slot": str(slot_id)},
                )
            )
            .mappings()
            .one()
        )

    async def _require_certain(self, conn: AsyncConnection, id: UUID) -> None:
        if (
            await conn.execute(
                text(
                    "SELECT reservation_id FROM experiment_usage WHERE experiment=:id "
                    "AND status='uncertain' LIMIT 1"
                ),
                {"id": str(id)},
            )
        ).first():
            raise BudgetDenied("uncertain usage requires explicit recovery")
        if (
            await conn.execute(
                text(
                    "SELECT id FROM experiment_effects WHERE experiment=:id AND "
                    "status='uncertain' LIMIT 1"
                ),
                {"id": str(id)},
            )
        ).first():
            raise BudgetDenied("uncertain cleanup requires explicit recovery")
        if (
            await conn.execute(
                text("""
SELECT a.id FROM mining_attempts a JOIN mining_slots s ON a.slot=s.id
WHERE s.diagnostic=:id AND a.status NOT IN ('running','pending')
AND (a.tool_call_count IS NULL OR
json_extract(a.validation_json,'$.cleanup_confirmed')=0) LIMIT 1
"""),
                {"id": str(id)},
            )
        ).first():
            raise BudgetDenied("unknown tool usage requires explicit recovery")

    async def _require_reconciled(self, conn: AsyncConnection, id: UUID) -> None:
        unresolved = (
            await conn.execute(
                text("""
SELECT 1 FROM experiment_usage
WHERE experiment=:id AND status NOT IN ('settled','conservative')
UNION ALL SELECT 1 FROM experiment_effects
WHERE experiment=:id AND status!='stopped'
"""),
                {"id": str(id)},
            )
        ).first()
        if unresolved:
            raise BudgetDenied(
                "retry or correction requires settled usage and confirmed cleanup"
            )

    def _caps(
        self, limits: TrialLimits, manifest: MiningDiagnosticManifest, kind: str
    ) -> UsageAmounts:
        return UsageAmounts(
            tokens=limits.max_model_tokens,
            requests=manifest.analyst_max_requests
            if kind == "trial_analysis"
            else manifest.synthesis_max_requests,
            active_seconds=limits.max_active_seconds,
            worker_wall_seconds=limits.wall_timeout_seconds,
            output_bytes=manifest.budget.max_total_output_bytes,
        )

    async def _slot_usage(
        self,
        conn: AsyncConnection,
        slot: RowMapping,
        manifest: MiningDiagnosticManifest,
    ) -> dict[str, UsageAmounts]:
        buckets = {
            key: dict.fromkeys(UsageAmounts.model_fields, 0)
            for key in ("spent", "reserved", "retained")
        }
        rows = (
            (
                await conn.execute(
                    text("""
SELECT u.* FROM experiment_usage u
JOIN mining_usage_scopes s ON s.reservation_id=u.reservation_id
WHERE s.slot=:slot
"""),
                    {"slot": slot["id"]},
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            requested = UsageAmounts.model_validate_json(row["requested_json"])
            observed = UsageAmounts.model_validate_json(row["settled_json"] or "{}")
            key = (
                "spent"
                if row["status"] == "settled"
                else "reserved"
                if row["status"] == "reserved"
                else "retained"
            )
            for name in UsageAmounts.model_fields:
                buckets[key][name] += (
                    getattr(observed, name)
                    if row["status"] in ("settled", "conservative")
                    else max(getattr(requested, name), getattr(observed, name))
                )
        caps = self._caps(
            TrialLimits.model_validate_json(slot["limits_json"]), manifest, slot["kind"]
        )
        result = {key: UsageAmounts(**value) for key, value in buckets.items()}
        result["remaining"] = UsageAmounts(
            **{
                name: max(
                    0,
                    getattr(caps, name)
                    - sum(value[name] for value in buckets.values()),
                )
                for name in UsageAmounts.model_fields
            }
        )
        return result

    async def slot_usage(self, slot_id: UUID) -> dict[str, UsageAmounts]:
        async with self.engine.connect() as conn:
            slot = await self._slot(conn, slot_id)
            run = await self._get(conn, UUID(slot["diagnostic"]))
            return await self._slot_usage(conn, slot, run.manifest)

    async def register_attempt(
        self,
        slot_id: UUID,
        job: RoleJob,
        input_sha: str,
        retry_authorization_id: UUID | None = None,
    ) -> UUID:
        job = RoleJob.model_validate_json(job.model_dump_json())
        if input_sha != job.input_digest:
            raise ValueError("job input digest mismatch")
        async with self._transaction() as conn:
            slot = await self._slot(conn, slot_id)
            id = UUID(slot["diagnostic"])
            run = await self._get(conn, id)
            validate_job_manifest(job, run.manifest)
            if (
                await conn.execute(
                    text("SELECT id FROM mining_attempts WHERE job_id=:job"),
                    {"job": str(job.job_id)},
                )
            ).first():
                raise RevisionConflict("job identity already registered")
            exports = (
                (
                    await conn.execute(
                        text("""
SELECT DISTINCT json_extract(a.job_json,'$.export_digest')
FROM mining_attempts a JOIN mining_slots s ON a.slot=s.id
WHERE s.diagnostic=:id
"""),
                        {"id": str(id)},
                    )
                )
                .scalars()
                .all()
            )
            if exports and exports != [job.export_digest]:
                raise ValueError("diagnostic executing export identity drift")
            if job.budget_slot_id != slot_id or job.experiment_id != id:
                raise ValueError("job slot identity mismatch")
            target = (
                job.correction_target
                if job.job_kind == "format_correction"
                else job.job_kind
            )
            if (
                target != slot["kind"]
                or job.harness_digest != run.manifest.initial_source_digest
            ):
                raise ValueError("job frozen source or slot kind mismatch")
            if run.stop_requested or run.status not in ("ready", "running"):
                raise BudgetDenied("diagnostic is stopped")
            await self._require_certain(conn, id)
            previous = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM mining_attempts WHERE slot=:slot ORDER "
                            "BY ordinal "
                            "DESC LIMIT 1"
                        ),
                        {"slot": str(slot_id)},
                    )
                )
                .mappings()
                .first()
            )
            correction_of = retry_of = None
            if previous:
                await self._require_reconciled(conn, id)
                if previous["status"] in ("running", "complete"):
                    raise RevisionConflict("slot already running or complete")
                if job.job_kind == "format_correction":
                    validation = json.loads(previous["validation_json"] or "{}")
                    if (
                        previous["kind"] != "primary"
                        or previous["status"] != "invalid"
                        or slot["correction_used"]
                        or not validation.get("normal_completion")
                        or job.correction_of_job_id != UUID(previous["job_id"])
                        or retry_authorization_id is not None
                    ):
                        raise RevisionConflict(
                            "correction requires latest normal invalid primary"
                        )
                    correction_of = previous["id"]
                else:
                    if retry_authorization_id is None:
                        raise RevisionConflict("retry requires explicit authorization")
                    auth = (
                        await conn.execute(
                            text("""
SELECT * FROM mining_retry_authorizations
WHERE id=:auth AND slot=:slot AND failed_attempt=:failed
AND status='available' AND config_digest=(SELECT config_digest
FROM mining_runs WHERE id=:id)
"""),
                            dict(
                                auth=str(retry_authorization_id),
                                slot=str(slot_id),
                                failed=previous["id"],
                                id=str(id),
                            ),
                        )
                    ).first()
                    if auth is None:
                        raise RevisionConflict(
                            "retry authorization unavailable or mismatched"
                        )
                    retry_of = previous["id"]
            elif (
                job.job_kind == "format_correction"
                or retry_authorization_id is not None
            ):
                raise RevisionConflict("correction or retry has no original attempt")
            remaining = (await self._slot_usage(conn, slot, run.manifest))["remaining"]
            frozen = TrialLimits.model_validate_json(slot["limits_json"])
            tool_count = (
                await conn.execute(
                    text(
                        "SELECT COALESCE(SUM(tool_call_count),0) FROM mining_attempts "
                        "WHERE slot=:slot"
                    ),
                    {"slot": str(slot_id)},
                )
            ).scalar_one()
            tool_remaining = frozen.max_tool_calls - tool_count
            if min(
                remaining.tokens,
                remaining.requests,
                remaining.active_seconds,
                remaining.worker_wall_seconds,
            ) <= 0 or (correction_of is None and tool_remaining <= 0):
                raise QuotaExhausted("diagnostic slot has no remaining allowance")
            expected = TrialLimits(
                max_model_tokens=remaining.tokens,
                max_steps=min(frozen.max_steps, remaining.requests),
                max_tool_calls=1 if correction_of else tool_remaining,
                max_active_seconds=remaining.active_seconds,
                wall_timeout_seconds=remaining.worker_wall_seconds,
            )
            if job.limits != expected:
                raise ValueError("job limits must equal registered remaining allowance")
            attempt = uuid4()
            await conn.execute(
                text("""
INSERT INTO mining_attempts
(id,slot,ordinal,job_id,kind,status,job_json,input_sha,retry_of,
correction_of,authorization_id)
VALUES (:attempt,:slot,:ordinal,:job,:kind,'running',:json,:sha,
:retry,:correction,:auth)
"""),
                dict(
                    attempt=str(attempt),
                    slot=str(slot_id),
                    job=str(job.job_id),
                    json=job.model_dump_json(),
                    sha=input_sha,
                    ordinal=previous["ordinal"] + 1 if previous else 0,
                    kind="format_correction" if correction_of else "primary",
                    retry=retry_of,
                    correction=correction_of,
                    auth=str(retry_authorization_id)
                    if retry_authorization_id
                    else None,
                ),
            )
            if correction_of:
                await conn.execute(
                    text("UPDATE mining_slots SET correction_used=1 WHERE id=:slot"),
                    {"slot": str(slot_id)},
                )
            if retry_authorization_id:
                await conn.execute(
                    text(
                        "UPDATE mining_retry_authorizations SET status='consumed' "
                        "WHERE "
                        "id=:auth"
                    ),
                    {"auth": str(retry_authorization_id)},
                )
            await conn.execute(
                text("UPDATE mining_slots SET status=:status WHERE id=:slot"),
                {
                    "slot": str(slot_id),
                    "status": "correcting" if correction_of else "running",
                },
            )
            phase = "analyzing" if slot["kind"] == "trial_analysis" else "synthesizing"
            await conn.execute(
                text(
                    "UPDATE mining_runs SET "
                    "status='running',phase=:phase,revision=revision+1 WHERE id=:id"
                ),
                dict(id=str(id), phase=phase),
            )
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET "
                    "status='running',stage='mining',revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id)},
            )
            await self._event(
                conn,
                id,
                "mining_attempt_registered",
                {
                    "attempt_id": str(attempt),
                    "job_id": str(job.job_id),
                    "slot_id": str(slot_id),
                },
            )
        self._admissions[job.job_id] = JobAdmission(
            job, run.manifest.model_dump_json(), remaining.requests
        )
        return attempt

    async def finish_attempt(
        self,
        attempt_id: UUID,
        status: SlotStatus,
        *,
        tool_call_count: int | None,
        normal_completion: bool,
        cleanup_confirmed: bool,
        output_ref: str | None = None,
        output_sha: str | None = None,
        receipts_ref: str | None = None,
        outcome_ref: str | None = None,
        error_code: str | None = None,
        final_disposition: str | None = None,
        artifact_hashes: dict[str, str] | None = None,
    ) -> None:
        """Checkpoint host-observed termination; report acceptance is a later gate."""
        if status not in (
            "complete",
            "invalid",
            "incomplete",
            "budget_exhausted",
            "infrastructure_error",
            "interrupted",
        ):
            raise ValueError("attempt must terminate explicitly")
        if tool_call_count is not None and (
            type(tool_call_count) is not int or tool_call_count < 0
        ):
            raise ValueError("invalid tool call count")
        if status in ("complete", "invalid", "incomplete") and not normal_completion:
            raise ValueError("report validation requires normal completion")
        async with self._transaction() as conn:
            attempt = (
                (
                    await conn.execute(
                        text("SELECT * FROM mining_attempts WHERE id=:id"),
                        {"id": str(attempt_id)},
                    )
                )
                .mappings()
                .one()
            )
            if attempt["status"] != "running":
                raise RevisionConflict("attempt already terminal")
            slot = await self._slot(conn, UUID(attempt["slot"]))
            id = UUID(slot["diagnostic"])
            job = RoleJob.model_validate_json(attempt["job_json"])
            before = await self._get(conn, id)
            if before.status in ("stopping", "interrupted"):
                status, normal_completion = "interrupted", False
            if status in ("complete", "invalid", "incomplete"):
                await self._require_certain(conn, id)
                await self._require_reconciled(conn, id)
            if attempt["kind"] == "format_correction" and tool_call_count not in (
                0,
                None,
            ):
                raise ValueError("format corrections have no tools")
            if (
                tool_call_count is not None
                and tool_call_count > job.limits.max_tool_calls
            ):
                status = "budget_exhausted"
            if not cleanup_confirmed or tool_call_count is None:
                status = "infrastructure_error"
            await conn.execute(
                text("""
UPDATE mining_attempts SET status=:status,
tool_call_count=:calls,validation_json=:validation,
output_ref=:output,output_sha=:sha,receipts_ref=:receipts,
outcome_ref=:outcome WHERE id=:id
"""),
                dict(
                    id=str(attempt_id),
                    status=status,
                    calls=tool_call_count,
                    output=output_ref,
                    sha=output_sha,
                    receipts=receipts_ref,
                    outcome=outcome_ref,
                    validation=json.dumps(
                        dict(
                            normal_completion=normal_completion,
                            cleanup_confirmed=cleanup_confirmed,
                            error_code=error_code,
                            final_disposition=final_disposition,
                            artifact_hashes=artifact_hashes,
                        )
                    ),
                ),
            )
            await conn.execute(
                text("""
UPDATE mining_slots SET status=:status,
satisfied_attempt_id=:attempt WHERE id=:slot
"""),
                dict(
                    slot=slot["id"],
                    status=status,
                    attempt=str(attempt_id) if status == "complete" else None,
                ),
            )
            if status != "complete":
                # Correction may be registered only after a normal invalid report;
                # all other failures need explicit recovery/retry authorization.
                await conn.execute(
                    text(
                        "UPDATE mining_runs SET status=:status,revision=revision+1 "
                        "WHERE "
                        "id=:id"
                    ),
                    {
                        "id": str(id),
                        "status": "interrupted"
                        if status == "interrupted"
                        else "blocked",
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE experiment_runs SET "
                        "status=:status,stop_requested=1,revision=revision+1 "
                        "WHERE id=:id"
                    ),
                    {
                        "id": str(id),
                        "status": "interrupted" if status == "interrupted" else "error",
                    },
                )
                if (
                    status == "invalid"
                    and normal_completion
                    and cleanup_confirmed
                    and tool_call_count is not None
                    and not slot["correction_used"]
                    and not before.stop_requested
                ):
                    await conn.execute(
                        text("UPDATE mining_runs SET status='ready' WHERE id=:id"),
                        {"id": str(id)},
                    )
                    await conn.execute(
                        text(
                            "UPDATE experiment_runs SET "
                            "status='ready',stop_requested=0 WHERE "
                            "id=:id"
                        ),
                        {"id": str(id)},
                    )
            await self._event(
                conn,
                id,
                "mining_attempt_terminal",
                {"attempt_id": str(attempt_id), "status": status},
            )
        self._admissions.pop(job.job_id, None)

    async def authorize_retry(
        self, diagnostic_id: UUID, slot_id: UUID, failed_attempt_id: UUID
    ) -> UUID:
        async with self._transaction() as conn:
            slot = await self._slot(conn, slot_id)
            if slot["diagnostic"] != str(diagnostic_id):
                raise ValueError("retry diagnostic identity mismatch")
            await self._require_certain(conn, diagnostic_id)
            await self._require_reconciled(conn, diagnostic_id)
            previous = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM mining_attempts WHERE slot=:slot ORDER "
                            "BY ordinal "
                            "DESC LIMIT 1"
                        ),
                        {"slot": str(slot_id)},
                    )
                )
                .mappings()
                .first()
            )
            if (
                previous is None
                or previous["id"] != str(failed_attempt_id)
                or previous["status"] in ("running", "complete")
            ):
                raise RevisionConflict("retry targets only the latest failed attempt")
            run = await self._get(conn, diagnostic_id)
            if run.stop_requested or run.status != "ready":
                raise RevisionConflict("recover before authorizing retries")
            remaining = (await self._slot_usage(conn, slot, run.manifest))["remaining"]
            used_tools = (
                await conn.execute(
                    text(
                        "SELECT COALESCE(SUM(tool_call_count),0) "
                        "FROM mining_attempts WHERE slot=:slot"
                    ),
                    {"slot": str(slot_id)},
                )
            ).scalar_one()
            if (
                used_tools
                >= TrialLimits.model_validate_json(slot["limits_json"]).max_tool_calls
            ):
                raise QuotaExhausted("retry cannot fit remaining tool allowance")
            if (
                min(
                    remaining.tokens,
                    remaining.requests,
                    remaining.active_seconds,
                    remaining.worker_wall_seconds,
                )
                <= 0
            ):
                raise QuotaExhausted("retry cannot fit remaining slot allowance")
            existing = (
                await conn.execute(
                    text(
                        "SELECT id FROM mining_retry_authorizations WHERE "
                        "failed_attempt=:failed AND status='available'"
                    ),
                    {"failed": str(failed_attempt_id)},
                )
            ).scalar_one_or_none()
            if existing:
                return UUID(existing)
            authorization = uuid4()
            await conn.execute(
                text("""
INSERT INTO mining_retry_authorizations
(id,slot,failed_attempt,config_digest,status)
SELECT :auth,:slot,:failed,config_digest,'available' FROM
mining_runs WHERE id=:id
"""),
                dict(
                    auth=str(authorization),
                    slot=str(slot_id),
                    failed=str(failed_attempt_id),
                    id=str(diagnostic_id),
                ),
            )
            await self._event(
                conn,
                diagnostic_id,
                "mining_retry_authorized",
                {
                    "authorization_id": str(authorization),
                    "failed_attempt_id": str(failed_attempt_id),
                },
            )
            return authorization

    def take_job_admission(
        self, job: RoleJob, manifest: MiningDiagnosticManifest
    ) -> JobAdmission:
        admission = self._admissions.pop(job.job_id, None)
        if (
            admission is None
            or admission.job != job
            or admission.manifest_json != manifest.model_dump_json()
        ):
            raise BudgetDenied("diagnostic job has no unused registered admission")
        return admission

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
            attempt = (
                (
                    await conn.execute(
                        text("""
SELECT a.*,s.diagnostic,s.kind AS slot_kind,s.limits_json FROM
mining_attempts a
JOIN mining_slots s ON s.id=a.slot WHERE a.job_id=:job AND
s.diagnostic=:id
"""),
                        dict(job=str(job_id), id=str(id)),
                    )
                )
                .mappings()
                .first()
            )
            if attempt is None or attempt["status"] != "running":
                raise BudgetDenied(
                    "diagnostic reservation requires registered live job"
                )
            if trial_id is not None or request.trial_starts or kind == "command":
                raise BudgetDenied("diagnostic commands and trial starts forbidden")
            if kind not in ("model", "worker"):
                raise BudgetDenied("unknown diagnostic reservation kind")
            await self._require_certain(conn, id)
            slot = await self._slot(conn, UUID(attempt["slot"]))
            run = await self._get(conn, id)
            usage = await self._slot_usage(conn, slot, run.manifest)
            remaining = usage["remaining"]
            if any(
                getattr(request, name) > getattr(remaining, name)
                for name in UsageAmounts.model_fields
            ):
                raise QuotaExhausted("diagnostic slot budget exceeded")
            reservation = await self._reserve_usage_in_transaction(
                conn, id, None, request, kind=kind, job_id=job_id
            )
            await conn.execute(
                text(
                    "INSERT INTO mining_usage_scopes VALUES (:rid,:slot,:attempt,:job)"
                ),
                dict(
                    rid=str(reservation),
                    slot=attempt["slot"],
                    attempt=attempt["id"],
                    job=str(job_id),
                ),
            )
            return reservation

    async def diagnostic_status(self, id: UUID) -> MiningDiagnosticStatus:
        async with self.engine.connect() as conn:
            run = await self._get(conn, id)
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM mining_runs WHERE id=:id"), {"id": str(id)}
                    )
                )
                .mappings()
                .one()
            )
            slots = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM mining_slots WHERE diagnostic=:id ORDER "
                            "BY ordinal"
                        ),
                        {"id": str(id)},
                    )
                )
                .mappings()
                .all()
            )
            values = []
            for slot in slots:
                attempts = (
                    (
                        await conn.execute(
                            text(
                                "SELECT * FROM mining_attempts WHERE slot=:slot "
                                "ORDER BY ordinal"
                            ),
                            {"slot": slot["id"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                values.append(
                    MiningSlotStatus(
                        slot_id=UUID(slot["id"]),
                        ordinal=slot["ordinal"],
                        kind=slot["kind"],
                        trial_id=UUID(slot["trial_id"]) if slot["trial_id"] else None,
                        status=slot["status"],
                        attempts=tuple(
                            MiningAttemptStatus(
                                attempt_id=UUID(a["id"]),
                                job_id=UUID(a["job_id"]),
                                kind=a["kind"],
                                status=a["status"],
                                error_code=json.loads(a["validation_json"] or "{}").get(
                                    "error_code"
                                ),
                            )
                            for a in attempts
                        ),
                        limits=TrialLimits.model_validate_json(slot["limits_json"]),
                        **await self._slot_usage(conn, slot, run.manifest),
                    )
                )
            uncertain = (
                await conn.execute(
                    text(
                        "SELECT id FROM experiment_effects WHERE experiment=:id AND "
                        "status!='stopped' LIMIT 1"
                    ),
                    {"id": str(id)},
                )
            ).first()
            failed_cleanup = (
                await conn.execute(
                    text("""SELECT a.id FROM mining_attempts a
                JOIN mining_slots s ON a.slot=s.id WHERE s.diagnostic=:id
                AND json_extract(a.validation_json,'$.cleanup_confirmed')=0 LIMIT 1"""),
                    {"id": str(id)},
                )
            ).first()
            next_action = (
                "continue"
                if row["status"] in ("ready", "running")
                else "none"
                if row["status"] in ("complete", "closed")
                else "recover"
            )
            pending = next((slot for slot in values if slot.status != "complete"), None)
            if row["status"] == "ready" and pending is not None and pending.attempts:
                authorization = (
                    await conn.execute(
                        text("""SELECT id FROM mining_retry_authorizations
                    WHERE slot=:slot AND failed_attempt=:attempt
                    AND status='available'"""),
                        {
                            "slot": str(pending.slot_id),
                            "attempt": str(pending.attempts[-1].attempt_id),
                        },
                    )
                ).first()
                if authorization is None:
                    next_action = "authorize-retry"
            return MiningDiagnosticStatus(
                diagnostic_id=id,
                status=row["status"],
                phase=row["phase"],
                selected_trial_ids=run.manifest.selected_trial_ids,
                slots=tuple(values),
                next_action=next_action,
                final_disposition=(
                    await conn.execute(
                        text("""
SELECT json_extract(a.validation_json,'$.final_disposition')
FROM mining_attempts a JOIN mining_slots s ON a.slot=s.id
WHERE s.diagnostic=:id AND s.ordinal=3 AND a.status='complete'
ORDER BY a.ordinal DESC LIMIT 1
"""),
                        {"id": str(id)},
                    )
                ).scalar_one_or_none()
                if row["status"] in ("complete", "closed")
                else None,
                cleanup_confirmed=not bool(uncertain or failed_cleanup),
                config_digest=row["config_digest"],
                investigation_source=run.manifest.investigation_source,
                executing_source=ExecutingSourceIdentity(
                    commit=run.manifest.initial_commit,
                    source_digest=run.manifest.initial_source_digest,
                    controller_digest=run.manifest.controller_digest,
                    dependency_lock_digest=run.manifest.dependency_lock_digest,
                ),
            )

    async def attempt_record(self, attempt_id: UUID) -> RowMapping:
        async with self.engine.connect() as conn:
            return (
                (
                    await conn.execute(
                        text("SELECT * FROM mining_attempts WHERE id=:id"),
                        {"id": str(attempt_id)},
                    )
                )
                .mappings()
                .one()
            )

    async def block_diagnostic(self, id: UUID, code: str) -> None:
        await self.fail_run(id, code)
        async with self._transaction() as conn:
            await conn.execute(
                text(
                    "UPDATE mining_runs SET status='blocked',error_code=:code,"
                    "revision=revision+1 WHERE id=:id "
                    "AND status NOT IN ('complete','closed')"
                ),
                {"id": str(id), "code": code},
            )

    async def complete_diagnostic(
        self, id: UUID, output_ref: str, output_sha: str
    ) -> None:
        async with self._transaction() as conn:
            run = await self._get(conn, id)
            if run.stop_requested or run.status not in ("ready", "running"):
                raise BudgetDenied("diagnostic stopped before final checkpoint")
            await self._require_certain(conn, id)
            await self._require_reconciled(conn, id)
            slots = (
                (
                    await conn.execute(
                        text("SELECT status FROM mining_slots WHERE diagnostic=:id"),
                        {"id": str(id)},
                    )
                )
                .scalars()
                .all()
            )
            if slots != ["complete"] * 4:
                raise RevisionConflict("all four validated slots required")
            await conn.execute(
                text("""UPDATE mining_runs SET status='complete',phase='finished',
                final_output_ref=:ref,final_output_sha=:sha,
                revision=revision+1 WHERE id=:id"""),
                {"id": str(id), "ref": output_ref, "sha": output_sha},
            )
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET status='closed',stage='finished',"
                    "revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id)},
            )
            await self._event(
                conn, id, "mining_diagnostic_complete", {"sha256": output_sha}
            )

    @classmethod
    async def open_readonly(cls, path: Path) -> Self:
        """No schema creation, migration, WAL-mode change, or write transaction."""
        if (
            not path.is_absolute()
            or path.name != "accounting.sqlite3"
            or any(p.is_symlink() for p in (path, *path.parents))
            or not path.is_file()
        ):
            raise ValueError("existing private diagnostic ledger required")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
            if conn.execute("PRAGMA user_version").fetchone()[0] != 3 or conn.execute(
                "SELECT schema_version FROM mining_metadata"
            ).fetchall() != [(1,)]:
                raise ValueError("unsupported diagnostic ledger")
            for (raw,) in conn.execute("SELECT manifest_json FROM experiment_runs"):
                MiningDiagnosticManifest.model_validate_json(raw)
        return cls(
            create_async_engine(
                "sqlite+aiosqlite:///" + path.as_uri() + "?mode=ro&uri=true"
            )
        )

    async def retry_authorization(self, slot_id: UUID) -> UUID | None:
        async with self.engine.connect() as conn:
            value = (
                await conn.execute(
                    text("""
SELECT r.id FROM mining_retry_authorizations r JOIN mining_slots s ON r.slot=s.id
JOIN mining_runs m ON m.id=s.diagnostic WHERE r.slot=:slot AND r.status='available'
AND r.config_digest=m.config_digest AND r.failed_attempt=(SELECT id FROM mining_attempts
WHERE slot=:slot ORDER BY ordinal DESC LIMIT 1)
"""),
                    {"slot": str(slot_id)},
                )
            ).scalar_one_or_none()
            return UUID(value) if value else None

    async def recovery_ready(self, id: UUID, owner: UUID) -> None:
        async with self._transaction() as conn:
            await self._require_owner(conn, id, owner)
            await self._require_certain(conn, id)
            await self._require_reconciled(conn, id)
        await self.recover_ready(id, owner)

    async def reconcile_attempt(
        self, attempt_id: UUID, count: int | None, *, cleanup_confirmed: bool = True
    ) -> None:
        """Restore only provable counts and cleanup, never accept an orphan report."""
        row = await self.attempt_record(attempt_id)
        if row["status"] == "running":
            await self.finish_attempt(
                attempt_id,
                "interrupted",
                tool_call_count=count,
                normal_completion=False,
                cleanup_confirmed=cleanup_confirmed,
            )
            return
        async with self._transaction() as conn:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM mining_attempts WHERE id=:id"),
                        {"id": str(attempt_id)},
                    )
                )
                .mappings()
                .one()
            )
            validation = json.loads(row["validation_json"] or "{}")
            validation["cleanup_confirmed"] = cleanup_confirmed
            if (
                row["tool_call_count"] is not None
                and count is not None
                and count != row["tool_call_count"]
            ):
                raise ValueError("recovered tool count drift")
            await conn.execute(
                text("""UPDATE mining_attempts SET
                tool_call_count=COALESCE(tool_call_count,:count),validation_json=:json
                WHERE id=:id"""),
                {"id": str(attempt_id), "count": count, "json": json.dumps(validation)},
            )

    async def close_diagnostic(self, id: UUID) -> None:
        async with self._transaction() as conn:
            await self._require_reconciled(conn, id)
            if (
                await conn.execute(
                    text("""SELECT a.id FROM mining_attempts a
                JOIN mining_slots s ON a.slot=s.id
                WHERE s.diagnostic=:id AND a.status='running'"""),
                    {"id": str(id)},
                )
            ).first():
                raise RevisionConflict(
                    "active attempt requires recovery before closing"
                )
            await conn.execute(
                text(
                    "UPDATE mining_runs SET status='closed',phase='finished',"
                    "revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id)},
            )
            await conn.execute(
                text(
                    "UPDATE experiment_runs SET status='closed',stage='finished',"
                    "stop_requested=1,"
                    "revision=revision+1 WHERE id=:id"
                ),
                {"id": str(id)},
            )
            await self._event(conn, id, "mining_diagnostic_closed", {})

    async def final_checkpoint(self, id: UUID) -> tuple[str | None, str | None]:
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT final_output_ref,final_output_sha "
                        "FROM mining_runs WHERE id=:id"
                    ),
                    {"id": str(id)},
                )
            ).one()
            return row[0], row[1]

    async def attempt_usage(self, attempt_id: UUID) -> tuple[RowMapping, ...]:
        async with self.engine.connect() as conn:
            return tuple(
                (
                    await conn.execute(
                        text("""SELECT u.* FROM experiment_usage u
                JOIN mining_usage_scopes s ON s.reservation_id=u.reservation_id
                WHERE s.attempt=:id"""),
                        {"id": str(attempt_id)},
                    )
                )
                .mappings()
                .all()
            )

    async def request_diagnostic_stop(self, id: UUID) -> None:
        async with self._transaction() as conn:
            await self._get(conn, id)
            await conn.execute(
                text("""UPDATE experiment_runs SET status='interrupted',
                stop_requested=1,error_code='operator_stopped',revision=revision+1
                WHERE id=:id AND status!='closed'"""),
                {"id": str(id)},
            )
            await conn.execute(
                text("""UPDATE mining_runs SET status='interrupted',
                error_code='operator_stopped',revision=revision+1
                WHERE id=:id AND status NOT IN ('complete','closed')"""),
                {"id": str(id)},
            )
            await self._event(conn, id, "mining_diagnostic_stopped", {})
