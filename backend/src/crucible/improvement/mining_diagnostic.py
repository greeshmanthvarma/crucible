"""Offline frozen inputs and sequential, read-only diagnostic orchestration."""

# Host-only coordinator; none of this module is exported to a worker.
import asyncio
import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from pydantic import TypeAdapter
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import NoResultFound

from crucible.benchmarks.contracts import TrialLimits
from crucible.improvement.contracts import (
    EDITABLE_PATHS,
    HarnessSnapshot,
    MiningDiagnosticManifest,
    UsageAmounts,
)
from crucible.improvement.manifest import manifest_digest
from crucible.improvement.mining_contracts import (
    AnalystAssignment,
    InspectionReceipt,
    MiningDiagnosticConfig,
    MiningDiagnosticStatus,
    MiningSlotStatus,
    PreparedMiningDiagnostic,
    SlotStatus,
    SynthesisAssignment,
    SynthesisReport,
    TrialAnalysisReport,
    ValidatedAnalysisRef,
)
from crucible.improvement.mining_inspection import read_inspection_receipts
from crucible.improvement.mining_store import MiningDiagnosticStore
from crucible.improvement.mining_validation import (
    MiningValidationError,
    load_inspection_package,
    validate_analysis_report,
    validate_synthesis_report,
)
from crucible.improvement.role_prompts import role_system_prompt
from crucible.improvement.source import (
    _digest,
    git_bytes,
    inspect_source,
    safe_path,
    stage_harness,
)
from crucible.improvement.store import BudgetDenied
from crucible.improvement.trajectory_contracts import EvidencePackage, TrialOverview
from crucible.improvement.trajectory_export import verify_trajectory_package
from crucible.improvement.worker_protocol import (
    IsolatedRoleExecution,
    JobOutcome,
    RoleJob,
    read_role_observation,
)
from crucible.improvement.worker_supervisor import (
    DockerPort,
    DockerRuntime,
    WorkerSupervisor,
    labels,
    read_regular_artifact,
    remove_owned_effect,
)
from crucible.repair.identity import canonical_json


def _write(path: Path, raw: bytes) -> None:
    """Durably preserve exact bytes without sanitizing validated model artifacts."""
    safe_path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=".mining-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _json(path: Path, value: object) -> None:
    _write(path, canonical_json(value))


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def diagnostic_policy_digests(package: EvidencePackage) -> dict[str, str]:
    """Content identities operators pin before offline preparation."""
    return {
        "selection_digest": _sha(canonical_json(package.selection)),
        "export_config_digest": _sha(canonical_json(package.limits)),
        "report_schema_digest": _sha(
            canonical_json(
                [
                    TrialAnalysisReport.model_json_schema(),
                    SynthesisReport.model_json_schema(),
                ]
            )
        ),
        "prompt_digest": _sha(
            canonical_json(
                [
                    role_system_prompt("trial_analysis"),
                    role_system_prompt("mining_synthesis"),
                    role_system_prompt(
                        "format_correction", correction_target="trial_analysis"
                    ),
                    role_system_prompt(
                        "format_correction", correction_target="mining_synthesis"
                    ),
                ]
            )
        ),
    }


def _registry(root: Path) -> dict[str, list[object]]:
    result: dict[str, list[object]] = {}
    total = 0
    for path in sorted(root.rglob("*")):
        safe_path(path)
        if path.is_dir():
            continue
        raw = read_regular_artifact(path, 2 * 1024 * 1024)
        total += len(raw)
        if total > 64 * 1024 * 1024 or len(result) >= 8192:
            raise ValueError("frozen view exceeds limits")
        result[path.relative_to(root).as_posix()] = [
            path.stat().st_mode & 0o777,
            _sha(raw),
        ]
    return result


def freeze_mining_diagnostic(
    config: MiningDiagnosticConfig, output: Path
) -> PreparedMiningDiagnostic:
    """Copy verified positive inputs only; never instantiate any execution port."""
    config = MiningDiagnosticConfig.model_validate_json(config.model_dump_json())
    for path in (config.repository, config.package_dir, output):
        safe_path(path)
        if not path.is_absolute():
            raise ValueError("absolute diagnostic paths required")
    if output.exists() or any(
        output.is_relative_to(p) or p.is_relative_to(output)
        for p in (config.repository, config.package_dir)
    ):
        raise ValueError("new nonoverlapping diagnostic output required")
    manifest = config.manifest
    package = verify_trajectory_package(config.package_dir)
    if (
        package.bundle_digest != manifest.package_digest
        or package.source != manifest.investigation_source
        or package.selected_trial_ids != manifest.selected_trial_ids
    ):
        raise ValueError("package identity mismatch")
    overviews = [
        TrialOverview.model_validate_json(
            read_regular_artifact(
                config.package_dir / package.trial_overview_refs[i], 8192
            )
        )
        for i in package.selected_trial_ids
    ]
    if len({o.case_id for o in overviews}) != 1 or {
        o.repeat_index for o in overviews
    } != {0, 1, 2}:
        raise ValueError("requires three repeats of one development task")
    if any(
        getattr(manifest, key) != value
        for key, value in diagnostic_policy_digests(package).items()
    ):
        raise ValueError("diagnostic prompt/schema/export/selection policy drift")
    if (
        git_bytes(config.repository, "rev-parse", "HEAD").decode().strip()
        != manifest.initial_commit
    ):
        raise ValueError("executing commit drift")
    inspection = inspect_source(config.repository, manifest.initial_commit)
    if (
        inspection.changed_paths
        or inspection.source_digest != manifest.initial_source_digest
        or _digest(
            {n: v for n, v in inspection.files.items() if n not in EDITABLE_PATHS}
        )
        != manifest.controller_digest
        or _sha(
            read_regular_artifact(
                config.repository / "backend/uv.lock", 2 * 1024 * 1024
            )
        )
        != manifest.dependency_lock_digest
    ):
        raise ValueError("clean committed executing source required")
    output.mkdir(mode=0o700)
    frozen = output / "frozen"
    frozen.mkdir(mode=0o700)
    snapshot = stage_harness(
        config.repository, manifest.initial_commit, frozen / "runtime", EDITABLE_PATHS
    )
    if snapshot.source_digest != manifest.initial_source_digest:
        raise ValueError("executing source changed during preparation")
    # Copy exactly verified registry members, never private siblings or ledger files.
    destination = frozen / "package"
    destination.mkdir()
    for name in (*package.files, "package-manifest.json"):
        raw = read_regular_artifact(config.package_dir / name, 2 * 1024 * 1024)
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        _write(path, raw)
    if verify_trajectory_package(destination) != package:
        raise ValueError("package changed during preparation")
    _write(frozen / "manifest.json", manifest.model_dump_json().encode())
    assignments = tuple(
        AnalystAssignment(
            slot_id=uuid4(),
            trial_id=i,
            package_digest=package.bundle_digest,
            investigation_source_digest=package.source.source_digest,
        )
        for i in manifest.selected_trial_ids
    )
    _json(frozen / "assignments.json", assignments)
    # UUID snapshot/slot metadata is retained, while content identity excludes it.
    digest = _sha(
        canonical_json(
            {"manifest": manifest_digest(manifest), "export": snapshot.export_digest}
        )
    )
    _json(output / "frozen-registry.json", _registry(frozen))
    prepared = PreparedMiningDiagnostic(
        diagnostic_id=manifest.experiment_id,
        config_digest=digest,
        package_digest=package.bundle_digest,
        executing_source_digest=snapshot.source_digest,
        root=output,
    )
    _write(output / "prepared.json", prepared.model_dump_json().encode())
    return prepared


class MiningDiagnosticRunner:
    def __init__(
        self,
        store: MiningDiagnosticStore,
        root: Path,
        supervisor: WorkerSupervisor | None,
        *,
        docker: DockerPort | None = None,
    ) -> None:
        safe_path(root)
        self.store, self.root, self.supervisor = store, root, supervisor
        self.docker = docker

    def _frozen(
        self, id: UUID
    ) -> tuple[PreparedMiningDiagnostic, MiningDiagnosticManifest, HarnessSnapshot]:
        prepared = PreparedMiningDiagnostic.model_validate_json(
            read_regular_artifact(self.root / "prepared.json", 65536)
        )
        manifest = MiningDiagnosticManifest.model_validate_json(
            read_regular_artifact(self.root / "frozen/manifest.json", 65536)
        )
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(self.root / "frozen/runtime/snapshot.json", 65536)
        )
        expected = json.loads(
            read_regular_artifact(self.root / "frozen-registry.json", 2 * 1024 * 1024)
        )
        if _registry(self.root / "frozen") != expected:
            raise ValueError("frozen input drift; new diagnostic required")
        package = verify_trajectory_package(self.root / "frozen/package")
        from crucible.improvement.source import verify_harness_export

        verify_harness_export(self.root / "frozen/runtime/harness", snapshot)
        if (
            prepared.diagnostic_id != id
            or manifest.experiment_id != id
            or prepared.root != self.root
            or prepared.config_digest
            != _sha(
                canonical_json(
                    {
                        "manifest": manifest_digest(manifest),
                        "export": snapshot.export_digest,
                    }
                )
            )
            or snapshot.source_digest != manifest.initial_source_digest
            or prepared.executing_source_digest != snapshot.source_digest
            or prepared.package_digest != package.bundle_digest
            or manifest.package_digest != package.bundle_digest
            or manifest.investigation_source != package.source
            or manifest.selected_trial_ids != package.selected_trial_ids
            or any(
                getattr(manifest, k) != v
                for k, v in diagnostic_policy_digests(package).items()
            )
        ):
            raise ValueError("frozen identity drift; new diagnostic required")
        return prepared, manifest, snapshot

    async def continue_run(self, diagnostic_id: UUID) -> MiningDiagnosticStatus:
        if self.supervisor is None:
            raise ValueError(
                "continuation requires an explicitly supplied execution runtime"
            )
        with self._lifecycle_lock():
            return await self._continue(diagnostic_id)

    @contextmanager
    def _lifecycle_lock(self) -> Iterator[None]:
        safe_path(self.root / "continue.lock")
        with (self.root / "continue.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ValueError(
                    "diagnostic lifecycle operation already active"
                ) from error
            yield

    async def initialize(self, id: UUID) -> MiningDiagnosticStatus:
        prepared, manifest, _ = self._frozen(id)
        assignments = TypeAdapter(tuple[AnalystAssignment, ...]).validate_json(
            read_regular_artifact(self.root / "frozen/assignments.json", 65536),
            strict=True,
        )
        try:
            run = await self.store.get(id)
        except NoResultFound:
            return await self.store.create_diagnostic(
                manifest, prepared.config_digest, prepared.package_digest, assignments
            )
        if (
            run.manifest != manifest
            or (await self.store.diagnostic_status(id)).config_digest
            != prepared.config_digest
        ):
            raise ValueError("ledger frozen identity drift")
        return await self.store.diagnostic_status(id)

    async def _continue(self, id: UUID) -> MiningDiagnosticStatus:
        prepared, manifest, snapshot = self._frozen(id)
        assignments = TypeAdapter(tuple[AnalystAssignment, ...]).validate_json(
            read_regular_artifact(self.root / "frozen/assignments.json", 65536),
            strict=True,
        )
        await self.initialize(id)
        await self.validate(id)
        status = await self.store.diagnostic_status(id)
        if status.status not in ("ready", "running"):
            return status
        for slot in status.slots:
            self._frozen(id)
            current = await self.store.diagnostic_status(id)
            if current.status not in ("ready", "running"):
                return current
            slot = current.slots[slot.ordinal]
            if slot.status == "complete":
                await self._accepted(slot)
                continue
            authorization = (
                await self.store.retry_authorization(slot.slot_id)
                if slot.attempts
                else None
            )
            if slot.attempts and authorization is None:
                await self.store.block_diagnostic(id, "retry_authorization_required")
                return await self.store.diagnostic_status(id)
            raw, attempt_status = await self._execute_slot(
                slot, manifest, snapshot, assignments, authorization
            )
            if attempt_status != "complete":
                return await self.store.diagnostic_status(id)
        raw = await self._accepted((await self.store.diagnostic_status(id)).slots[3])
        report = SynthesisReport.model_validate_json(raw)
        _json(self.root / "synthesis-report.json", report)
        _json(self.root / "mining-result.json", report.result)
        # Durable bytes precede the terminal ledger transition.
        output = read_regular_artifact(self.root / "synthesis-report.json", 1_048_576)
        result = read_regular_artifact(self.root / "mining-result.json", 1_048_576)
        _json(
            self.root / "final-manifest.json",
            {"synthesis-report.json": _sha(output), "mining-result.json": _sha(result)},
        )
        await self.store.complete_diagnostic(id, "synthesis-report.json", _sha(output))
        return await self.store.diagnostic_status(id)

    def _attempt_artifacts(self, row: RowMapping) -> tuple[RoleJob, Path]:
        # Store rows are read by callers; the job binds every private path.
        job = RoleJob.model_validate_json(row["job_json"])
        directory = self.root / "attempts" / str(job.job_id)
        if (
            RoleJob.model_validate_json(
                read_regular_artifact(directory / "job.json", 65536)
            )
            != job
        ):
            raise ValueError("recorded job drift")
        workspace = directory / "workspace"
        if _sha(
            read_regular_artifact(workspace / "input.json", 1_048_576)
        ) != job.input_digest or _registry(workspace) != json.loads(
            read_regular_artifact(
                directory / "workspace-registry.json", 2 * 1024 * 1024
            )
        ):
            raise ValueError("recorded workspace drift")
        validation = json.loads(row["validation_json"] or "{}")
        hashes = validation.get("artifact_hashes") or {}
        for name, sha in hashes.items():
            if name not in {
                "observation.json",
                "assistant.txt",
                "trace.json",
                "inspection-receipts.json",
                "outcome.json",
            }:
                raise ValueError("unknown recorded artifact")
            if _sha(read_regular_artifact(directory / name, 1_048_576)) != sha:
                raise ValueError("recorded artifact drift")
        return job, directory

    async def _accepted(self, slot: MiningSlotStatus) -> bytes:
        if not slot.attempts or slot.attempts[-1].status != "complete":
            raise ValueError("missing completed report checkpoint")
        attempt = await self.store.attempt_record(slot.attempts[-1].attempt_id)
        job, directory = self._attempt_artifacts(attempt)
        validation = json.loads(attempt["validation_json"] or "{}")
        if (
            not validation.get("normal_completion")
            or not validation.get("cleanup_confirmed")
            or attempt["tool_call_count"] is None
            or set(validation.get("artifact_hashes") or {})
            != {
                "observation.json",
                "assistant.txt",
                "trace.json",
                "inspection-receipts.json",
                "outcome.json",
            }
        ):
            raise ValueError("unverified report checkpoint")
        outcome = json.loads(
            read_regular_artifact(directory / "outcome.json", 1_048_576)
        )
        if (
            outcome["termination"] != "completed"
            or outcome["failure"]
            or outcome["cleanup_failure"]
            or not outcome["cleanup_confirmed"]
            or outcome["job_id"] != str(job.job_id)
        ):
            raise ValueError("report was not normally completed")
        observation = read_role_observation(
            read_regular_artifact(directory / "observation.json", 1_048_576), job
        )
        if (
            observation.termination != "completed"
            or observation.tool_call_count != attempt["tool_call_count"]
        ):
            raise ValueError("report observation drift")
        events = await self.store.events(job.experiment_id, "usage_reserved")
        ids = {
            str(e["reservation_id"])
            for e in events
            if e.get("job_id") == str(job.job_id)
        }
        if any(
            str(r["reservation_id"]) in ids
            and r["status"] not in ("settled", "conservative")
            for r in await self.store.usage_records(job.experiment_id)
        ):
            raise ValueError("report accounting unresolved")
        raw = read_regular_artifact(directory / "assistant.txt", 1_048_576)
        path = self.root / "reports" / (str(slot.slot_id) + ".json")
        if (
            _sha(raw) != attempt["output_sha"]
            or read_regular_artifact(path, 1_048_576) != raw
            or read_regular_artifact(path.with_suffix(".sha256"), 64).decode()
            != _sha(raw)
        ):
            raise ValueError("validated report drift")
        primary = (
            await self.store.attempt_record(UUID(attempt["correction_of"]))
            if attempt["correction_of"]
            else attempt
        )
        primary_job, primary_directory = self._attempt_artifacts(primary)
        primary_observation = read_role_observation(
            read_regular_artifact(primary_directory / "observation.json", 1_048_576),
            primary_job,
        )
        assert primary_observation.inspection_receipts_sha256 is not None
        receipts = read_inspection_receipts(
            read_regular_artifact(
                primary_directory / "inspection-receipts.json", 1_048_576
            ),
            job_id=primary_job.job_id,
            expected_sha256=primary_observation.inspection_receipts_sha256,
            kind=primary_job.job_kind,
        )
        workspace = primary_directory / "workspace"
        extra = {
            p.relative_to(workspace).as_posix(): read_regular_artifact(p, 1_048_576)
            for p in workspace.rglob("*")
            if p.is_file()
            and (
                p.relative_to(workspace).as_posix() == "input.json"
                or p.relative_to(workspace).parts[0] == "reports"
            )
        }
        package = load_inspection_package(
            self.root / "frozen/package",
            job_id=primary_job.job_id,
            workspace_files=extra,
        )
        if slot.kind == "trial_analysis":
            assert slot.trial_id is not None
            assignment = AnalystAssignment(
                slot_id=slot.slot_id,
                trial_id=slot.trial_id,
                package_digest=package.bundle_digest,
                investigation_source_digest=package.source.source_digest,
            )
            if not validate_analysis_report(
                raw, assignment, package, receipts
            ).investigation_complete:
                raise ValueError("incomplete checkpoint")
        else:
            validate_synthesis_report(raw, package, receipts)
        return raw

    async def validate(self, id: UUID) -> MiningDiagnosticStatus:
        prepared, manifest, _ = self._frozen(id)
        run = await self.store.get(id)
        status = await self.store.diagnostic_status(id)
        if run.manifest != manifest or status.config_digest != prepared.config_digest:
            raise ValueError("ledger frozen identity drift")
        for slot in status.slots:
            if slot.status == "complete":
                await self._accepted(slot)
        final_ref, final_sha = await self.store.final_checkpoint(id)
        if final_ref is not None:
            if (
                final_ref != "synthesis-report.json"
                or _sha(read_regular_artifact(self.root / final_ref, 1_048_576))
                != final_sha
            ):
                raise ValueError("final checkpoint drift")
            final = json.loads(
                read_regular_artifact(self.root / "final-manifest.json", 4096)
            )
            if set(final) != {"synthesis-report.json", "mining-result.json"}:
                raise ValueError("final output registry drift")
            for name, sha in final.items():
                if _sha(read_regular_artifact(self.root / name, 1_048_576)) != sha:
                    raise ValueError("final output drift")
            report = SynthesisReport.model_validate_json(
                await self._accepted(status.slots[3])
            )
            if read_regular_artifact(
                self.root / "synthesis-report.json", 1_048_576
            ) != canonical_json(report) or read_regular_artifact(
                self.root / "mining-result.json", 1_048_576
            ) != canonical_json(report.result):
                raise ValueError("final output differs from accepted checkpoint")
        return status

    async def _reconcile(self, id: UUID) -> bool:
        """Only exact ledger-owned Docker names and labels may be removed."""
        jobs = {
            str(a.job_id): RoleJob.model_validate_json(
                (await self.store.attempt_record(a.attempt_id))["job_json"]
            )
            for slot in (await self.store.diagnostic_status(id)).slots
            for a in slot.attempts
        }
        clean = True
        for effect in reversed(await self.store.effects(id)):
            if effect["status"] == "stopped":
                continue
            eid = UUID(str(effect["id"]))
            try:
                expected = json.loads(str(effect["ownership_labels_json"]))
                job = jobs.get(expected.get("crucible.improvement.job"))
                if (
                    job is None
                    or expected != labels(job)
                    or effect["kind"] not in ("network", "relay", "worker")
                ):
                    raise ValueError("unrecognized diagnostic effect ownership")
                kind = str(effect["kind"])
                expected_name = "improvement-" + kind + "-" + str(job.job_id)
                if json.loads(str(effect["intent_json"])) != {"name": expected_name}:
                    raise ValueError("diagnostic effect name drift")
                docker = self.docker or DockerRuntime()
                await remove_owned_effect(docker, kind, expected_name, expected)
                await self.store.effect_state(eid, "stopped")
            except Exception:
                clean = False
                await self.store.effect_state(eid, "uncertain")
        return clean

    async def _recover_worker_artifacts(self, attempt_id: UUID) -> int | None:
        """Collect a stopped worker's durable outputs without accepting its report."""
        row = await self.store.attempt_record(attempt_id)
        job, directory = self._attempt_artifacts(row)
        root = self.root / "jobs" / str(job.job_id)
        outcome_raw = read_regular_artifact(root / "job-outcome.json", 1_048_576)
        outcome = TypeAdapter(JobOutcome).validate_json(outcome_raw, strict=True)
        if (
            outcome.job_id != job.job_id
            or outcome.experiment_id != job.experiment_id
            or outcome.attempt_id != job.role_id
            or outcome.harness_digest != job.harness_digest
            or outcome.repeat_index is not None
            or not outcome.cleanup_confirmed
            or outcome.cleanup_failure
        ):
            raise ValueError("unverified durable worker outcome")
        state = root / "state"
        observation = read_role_observation(
            read_regular_artifact(state / "observation.json", 1_048_576), job
        )
        names = (
            "observation.json",
            "assistant.txt",
            "trace.json",
            "inspection-receipts.json",
        )
        files = {name: read_regular_artifact(state / name, 1_048_576) for name in names}
        assert observation.inspection_receipts_sha256 is not None
        read_inspection_receipts(
            files["inspection-receipts.json"],
            job_id=job.job_id,
            expected_sha256=observation.inspection_receipts_sha256,
            kind=job.job_kind,
        )
        if row["output_sha"] and _sha(files["assistant.txt"]) != row["output_sha"]:
            raise ValueError("durable worker output drift")
        hashes = json.loads(row["validation_json"] or "{}").get("artifact_hashes") or {}
        for name, raw in {**files, "outcome.json": outcome_raw}.items():
            if name in hashes and _sha(raw) != hashes[name]:
                raise ValueError("durable worker artifact differs from checkpoint")
        for name, raw in files.items():
            _write(directory / name, raw)
        _write(directory / "outcome.json", outcome_raw)
        intent = json.loads(
            read_regular_artifact(directory / "artifact-reservation.json", 4096)
        )
        if intent.get("job_id") != str(job.job_id):
            raise ValueError("artifact reservation intent drift")
        reservation = next(
            (
                r
                for r in await self.store.attempt_usage(attempt_id)
                if r["reservation_id"] == intent.get("reservation_id")
            ),
            None,
        )
        if (
            reservation is None
            or reservation["kind"] != "worker"
            or UsageAmounts.model_validate_json(reservation["requested_json"])
            != UsageAmounts(output_bytes=4 * 1_048_576)
        ):
            raise ValueError("artifact reservation scope mismatch")
        if reservation["status"] == "reserved":
            await self.store.settle_usage(
                UUID(reservation["reservation_id"]),
                UsageAmounts(output_bytes=sum(map(len, files.values()))),
            )
        # An already uncertain exposure is retained; recovery never rewrites it.
        return observation.tool_call_count

    async def recover(self, id: UUID) -> MiningDiagnosticStatus:
        with self._lifecycle_lock():
            status = await self.validate(id)
            if status.status in ("complete", "closed"):
                return status
            stopped = await self.store.request_stop(id)
            owner = await self.store.claim_recovery(
                id,
                stopped.revision,
                await self.store.execution_owner(id),
                os.getpid(),
                None,
            )
            try:
                if self.supervisor is not None:
                    for slot in status.slots:
                        for attempt in slot.attempts:
                            if attempt.status != "complete":
                                try:
                                    await self.supervisor.stop(attempt.job_id)
                                except Exception:
                                    row = await self.store.attempt_record(
                                        attempt.attempt_id
                                    )
                                    await self.store.reconcile_attempt(
                                        attempt.attempt_id,
                                        row["tool_call_count"],
                                        cleanup_confirmed=False,
                                    )
                                    await self.store.block_diagnostic(
                                        id, "credential_cleanup_uncertain"
                                    )
                                    return await self.store.diagnostic_status(id)
                if not await self._reconcile(id):
                    await self.store.block_diagnostic(id, "cleanup_uncertain")
                    return await self.store.diagnostic_status(id)
                for slot in status.slots:
                    for attempt in slot.attempts:
                        if attempt.status == "complete":
                            continue
                        row = await self.store.attempt_record(attempt.attempt_id)
                        job, directory = self._attempt_artifacts(row)
                        count = row["tool_call_count"]
                        try:
                            count = await self._recover_worker_artifacts(
                                attempt.attempt_id
                            )
                        except (OSError, ValueError):
                            pass  # Missing proof retains exposure and unknown counts.
                        # A real observation can recover counts, never completion.
                        observation_path = directory / "observation.json"
                        if observation_path.exists():
                            count = read_role_observation(
                                read_regular_artifact(observation_path, 1_048_576), job
                            ).tool_call_count
                        await self.store.reconcile_attempt(attempt.attempt_id, count)
                try:
                    await self.store.recovery_ready(id, owner)
                except BudgetDenied:
                    await self.store.block_diagnostic(id, "accounting_uncertain")
                return await self.store.diagnostic_status(id)
            finally:
                if await self.store.execution_owner(id) == str(owner):
                    await self.store.release_execution(
                        id, owner, (await self.store.get(id)).stage
                    )

    async def authorize_retry(
        self, id: UUID, slot_id: UUID, failed_attempt_id: UUID
    ) -> UUID:
        with self._lifecycle_lock():
            await self.validate(id)
            remaining = UsageAmounts.model_validate(
                (await self.store.usage_summary(id))["remaining"]
            )
            if (
                min(
                    remaining.tokens,
                    remaining.requests,
                    remaining.active_seconds,
                    remaining.worker_wall_seconds,
                    remaining.output_bytes,
                )
                <= 0
            ):
                raise BudgetDenied("retry cannot fit remaining diagnostic allowance")
            return await self.store.authorize_retry(id, slot_id, failed_attempt_id)

    async def stop(self, id: UUID) -> MiningDiagnosticStatus:
        # A drifted investigation must still be stoppable; ownership comes only
        # from the private ledger, never from model output or changed inputs.
        prepared = PreparedMiningDiagnostic.model_validate_json(
            read_regular_artifact(self.root / "prepared.json", 65536)
        )
        if prepared.diagnostic_id != id or prepared.root != self.root:
            raise ValueError("diagnostic stop identity mismatch")
        status = await self.store.diagnostic_status(id)
        if status.status in ("complete", "closed"):
            return status
        await self.store.request_diagnostic_stop(id)
        if self.supervisor is not None:
            for slot in status.slots:
                for attempt in slot.attempts:
                    if attempt.status == "running":
                        await self.supervisor.stop(attempt.job_id)
        try:
            with self._lifecycle_lock():
                if not await self._reconcile(id):
                    await self.store.block_diagnostic(id, "cleanup_uncertain")
        except ValueError:
            pass  # Active continuation observes the durable stop and performs cleanup.
        return await self.store.diagnostic_status(id)

    async def close(self, id: UUID) -> MiningDiagnosticStatus:
        with self._lifecycle_lock():
            await self.validate(id)
            await self.store.close_diagnostic(id)
            return await self.store.diagnostic_status(id)

    async def _workspace(
        self,
        directory: Path,
        slot: MiningSlotStatus,
        manifest: MiningDiagnosticManifest,
        assignments: tuple[AnalystAssignment, ...],
        correction: dict[str, object] | None,
        analysis_slots: tuple[MiningSlotStatus, ...],
    ) -> tuple[Path, bytes]:
        workspace = directory / "workspace"
        workspace.mkdir()
        package = verify_trajectory_package(self.root / "frozen/package")
        schema = (
            TrialAnalysisReport if slot.kind == "trial_analysis" else SynthesisReport
        )
        if correction is not None:
            payload: dict[str, object] = {
                "output_schema": schema.model_json_schema(),
                **correction,
            }
        else:
            if slot.kind == "trial_analysis":
                assignment = assignments[slot.ordinal]
                payload = {
                    "assignment": assignment.model_dump(mode="json"),
                    "overview": "trial/overview.json",
                    "index": "trial/trajectory.jsonl",
                    "details_manifest": "trial/details-manifest.json",
                }
            else:
                refs = []
                reports = []
                for analyst in analysis_slots:
                    assert analyst.trial_id is not None
                    raw = await self._accepted(analyst)
                    name = "reports/" + str(analyst.trial_id) + ".json"
                    path = workspace / name
                    path.parent.mkdir(exist_ok=True)
                    _write(path, raw)
                    refs.append(
                        ValidatedAnalysisRef(
                            trial_id=analyst.trial_id, path=name, sha256=_sha(raw)
                        )
                    )
                    reports.append(json.loads(raw))
                assignment_synthesis = SynthesisAssignment(
                    slot_id=slot.slot_id,
                    selected_trial_ids=manifest.selected_trial_ids,
                    package_digest=package.bundle_digest,
                    investigation_source_digest=package.source.source_digest,
                    reports=(refs[0], refs[1], refs[2]),
                )
                payload = {
                    "assignment": assignment_synthesis.model_dump(mode="json"),
                    "reports": reports,
                    "overviews": list(package.trial_overview_refs.values()),
                }
            payload["output_schema"] = schema.model_json_schema()
            # Hashes and canonical lookup keys are metadata, never raw trace text.
            # Without these the read-only model cannot name precise file digests.
            payload["file_hashes"] = {
                (
                    "trial/" + name.removeprefix(f"trials/{slot.trial_id}/")
                    if name.startswith(f"trials/{slot.trial_id}/")
                    else name
                ): sha
                for name, sha in package.files.items()
                if name.startswith("source/")
                or (
                    name.startswith("trials/")
                    and (
                        slot.kind == "mining_synthesis"
                        or name.startswith(f"trials/{slot.trial_id}/")
                    )
                )
            }
            locators = json.loads(
                read_regular_artifact(
                    self.root / "frozen/package" / package.record_locator_ref,
                    2 * 1024 * 1024,
                )
            )
            payload["record_registry"] = [
                entry
                for entry in locators
                if slot.kind == "mining_synthesis"
                or entry["trial_id"] == str(slot.trial_id)
            ]
            for name in package.files:
                if name.startswith("source/"):
                    target = name
                elif slot.kind == "mining_synthesis" and name.startswith("trials/"):
                    target = name
                elif name.startswith(f"trials/{slot.trial_id}/"):
                    target = "trial/" + name.removeprefix(f"trials/{slot.trial_id}/")
                else:
                    continue
                path = workspace / target
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(
                    read_regular_artifact(
                        self.root / "frozen/package" / name, 2 * 1024 * 1024
                    )
                )
        encoded = canonical_json(payload)
        # Correction includes the original output; synthesis includes compact reports.
        if len(encoded) > 1_048_576:
            raise ValueError("diagnostic input exceeds cap")
        _write(workspace / "input.json", encoded)
        git_bytes(workspace, "init", "-q")
        return workspace, encoded

    async def _execute_slot(
        self,
        slot: MiningSlotStatus,
        manifest: MiningDiagnosticManifest,
        snapshot: HarnessSnapshot,
        assignments: tuple[AnalystAssignment, ...],
        retry_authorization: UUID | None = None,
    ) -> tuple[bytes, SlotStatus]:
        analysis_slots = (
            await self.store.diagnostic_status(manifest.experiment_id)
        ).slots[:3]
        correction: dict[str, object] | None = None
        primary_job: RoleJob | None = None
        primary_receipts: tuple[InspectionReceipt, ...] = ()
        primary_extra: dict[str, bytes] = {}
        for opportunity in range(2):
            self._frozen(manifest.experiment_id)
            remaining = (await self.store.slot_usage(slot.slot_id))["remaining"]
            if (
                min(
                    remaining.tokens,
                    remaining.requests,
                    remaining.active_seconds,
                    remaining.worker_wall_seconds,
                )
                <= 0
            ):
                await self.store.block_diagnostic(
                    manifest.experiment_id, "budget_exhausted"
                )
                return b"", "budget_exhausted"
            used_tools = 0
            for previous_attempt in slot.attempts:
                used_tools += (
                    await self.store.attempt_record(previous_attempt.attempt_id)
                )["tool_call_count"] or 0
            limits = TrialLimits(
                max_model_tokens=remaining.tokens,
                max_steps=min(slot.limits.max_steps, remaining.requests),
                max_tool_calls=1
                if correction
                else slot.limits.max_tool_calls - used_tools,
                max_active_seconds=remaining.active_seconds,
                wall_timeout_seconds=remaining.worker_wall_seconds,
            )
            job_id = uuid4()
            directory = self.root / "attempts" / str(job_id)
            directory.mkdir(parents=True, mode=0o700)
            try:
                workspace, encoded = await self._workspace(
                    directory, slot, manifest, assignments, correction, analysis_slots
                )
            except ValueError:
                await self.store.block_diagnostic(
                    manifest.experiment_id, "input_validation_failed"
                )
                return b"", "infrastructure_error"
            registry = _registry(workspace)
            _json(directory / "workspace-registry.json", registry)
            kind: Literal["trial_analysis", "mining_synthesis", "format_correction"] = (
                "format_correction" if correction else slot.kind
            )
            job = RoleJob(
                schema_version=2,
                protocol_version=2,
                job_id=job_id,
                role_id=uuid4(),
                round_id=slot.slot_id,
                experiment_id=manifest.experiment_id,
                job_kind=kind,
                harness_digest=snapshot.source_digest,
                export_digest=snapshot.export_digest,
                model=manifest.model,
                limits=limits,
                input_digest=_sha(encoded),
                workspace="/workspace",
                command_image=manifest.worker_image,
                command_platform=manifest.worker_platform,
                output_limit_bytes=1_048_576,
                wall_deadline_seconds=limits.wall_timeout_seconds,
                budget_slot_id=slot.slot_id,
                correction_of_job_id=primary_job.job_id
                if correction and primary_job
                else None,
                correction_target=slot.kind if correction else None,
            )
            _write(directory / "job.json", job.model_dump_json().encode())
            try:
                attempt = await self.store.register_attempt(
                    slot.slot_id,
                    job,
                    job.input_digest,
                    retry_authorization if opportunity == 0 else None,
                )
            except BudgetDenied:
                await self.store.block_diagnostic(
                    manifest.experiment_id, "budget_exhausted"
                )
                return b"", "budget_exhausted"
            if primary_job is None:
                primary_job = job
            try:
                # Docker stdout has its own supervisor reservation. Charge the
                # four registered worker files separately, including failed jobs.
                artifact_reservation = await self.store.reserve_usage(
                    manifest.experiment_id,
                    None,
                    UsageAmounts(output_bytes=4 * 1_048_576),
                    kind="worker",
                    job_id=job.job_id,
                )
            except BudgetDenied:
                await self.store.finish_attempt(
                    attempt,
                    "budget_exhausted",
                    tool_call_count=0,
                    normal_completion=False,
                    cleanup_confirmed=True,
                    error_code="artifact_quota_denied",
                )
                return b"", "budget_exhausted"
            _json(
                directory / "artifact-reservation.json",
                {"job_id": job.job_id, "reservation_id": artifact_reservation},
            )
            artifacts_known = False
            artifact_bytes = 0
            execution: IsolatedRoleExecution | None = None
            cleanup = False
            failure: BaseException | None = None
            assert self.supervisor is not None
            try:
                execution = await self.supervisor.execute(
                    job,
                    snapshot,
                    workspace,
                    harness_export=self.root / "frozen/runtime/harness",
                )
            except BaseException as error:
                failure = error
            finally:
                try:
                    await self.supervisor.stop(job.job_id)
                    cleanup = True
                except BaseException as error:
                    failure = failure or error
            status: SlotStatus = "infrastructure_error"
            normal = False
            count = None
            raw = b""
            receipts: tuple[InspectionReceipt, ...] = ()
            error_code: str | None = None
            try:
                if execution is not None:
                    if (
                        execution.workspace != workspace
                        or execution.state_root
                        != self.root / "jobs" / str(job.job_id) / "state"
                    ):
                        raise ValueError("execution artifact root mismatch")
                    if execution.outcome is not None:
                        _json(directory / "outcome.json", execution.outcome)
                    outcome = execution.outcome
                    if (
                        outcome is None
                        or outcome.job_id != job.job_id
                        or outcome.experiment_id != job.experiment_id
                        or outcome.attempt_id != job.role_id
                        or outcome.harness_digest != job.harness_digest
                        or outcome.cleanup_failure
                        or not execution.cleanup_confirmed
                        or not outcome.cleanup_confirmed
                        or set(outcome.unresolved_reservations) - {artifact_reservation}
                        or execution.commands
                    ):
                        raise ValueError("unsettled or failed execution")
                    if execution.observation is None:
                        # Only proven non-dispatch establishes zero tool usage
                        # when no worker observation exists.
                        if (
                            outcome.stage != "admission"
                            or outcome.failure is None
                            or outcome.failure.dispatch != "not_sent"
                            or outcome.resource_reservation_id is not None
                            or outcome.resource_usage != UsageAmounts()
                        ):
                            raise ValueError(
                                "missing observation with unknown execution"
                            )
                        count = 0
                        artifacts_known = True
                        status = (
                            "budget_exhausted"
                            if outcome.failure.category == "quota"
                            else "infrastructure_error"
                        )
                    else:
                        # Apply identical validation to real and scripted workers.
                        observation_raw = read_regular_artifact(
                            execution.state_root / "observation.json", 1_048_576
                        )
                        observation = read_role_observation(observation_raw, job)
                        if observation != execution.observation:
                            raise ValueError("observation envelope mismatch")
                        count = observation.tool_call_count
                        for name in (
                            "observation.json",
                            "assistant.txt",
                            "trace.json",
                            "inspection-receipts.json",
                        ):
                            _write(
                                directory / name,
                                read_regular_artifact(
                                    execution.state_root / name, 1_048_576
                                ),
                            )
                        artifacts_known = True
                        artifact_bytes = sum(
                            (directory / name).stat().st_size
                            for name in (
                                "observation.json",
                                "assistant.txt",
                                "trace.json",
                                "inspection-receipts.json",
                            )
                        )
                        raw = read_regular_artifact(
                            directory / "assistant.txt", 1_048_576
                        )
                        assert observation.inspection_receipts_sha256 is not None
                        receipts = read_inspection_receipts(
                            read_regular_artifact(
                                directory / "inspection-receipts.json", 1_048_576
                            ),
                            job_id=job.job_id,
                            expected_sha256=observation.inspection_receipts_sha256,
                            kind=job.job_kind,
                        )
                        if failure is not None:
                            raise ValueError("execution or cleanup failed")
                        if _registry(workspace) != registry:
                            raise ValueError("workspace drift")
                        self._frozen(manifest.experiment_id)
                        if (
                            observation.termination == "budget_exhausted"
                            or outcome.termination == "budget_exhausted"
                        ):
                            status = "budget_exhausted"
                        elif (
                            observation.termination == "cancelled"
                            or outcome.termination == "interrupted"
                        ):
                            status = "interrupted"
                        elif (
                            not outcome.failure
                            and observation.termination == "completed"
                            and outcome.termination == "completed"
                        ):
                            normal = True
                            if not correction:
                                primary_receipts = receipts
                            extra = {
                                p.relative_to(
                                    workspace
                                ).as_posix(): read_regular_artifact(p, 2 * 1024 * 1024)
                                for p in workspace.rglob("*")
                                if p.is_file()
                                and (
                                    p.name == "input.json"
                                    or p.relative_to(workspace).parts[0] == "reports"
                                )
                            }
                            if not correction:
                                primary_extra = extra
                            package = load_inspection_package(
                                self.root / "frozen/package",
                                job_id=primary_job.job_id,
                                workspace_files=primary_extra,
                            )
                            if slot.kind == "trial_analysis":
                                report = validate_analysis_report(
                                    raw,
                                    assignments[slot.ordinal],
                                    package,
                                    primary_receipts,
                                )
                                status = (
                                    "complete"
                                    if report.investigation_complete
                                    else "incomplete"
                                )
                            else:
                                validate_synthesis_report(
                                    raw, package, primary_receipts
                                )
                                status = "complete"
            except MiningValidationError as error:
                status, error_code = "invalid", error.code
            except (ValueError, OSError):
                status, error_code = (
                    "infrastructure_error",
                    "execution_validation_failed",
                )
            if failure is not None:
                normal = False
                status = (
                    "interrupted"
                    if isinstance(failure, asyncio.CancelledError)
                    else "infrastructure_error"
                )
            if artifacts_known and cleanup:
                await self.store.settle_usage(
                    artifact_reservation, UsageAmounts(output_bytes=artifact_bytes)
                )
            else:
                await self.store.mark_uncertain(
                    artifact_reservation, UsageAmounts(output_bytes=artifact_bytes)
                )
            if status == "complete":
                reports = self.root / "reports"
                reports.mkdir(exist_ok=True)
                accepted = reports / (str(slot.slot_id) + ".json")
                _write(accepted, raw)
                _write(accepted.with_suffix(".sha256"), _sha(raw).encode())
            await self.store.finish_attempt(
                attempt,
                status,
                tool_call_count=count,
                normal_completion=normal,
                cleanup_confirmed=cleanup
                and bool(execution and execution.cleanup_confirmed),
                output_ref=str((directory / "assistant.txt").relative_to(self.root))
                if raw
                else None,
                output_sha=_sha(raw) if raw else None,
                receipts_ref=str(
                    (directory / "inspection-receipts.json").relative_to(self.root)
                )
                if execution
                else None,
                outcome_ref=str((directory / "outcome.json").relative_to(self.root))
                if execution and execution.outcome
                else None,
                error_code=error_code,
                artifact_hashes={
                    name: _sha(read_regular_artifact(directory / name, 1_048_576))
                    for name in (
                        "observation.json",
                        "assistant.txt",
                        "trace.json",
                        "inspection-receipts.json",
                        "outcome.json",
                    )
                    if (directory / name).is_file()
                },
                final_disposition=SynthesisReport.model_validate_json(
                    raw
                ).result.disposition
                if status == "complete" and slot.kind == "mining_synthesis"
                else None,
            )
            if failure is not None and not isinstance(failure, Exception):
                raise failure
            if (
                status == "invalid"
                and opportunity == 0
                and normal
                and cleanup
                and not any(a.kind == "format_correction" for a in slot.attempts)
            ):
                correction = {
                    "original_output": raw.decode("utf-8", errors="replace"),
                    "validation_errors": [error_code or "invalid_report"],
                }
                continue
            return raw, status
        raise AssertionError("bounded correction loop exhausted")
