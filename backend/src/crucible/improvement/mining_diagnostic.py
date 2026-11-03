"""Offline frozen inputs and sequential, read-only diagnostic orchestration."""

# Host-only coordinator; none of this module is exported to a worker.
import asyncio
import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from pydantic import TypeAdapter
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
    RoleJob,
    read_role_observation,
)
from crucible.improvement.worker_supervisor import (
    WorkerSupervisor,
    read_regular_artifact,
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
    if len({(o.case_id, o.task_id) for o in overviews}) != 1 or {
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
    ) -> None:
        safe_path(root)
        self.store, self.root, self.supervisor = store, root, supervisor

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
        safe_path(self.root / "continue.lock")
        # Independent invocations cannot both construct or dispatch a next intent.
        with (self.root / "continue.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ValueError("diagnostic continuation already active") from error
            return await self._continue(diagnostic_id)

    async def _continue(self, id: UUID) -> MiningDiagnosticStatus:
        prepared, manifest, snapshot = self._frozen(id)
        assignments = TypeAdapter(tuple[AnalystAssignment, ...]).validate_json(
            read_regular_artifact(self.root / "frozen/assignments.json", 65536),
            strict=True,
        )
        try:
            run = await self.store.get(id)
        except NoResultFound:
            await self.store.create_diagnostic(
                manifest, prepared.config_digest, prepared.package_digest, assignments
            )
        else:
            if (
                run.manifest != manifest
                or (await self.store.diagnostic_status(id)).config_digest
                != prepared.config_digest
            ):
                raise ValueError("ledger frozen identity drift")
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
                # Task 5 adds execution-free recovery. Never replay existing work.
                await self._accepted(slot)
                continue
            if slot.attempts:
                await self.store.block_diagnostic(id, "recovery_required")
                return await self.store.diagnostic_status(id)
            raw, attempt_status = await self._execute_slot(
                slot, manifest, snapshot, assignments
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

    async def _accepted(self, slot: MiningSlotStatus) -> bytes:
        path = self.root / "reports" / (str(slot.slot_id) + ".json")
        raw = read_regular_artifact(path, 1_048_576)
        digest = read_regular_artifact(path.with_suffix(".sha256"), 64).decode()
        if not slot.attempts or slot.attempts[-1].status != "complete":
            raise ValueError("missing completed report checkpoint")
        attempt = await self.store.attempt_record(slot.attempts[-1].attempt_id)
        if _sha(raw) != attempt["output_sha"] or _sha(raw) != digest:
            raise ValueError("validated report drift")
        return raw

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
                assert primary_job is not None
                return b"", "budget_exhausted"
            limits = TrialLimits(
                max_model_tokens=remaining.tokens,
                max_steps=min(slot.limits.max_steps, remaining.requests),
                max_tool_calls=1 if correction else slot.limits.max_tool_calls,
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
                    slot.slot_id, job, job.input_digest
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
                final_disposition=SynthesisReport.model_validate_json(
                    raw
                ).result.disposition
                if status == "complete" and slot.kind == "mining_synthesis"
                else None,
            )
            if failure is not None and not isinstance(failure, Exception):
                raise failure
            if status == "invalid" and opportunity == 0 and normal and cleanup:
                correction = {
                    "original_output": raw.decode("utf-8", errors="replace"),
                    "validation_errors": [error_code or "invalid_report"],
                }
                continue
            return raw, status
        raise AssertionError("bounded correction loop exhausted")
