"""Explicit one-shot calibration; never creates a measured experiment."""

import asyncio
import hashlib
import json
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4, uuid5

from crucible.benchmarks.admission import validate_admission
from crucible.benchmarks.cli import _operator_line, validate_controller_layout
from crucible.benchmarks.contracts import (
    BenchmarkCase,
    SolverResult,
    TrialIdentity,
    TrialResult,
)
from crucible.benchmarks.evidence import TrialEvidence, atomic_json
from crucible.benchmarks.manifests import (
    _git,
    case_digest,
    export_solver_case,
    load_case,
)
from crucible.benchmarks.patches import collect_solution_patch
from crucible.benchmarks.swebench import SwebenchGrader
from crucible.domain.clock import SystemClock
from crucible.domain.commands import CommandSpec
from crucible.improvement.contracts import (
    EDITABLE_PATHS,
    CalibrationManifest,
    Contract,
    UsageAmounts,
)
from crucible.improvement.runtime import execution_runtime
from crucible.improvement.source import (
    _digest,
    inspect_source,
    safe_path,
    stage_harness,
)
from crucible.improvement.store import CalibrationStore
from crucible.improvement.worker_protocol import (
    SolverCaseWire,
    WorkerJob,
    WorkerRequest,
)
from crucible.improvement.worker_supervisor import DockerRuntime, read_regular_artifact
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.identity import digest


class IsolatedCalibrationConfig(Contract):
    manifest: CalibrationManifest
    repository: Path
    data_dir: Path


def grading_allowed(termination: str, cleanup_confirmed: bool) -> bool:
    return cleanup_confirmed and termination in ("completed", "budget_exhausted")


def validate_calibration_inputs(
    manifest: CalibrationManifest, repository: Path, output: Path, data_dir: Path
) -> BenchmarkCase:
    for path in (
        repository,
        output,
        data_dir,
        manifest.case_manifest,
        manifest.admission_record,
    ):
        safe_path(path)
    case = load_case(manifest.case_manifest)
    if case.split != "calibration" or case_digest(case) != manifest.case_digest:
        raise ValueError("only the exact retired calibration case is allowed")
    if (
        hashlib.sha256(
            read_regular_artifact(manifest.admission_record, 1048576)
        ).hexdigest()
        != manifest.admission_digest
    ):
        raise ValueError("calibration admission drift")
    validate_admission(manifest.admission_record, case, manifest.grader)
    validate_controller_layout(
        output,
        case,
        manifest.grader,
        protected=(manifest.case_manifest, manifest.admission_record),
        write_roots=(data_dir,),
    )
    if repository.resolve() != Path(__file__).parents[4].resolve():
        raise ValueError(
            "calibration controller must run from the requested repository"
        )
    if (
        _git(repository, "status", "--porcelain")
        or _git(repository, "rev-parse", "HEAD").decode().strip()
        != manifest.initial_commit
    ):
        raise ValueError("calibration requires the exact clean baseline")
    source = inspect_source(repository, manifest.initial_commit)
    if (
        source.source_digest != manifest.initial_source_digest
        or _digest(
            {
                key: value
                for key, value in source.files.items()
                if key not in EDITABLE_PATHS
            }
        )
        != manifest.controller_digest
    ):
        raise ValueError("calibration source/controller drift")
    if (
        hashlib.sha256((repository / "backend/uv.lock").read_bytes()).hexdigest()
        != manifest.dependency_lock_digest
    ):
        raise ValueError("calibration dependency drift")
    return case


async def interactive_approval(job: WorkerRequest, spec: CommandSpec) -> bool:
    print(
        json.dumps(
            {"job_id": str(job.job_id), "command": json.loads(spec.canonical_json())}
        ),
        flush=True,
    )
    print("Approve this exact command? Type approve or deny: ", end="", flush=True)
    try:
        return (await _operator_line()).strip().lower() == "approve"
    except EOFError:
        return False


async def run_isolated_calibration(config: Path, output: Path) -> TrialResult:
    settings = IsolatedCalibrationConfig.model_validate_json(
        read_regular_artifact(config, 1048576)
    )
    manifest = settings.manifest
    case = await asyncio.to_thread(
        validate_calibration_inputs,
        manifest,
        settings.repository,
        output,
        settings.data_dir,
    )
    identity = TrialIdentity(
        trial_id=manifest.trial_id,
        case_digest=manifest.case_digest,
        harness_commit=manifest.initial_commit,
        harness_tree_digest=manifest.initial_source_digest,
        model_config_digest=digest(manifest.model.model_dump()),
        limits_digest=digest(manifest.trial_limits),
        repeat_index=0,
    )
    evidence = TrialEvidence(
        output, {"operation": "isolated-trial", **settings.model_dump(mode="json")}
    )
    store: CalibrationStore | None = None
    solver = None
    patch = None
    job = None
    try:
        # Exclusive local identity registry survives all later failures and outputs.
        registry = settings.data_dir / "identities"
        registry.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (registry / f"{manifest.trial_id}.json").open("x") as handle:
            handle.write(identity.model_dump_json())
            handle.flush()
            os.fsync(handle.fileno())
        runtime = settings.data_dir / str(manifest.experiment_id)
        runtime.mkdir(exist_ok=False, mode=0o700)
        store = await CalibrationStore.open(runtime / "calibration.sqlite3")
        await store.create(manifest, str(settings.repository), str(manifest.trial_id))
        # stage_harness requires its staging root outside the source checkout.
        with TemporaryDirectory(
            prefix="crucible-calibration-", dir="/private/tmp"
        ) as temporary:
            staged = Path(temporary) / "snapshot"
            snapshot = await asyncio.to_thread(
                stage_harness,
                settings.repository,
                manifest.initial_commit,
                staged,
                EDITABLE_PATHS,
            )
            shutil.copytree(staged, output / "snapshot")
        if (
            snapshot.source_digest != manifest.initial_source_digest
            or snapshot.kind != "baseline"
        ):
            raise ValueError("calibration source changed while preparing")
        await store.register_snapshot(
            manifest.experiment_id, snapshot, "snapshot/snapshot.json"
        )
        solver_case = await asyncio.to_thread(
            export_solver_case, case, output / "solver"
        )
        tree = _git(solver_case.workspace, "rev-parse", "HEAD^{tree}").decode().strip()
        await store.admit_trial(
            manifest.experiment_id, snapshot.snapshot_id, case.case_id
        )
        start = await store.reserve_usage(
            manifest.experiment_id,
            manifest.trial_id,
            UsageAmounts(trial_starts=1),
            kind="worker",
        )
        await store.settle_usage(start, UsageAmounts(trial_starts=1))
        job = WorkerJob(
            schema_version=2,
            protocol_version=2,
            job_kind="trial",
            job_id=uuid4(),
            experiment_id=manifest.experiment_id,
            trial_id=manifest.trial_id,
            repeat_index=0,
            harness_digest=snapshot.source_digest,
            export_digest=snapshot.export_digest,
            solver_case=SolverCaseWire(
                case_id=case.case_id,
                base_commit=case.base_commit,
                base_tree=tree,
                prompt=solver_case.prompt,
                image=case.image,
                platform=case.platform,
                workspace="/workspace",
            ),
            model=manifest.model,
            limits=manifest.trial_limits,
            output_limit_bytes=min(
                manifest.command_policy.max_output_bytes_per_call, 8388608
            ),
            wall_deadline_seconds=manifest.trial_limits.wall_timeout_seconds,
        )
        gateway = ScopedModelGateway(
            LiteLLMResponsesGateway(disable_retries=True),
            clock=SystemClock(),
            endpoint="http://broker/internal/improvement/model",
        )
        evidence.stage("solving")
        await store.trial_state(manifest.trial_id, "solving")
        async with execution_runtime(
            store, runtime, manifest, DockerRuntime(), gateway, interactive_approval
        ) as supervisor:
            try:
                execution = await supervisor.execute(
                    job,
                    snapshot,
                    solver_case.workspace,
                    harness_export=output / "snapshot/harness",
                )
            finally:
                await supervisor.stop(job.job_id)
        atomic_json(output / "execution.json", execution)
        if execution.observation is None:
            raise ValueError("calibration worker execution failed")
        for reference, destination in (
            (execution.observation.trace_ref, "trace.json"),
            (execution.observation.usage_ref, "usage.json"),
        ):
            (output / destination).write_bytes(
                read_regular_artifact(
                    execution.state_root / reference, job.output_limit_bytes
                )
            )
        solver = SolverResult(
            execution.observation.termination,
            uuid5(job.job_id, "task"),
            uuid5(job.job_id, "run"),
            solver_case.workspace,
            output / "trace.json",
            output / "usage.json",
        )
        if not grading_allowed(solver.termination, execution.cleanup_confirmed):
            raise RuntimeError("calibration solver did not yield gradable evidence")
        evidence.stage("snapshotting")
        patch = await asyncio.to_thread(
            collect_solution_patch,
            solver_case.workspace,
            tree,
            output / "solution.diff",
            base_commit=case.base_commit,
        )
        evidence.stage("grading")
        grade = await SwebenchGrader(manifest.grader).grade(
            case, patch, manifest.trial_id, output / "grading"
        )
        result = TrialResult(
            identity,
            solver,
            patch,
            grade,
            "error" if grade.verdict == "error" else "complete",
            grade.code if grade.verdict == "error" else None,
        )
        atomic_json(output / "authoritative-result.json", result)
        await store.trial_state(
            manifest.trial_id,
            result.status,
            artifact=json.dumps(
                {
                    "path": str(output / "authoritative-result.json"),
                    "sha256": hashlib.sha256(
                        (output / "authoritative-result.json").read_bytes()
                    ).hexdigest(),
                }
            ),
            verdict=grade.verdict,
            termination=solver.termination,
            error=result.error_code,
        )
        evidence.finish(result, result.status)
        return result
    except BaseException as error:
        interrupted = isinstance(error, asyncio.CancelledError)
        result = TrialResult(
            identity,
            solver,
            patch,
            None,
            "interrupted" if interrupted else "error",
            type(error).__name__,
        )
        evidence.finish(result, result.status)
        if store:
            await store.fail_run(
                manifest.experiment_id, type(error).__name__, interrupted=interrupted
            )
        raise
    finally:
        if store:
            await store.close()
