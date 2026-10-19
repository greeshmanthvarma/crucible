"""Authoritative whole-task composition. Worker claims never supply verdicts."""

import asyncio
import hashlib
import json
import math
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from crucible.improvement.selection import EvaluationSet
from uuid import UUID, uuid4, uuid5

from pydantic import TypeAdapter

from crucible.benchmarks.admission import validate_admission
from crucible.benchmarks.contracts import (
    BenchmarkCase,
    GradeResult,
    SolutionPatch,
    SolverResult,
    TrialIdentity,
    TrialResult,
)
from crucible.benchmarks.evidence import atomic_json
from crucible.benchmarks.manifests import (
    _git,
    case_digest,
    export_solver_case,
    load_case,
)
from crucible.benchmarks.patches import collect_solution_patch
from crucible.benchmarks.swebench import evaluation_identity
from crucible.improvement.checks import CHECK_IDS, CandidateCheckResult, suite_digest
from crucible.improvement.contracts import (
    ExperimentManifest,
    ExperimentPaths,
    HarnessSnapshot,
    UsageAmounts,
)
from crucible.improvement.fingerprints import trial_key
from crucible.improvement.manifest import CaseRegistry
from crucible.improvement.source import safe_path
from crucible.improvement.store import ExperimentStore
from crucible.improvement.worker_protocol import (
    IsolatedExecution,
    SolverCaseWire,
    WorkerJob,
)
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json, digest


class IsolatedPort(Protocol):
    async def execute(
        self,
        job: WorkerJob,
        snapshot: HarnessSnapshot,
        solver_export: Path,
        *,
        harness_export: Path,
    ) -> IsolatedExecution: ...
    async def stop(self, job_id: UUID) -> None: ...


class GraderPort(Protocol):
    async def grade(
        self,
        case: BenchmarkCase,
        patch: SolutionPatch,
        trial_id: UUID,
        output_dir: Path,
    ) -> GradeResult: ...


def selection_schedule(manifest: ExperimentManifest) -> tuple[tuple[str, int], ...]:
    return tuple(
        (case.case_id, index)
        for case in manifest.cases
        if case.split != "final-test"
        for index in range(3)
    )


def owned_artifact(root: Path, reference: str) -> Path:
    path = Path(reference)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError("invalid artifact reference")
    output = root / path
    safe_path(output)
    return output


async def reusable_trial(
    store: ExperimentStore, experiment_id: UUID, key: str, *, root: Path
) -> TrialResult | None:
    for row in await store.trials(experiment_id):
        if row["key"] != key or row["status"] != "complete":
            continue
        reference = json.loads(str(row["artifact_ref"]))
        path = owned_artifact(root, reference["path"])
        raw = read_regular_artifact(path, 2 * 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
            raise ValueError("cached evidence drift")
        envelope = json.loads(raw)
        for artifact, expected in envelope["artifacts"].items():
            content = read_regular_artifact(
                owned_artifact(path.parent, artifact), 8 * 1024 * 1024
            )
            if hashlib.sha256(content).hexdigest() != expected:
                raise ValueError("cached artifact drift")
        result = TypeAdapter(TrialResult).validate_json(
            canonical_json(envelope["result"]), strict=True
        )
        if (
            result.status != "complete"
            or result.grade is None
            or result.grade.verdict == "error"
        ):
            raise ValueError("invalid complete evidence")
        await store.record_event(
            experiment_id,
            "trial_reused",
            {"trial_id": str(result.identity.trial_id), "key": key},
        )
        return result
    return None


class IsolatedTrialRunner:
    def __init__(
        self,
        store: ExperimentStore,
        paths: ExperimentPaths,
        supervisor: IsolatedPort,
        grader: GraderPort,
        protected_suite_root: Path,
    ) -> None:
        self.store, self.paths, self.supervisor, self.grader = (
            store,
            paths,
            supervisor,
            grader,
        )
        self.protected_suite_root = protected_suite_root

    def resolve_case(self, manifest: ExperimentManifest, case_id: str) -> BenchmarkCase:
        ref = next((c for c in manifest.cases if c.case_id == case_id), None)
        if ref is None or ref.split == "final-test":
            raise ValueError("final-test scheduling is unavailable")
        registry = CaseRegistry.model_validate_json(
            read_regular_artifact(self.paths.registry_root / "registry.json", 1_048_576)
        )
        entry = registry.cases[case_id]
        if entry.retired or registry.pending_slots:
            raise ValueError("case registry not admitted")
        case = load_case(entry.case_manifest)
        if case_digest(case) != ref.case_digest or case.split != ref.split:
            raise ValueError("frozen case drift")
        admission = read_regular_artifact(entry.admission_record, 1_048_576)
        if hashlib.sha256(admission).hexdigest() != ref.admission_digest:
            raise ValueError("admission drift")
        validate_admission(entry.admission_record, case, manifest.grader)
        return case

    async def load_snapshot(
        self, experiment_id: UUID, snapshot_id: UUID
    ) -> tuple[HarnessSnapshot, Path]:
        row = await self.store.snapshot(experiment_id, snapshot_id)
        path = owned_artifact(self.paths.root, str(row["artifact_ref"]))
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(path, 65536)
        )
        if (
            str(snapshot.snapshot_id),
            snapshot.source_digest,
            snapshot.export_digest,
        ) != (str(row["id"]), row["source_digest"], row["export_digest"]):
            raise ValueError("snapshot registry drift")
        if (snapshot.parent_commit, snapshot.kind, snapshot.patch_digest) != (
            row["parent_commit"],
            row["kind"],
            row["patch_digest"],
        ) or row["status"] != "verified":
            raise ValueError("snapshot protected metadata drift")
        return snapshot, path.parent / "harness"

    async def require_candidate_checks(
        self,
        experiment_id: UUID,
        snapshot: HarnessSnapshot,
        manifest: ExperimentManifest,
    ) -> None:
        await verified_candidate_checks(
            self.store,
            self.paths,
            experiment_id,
            snapshot,
            manifest,
            self.protected_suite_root,
        )

    async def run(
        self, experiment_id: UUID, snapshot_id: UUID, case_id: str, repeat_index: int
    ) -> TrialResult:
        record = await self.store.get(experiment_id)
        if record.stop_requested or record.status not in ("ready", "running"):
            raise ValueError("experiment is not ready")
        manifest = record.manifest
        case = self.resolve_case(manifest, case_id)
        snapshot, harness = await self.load_snapshot(experiment_id, snapshot_id)
        if snapshot.kind == "baseline" and (
            snapshot.source_digest != manifest.initial_source_digest
            or snapshot.parent_commit != manifest.initial_commit
        ):
            raise ValueError("baseline identity drift")
        if snapshot.kind == "candidate":
            await self.require_candidate_checks(experiment_id, snapshot, manifest)
        ref = next(c for c in manifest.cases if c.case_id == case_id)
        key = trial_key(manifest, snapshot, ref, repeat_index)
        cached = await reusable_trial(
            self.store, experiment_id, key, root=self.paths.root
        )
        if cached:
            return cached
        start = await self.store.reserve_usage(
            experiment_id, None, UsageAmounts(trial_starts=1), kind="worker"
        )
        await self.store.settle_usage(start, UsageAmounts(trial_starts=1))
        trial = await self.store.create_trial(
            experiment_id, snapshot_id, case_id, repeat_index, key
        )
        root = self.paths.root / "trials" / str(trial)
        root.mkdir(parents=True, exist_ok=False, mode=0o700)
        identity = TrialIdentity(
            trial_id=trial,
            case_digest=ref.case_digest,
            harness_commit=snapshot.parent_commit,
            harness_tree_digest=snapshot.source_digest,
            model_config_digest=digest(manifest.model),
            limits_digest=digest(manifest.trial_limits),
            repeat_index=repeat_index,
        )
        atomic_json(
            root / "request.json",
            {"identity": identity, "key": key, "snapshot_id": snapshot_id},
        )
        job_id = uuid4()
        solver = None
        patch = None
        grade = None
        host_output = None
        host_output_settled = False
        try:
            host_output = await self.store.reserve_usage(
                experiment_id,
                trial,
                UsageAmounts(
                    output_bytes=8 * 1024 * 1024
                    + 3
                    * min(
                        manifest.command_policy.max_output_bytes_per_call,
                        8 * 1024 * 1024,
                    )
                    + manifest.grader.output_limit_bytes
                    + 1_048_576
                ),
                kind="worker",
            )
            await self.store.trial_state(trial, "starting")
            solver_case = await asyncio.to_thread(
                export_solver_case, case, root / "workspace"
            )
            base_tree = (
                (
                    await asyncio.to_thread(
                        _git, solver_case.workspace, "rev-parse", "HEAD^{tree}"
                    )
                )
                .decode()
                .strip()
            )
            base = root / "trusted-base"
            base.mkdir()
            await asyncio.to_thread(
                shutil.copytree, solver_case.workspace / ".git", base / ".git"
            )
            job = WorkerJob(
                schema_version=manifest.schema_version,
                job_kind="trial" if manifest.schema_version == 2 else None,
                job_id=job_id,
                experiment_id=experiment_id,
                trial_id=trial,
                repeat_index=repeat_index,
                harness_digest=snapshot.source_digest,
                export_digest=snapshot.export_digest,
                solver_case=SolverCaseWire(
                    case_id=case.case_id,
                    base_commit=case.base_commit,
                    base_tree=base_tree,
                    prompt=solver_case.prompt,
                    image=case.image,
                    platform=case.platform,
                    workspace="/workspace",
                ),
                model=manifest.model,
                limits=manifest.trial_limits,
                output_limit_bytes=min(
                    manifest.command_policy.max_output_bytes_per_call, 8 * 1024 * 1024
                ),
                wall_deadline_seconds=manifest.trial_limits.wall_timeout_seconds,
                protocol_version=manifest.worker_protocol_version or 1,
            )
            initial_files = [
                p
                for p in solver_case.workspace.rglob("*")
                if p.is_file()
                and ".git" not in p.relative_to(solver_case.workspace).parts
            ]
            baseline_total = sum(p.stat().st_size for p in initial_files)
            baseline_largest = max((p.stat().st_size for p in initial_files), default=0)
            snapshot_limits = (
                baseline_largest + 8 * 1024 * 1024,
                baseline_total + 16 * 1024 * 1024,
                len(initial_files) * 4 + 4096,
            )
            await self.store.trial_state(trial, "solving")
            execution = await self.supervisor.execute(
                job, snapshot, solver_case.workspace, harness_export=harness
            )
            if not execution.cleanup_confirmed:
                raise ValueError("owned worker/command cleanup uncertain")
            current = await self.store.get(experiment_id)
            if current.stop_requested or current.status not in ("ready", "running"):
                raise ValueError("controller stop or uncertain usage")
            observation = execution.observation
            if observation.termination == "cancelled":
                raise asyncio.CancelledError
            if observation.termination in ("provider_error", "infrastructure_error"):
                raise ValueError("worker infrastructure failure")
            for source, name in [
                (observation.trace_ref, "trace.json"),
                (observation.usage_ref, "worker-usage.json"),
            ]:
                data = read_regular_artifact(
                    owned_artifact(execution.state_root, source), job.output_limit_bytes
                )
                atomic_json(
                    root / name,
                    {"trust": "untrusted-worker-observation", "data": json.loads(data)},
                )
            atomic_json(
                root / "usage.json",
                {
                    "trust": "protected-host",
                    "usage": execution.usage,
                    "commands": execution.commands,
                },
            )
            solver = SolverResult(
                observation.termination,
                uuid5(job_id, "task"),
                uuid5(job_id, "run"),
                solver_case.workspace,
                root / "trace.json",
                root / "usage.json",
            )
            await self.store.trial_state(trial, "snapshotting")
            patch = await asyncio.to_thread(
                collect_solution_patch,
                solver_case.workspace,
                base_tree,
                root / "solution.diff",
                base_commit=case.base_commit,
                base_repository=base,
                snapshot_limits=snapshot_limits,
            )
            await self.store.trial_state(trial, "grading")
            effect = await self.store.begin_effect(
                experiment_id,
                trial,
                "grading",
                {
                    "output": str(root / "grade"),
                    "evaluation": evaluation_identity(
                        trial,
                        ref.case_digest,
                        patch.sha256,
                        manifest.grader.driver_digest,
                    ),
                },
                {
                    "crucible.benchmark.trial": str(trial),
                    "crucible.benchmark.evaluation": evaluation_identity(
                        trial,
                        ref.case_digest,
                        patch.sha256,
                        manifest.grader.driver_digest,
                    ),
                },
            )
            grade_usage = await self.store.reserve_usage(
                experiment_id,
                trial,
                UsageAmounts(active_seconds=manifest.grader.timeout_seconds),
                kind="worker",
            )
            grade_usage_settled = False
            grade_started = time.monotonic()
            grade_task = asyncio.create_task(
                self.grader.grade(case, patch, trial, root / "grade")
            )

            async def stop_requested() -> None:
                while not (await self.store.get(experiment_id)).stop_requested:
                    await asyncio.sleep(0.1)

            stop = asyncio.create_task(stop_requested())
            try:
                done, _ = await asyncio.wait(
                    (grade_task, stop), return_when=asyncio.FIRST_COMPLETED
                )
                if stop in done:
                    raise asyncio.CancelledError
                grade = await grade_task
                if grade.code != "grading_cleanup_failed":
                    await self.store.settle_usage(
                        grade_usage,
                        UsageAmounts(
                            active_seconds=int(time.monotonic() - grade_started + 0.999)
                        ),
                    )
                    grade_usage_settled = True
                await self.store.effect_state(
                    effect,
                    "uncertain"
                    if grade.code == "grading_cleanup_failed"
                    else "stopped",
                )
            finally:
                grade_task.cancel()
                stop.cancel()
                await asyncio.gather(grade_task, stop, return_exceptions=True)
                if not grade_usage_settled:
                    await self.store.mark_uncertain(grade_usage)
                if grade is None:
                    await self.store.effect_state(effect, "uncertain")
            if grade.verdict == "error":
                raise ValueError("grading infrastructure failure")
            result = TrialResult(identity, solver, patch, grade, "complete", None)
            artifacts = {}
            for path in root.rglob("*"):
                if path.is_file() and not any(
                    p in ("workspace", "trusted-base")
                    for p in path.relative_to(root).parts
                ):
                    data = read_regular_artifact(path, 8 * 1024 * 1024)
                    artifacts[path.relative_to(root).as_posix()] = hashlib.sha256(
                        data
                    ).hexdigest()
            atomic_json(
                root / "result.json", {"result": result, "artifacts": artifacts}
            )
            await self.store.settle_usage(
                host_output,
                UsageAmounts(
                    output_bytes=sum(
                        (root / relative).stat().st_size for relative in artifacts
                    )
                    + (root / "result.json").stat().st_size
                ),
            )
            host_output_settled = True
            await self.store.trial_state(
                trial,
                "complete",
                artifact=json.dumps(
                    {
                        "path": str(
                            (root / "result.json").relative_to(self.paths.root)
                        ),
                        "sha256": hashlib.sha256(
                            (root / "result.json").read_bytes()
                        ).hexdigest(),
                    }
                ),
                verdict=grade.verdict,
                termination=solver.termination,
            )
            return result
        except BaseException as failure:
            interrupted = isinstance(failure, asyncio.CancelledError)
            cleanup = True
            try:
                await self.supervisor.stop(job_id)
            except BaseException:
                cleanup = False
            code = (
                "cleanup_uncertain"
                if not cleanup
                else ("operator_interrupted" if interrupted else "isolated_trial_error")
            )
            await self.store.fail_run(
                experiment_id, code, interrupted=interrupted and cleanup
            )
            if host_output is not None and not host_output_settled:
                total = 0
                for artifact in root.rglob("*"):
                    if artifact.is_file() and not any(
                        p in ("workspace", "trusted-base")
                        for p in artifact.relative_to(root).parts
                    ):
                        total += artifact.stat().st_size
                await self.store.settle_usage(
                    host_output, UsageAmounts(output_bytes=total)
                )
            result = TrialResult(
                identity,
                solver,
                patch,
                grade,
                "interrupted" if interrupted else "error",
                code,
            )
            atomic_json(
                root / ("interrupted.json" if interrupted else "error.json"), result
            )
            await self.store.trial_state(
                trial, "interrupted" if interrupted else "error", error=code
            )
            if interrupted:
                raise
            if not isinstance(failure, Exception):
                raise
            return result

    async def run_selection_baseline(
        self, experiment_id: UUID, snapshot_id: UUID
    ) -> tuple[TrialResult, ...]:
        manifest = (await self.store.get(experiment_id)).manifest
        results = []
        for case, index in selection_schedule(manifest):
            result = await self.run(experiment_id, snapshot_id, case, index)
            results.append(result)
            if result.status != "complete":
                break
        return tuple(results)


async def verified_candidate_checks(
    store: ExperimentStore,
    paths: ExperimentPaths,
    experiment_id: UUID,
    snapshot: HarnessSnapshot,
    manifest: ExperimentManifest,
    protected_suite_root: Path,
) -> CandidateCheckResult:
    runtime = digest(
        [
            manifest.worker_image,
            manifest.worker_platform,
            manifest.dependency_lock_digest,
            manifest.controller_digest,
        ]
    )
    if suite_digest(protected_suite_root) != manifest.boundary_suite_digest:
        raise ValueError("protected suite drift")
    for event in reversed(await store.events(experiment_id, "candidate_checks")):
        path = owned_artifact(paths.root, str(event["path"]))
        raw = read_regular_artifact(path, 65536)
        if hashlib.sha256(raw).hexdigest() != event["sha256"]:
            raise ValueError("check evidence drift")
        result = TypeAdapter(CandidateCheckResult).validate_json(raw, strict=True)
        if (
            result.snapshot_digest,
            result.export_digest,
            result.suite_digest,
            result.runtime_digest,
        ) == (
            snapshot.source_digest,
            snapshot.export_digest,
            manifest.boundary_suite_digest,
            runtime,
        ):
            if result.verdict == "pass" and result.cleanup_confirmed:
                log = read_regular_artifact(path.parent / "checks.log.json", 1_048_576)
                if hashlib.sha256(log).hexdigest() != result.log_digest:
                    raise ValueError("check log integrity drift")
                rows = json.loads(log)
                if sorted(
                    r["test_id"] for r in rows if r["verdict"] == "pass"
                ) != sorted(CHECK_IDS):
                    raise ValueError("protected check collection incomplete")
                return result
            raise ValueError("candidate protected checks did not pass")
    raise ValueError("matching candidate checks required before paid trial")


async def build_evaluation_set(
    store: ExperimentStore,
    paths: ExperimentPaths,
    experiment_id: UUID,
    snapshot_id: UUID,
) -> "EvaluationSet":
    """Read recorded host results, preserving original IDs across accepted lineage."""
    from crucible.improvement.manifest import manifest_digest
    from crucible.improvement.selection import EvaluationSet, ScoredTrial

    run = await store.get(experiment_id)
    snapshot = await store.snapshot(experiment_id, snapshot_id)
    required = tuple(c for c in run.manifest.cases if c.split != "final-test")
    refs = {c.case_id: c for c in required}
    selected = {}
    for row in await store.trials(experiment_id):
        if row["snapshot"] == str(snapshot_id) and row["case_id"] in refs:
            key = str(row["case_id"]), int(str(row["repeat_index"]))
            if key in selected:
                raise ValueError(
                    "ambiguous trial generations require explicit retry policy"
                )
            selected[key] = row
    trials = []
    for (case_id, repeat), row in sorted(selected.items()):
        case = refs[case_id]
        snapshot_contract = HarnessSnapshot.model_validate_json(
            canonical_json(
                dict(
                    snapshot_id=snapshot_id,
                    parent_commit=snapshot["parent_commit"],
                    kind=snapshot["kind"],
                    source_digest=snapshot["source_digest"],
                    export_digest=snapshot["export_digest"],
                    patch_digest=snapshot["patch_digest"],
                )
            )
        )
        if row["key"] != trial_key(run.manifest, snapshot_contract, case, repeat):
            raise ValueError("trial fingerprint drift")
        if row["status"] != "complete":
            trials.append(
                ScoredTrial(
                    case_id=case_id,
                    split=case.split,
                    repeat_index=repeat,
                    original_trial_id=UUID(str(row["id"])),
                    verdict="interrupted"
                    if row["status"] == "interrupted"
                    else "error",
                    evidence_digest=digest([row["id"], row["status"]]),
                )
            )
            continue
        reference = json.loads(str(row["artifact_ref"]))
        path = owned_artifact(paths.root, reference["path"])
        raw = read_regular_artifact(path, 2 * 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
            raise ValueError("evaluation result digest drift")
        envelope = json.loads(raw)
        for name, sha in envelope["artifacts"].items():
            artifact = read_regular_artifact(
                owned_artifact(path.parent, name), 8 * 1024 * 1024
            )
            if hashlib.sha256(artifact).hexdigest() != sha:
                raise ValueError("evaluation artifact digest drift")
        result = TypeAdapter(TrialResult).validate_json(
            canonical_json(envelope["result"]), strict=True
        )
        identity = result.identity
        if (
            (
                str(identity.trial_id),
                identity.case_digest,
                identity.harness_commit,
                identity.harness_tree_digest,
                identity.model_config_digest,
                identity.limits_digest,
                identity.repeat_index,
            )
            != (
                row["id"],
                case.case_digest,
                snapshot["parent_commit"],
                snapshot["source_digest"],
                digest(run.manifest.model),
                digest(run.manifest.trial_limits),
                repeat,
            )
            or result.status != "complete"
            or result.grade is None
            or result.patch is None
            or result.solver is None
            or result.grade.verdict not in ("pass", "fail")
            or result.grade.verdict != row["grade_verdict"]
            or result.grade.driver_commit != run.manifest.grader.driver_commit
            or result.grade.report_path is None
            or result.grade.patch_sha256 != result.patch.sha256
            or envelope["artifacts"].get("solution.diff") != result.patch.sha256
            or result.grade.resolved != (result.grade.verdict == "pass")
            or not math.isfinite(result.grade.elapsed_seconds)
            or result.grade.elapsed_seconds < 0
            or result.solver.termination != row["solver_termination"]
            or result.solver.termination
            not in ("completed", "budget_exhausted", "behavior_error")
        ):
            raise ValueError("incomparable or incomplete recorded trial")
        if result.grade.evaluation_id != evaluation_identity(
            identity.trial_id,
            case.case_digest,
            result.patch.sha256,
            run.manifest.grader.driver_digest,
        ):
            raise ValueError("grade evaluation identity drift")
        assert result.grade.report_path is not None
        for protected_path in (result.grade.report_path, result.grade.log_path):
            try:
                relative = protected_path.relative_to(path.parent).as_posix()
            except ValueError as error:
                raise ValueError("grade evidence outside original trial") from error
            if relative not in envelope["artifacts"]:
                raise ValueError("missing protected grade evidence")
        trials.append(
            ScoredTrial(
                case_id=case_id,
                split=case.split,
                repeat_index=repeat,
                original_trial_id=identity.trial_id,
                verdict=result.grade.verdict,
                evidence_digest=reference["sha256"],
            )
        )
    return EvaluationSet(
        source_digest=str(snapshot["source_digest"]),
        export_digest=str(snapshot["export_digest"]),
        config_policy_digest=manifest_digest(run.manifest),
        required_cases=required,
        trials=tuple(trials),
    )
