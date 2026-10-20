"""Bounded data contracts for advisory development analysis and proposals."""

import hashlib
import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Self
from uuid import uuid4

if TYPE_CHECKING:
    from crucible.improvement.contracts import ExperimentManifest, HarnessSnapshot
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.store import ExperimentStore, RoundRecord
    from crucible.improvement.worker_protocol import RoleJob
    from crucible.improvement.worker_supervisor import WorkerSupervisor
    from crucible.improvement.worktree import OwnedWorktree
from uuid import UUID

from pydantic import Field, model_validator

from crucible.benchmarks.contracts import Sha256
from crucible.improvement.contracts import EDITABLE_PATHS, Contract

Identifier = Annotated[
    str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", max_length=100)
]
Summary = Annotated[str, Field(max_length=4000)]
Risk = Annotated[str, Field(max_length=1000)]
RoleKind = Literal["mining", "format_correction", "proposal"]


class EvidenceRef(Contract):
    trial_id: UUID
    artifact: Literal["trace", "grade_feedback", "solution_diff"]
    sha256: Sha256


class DevelopmentView(Contract):
    view_id: UUID
    baseline_snapshot_id: UUID
    index_digest: Sha256
    evidence_refs: Annotated[tuple[EvidenceRef, ...], Field(max_length=36864)]
    source_digest: Sha256
    prior_decisions_digest: Sha256


class Weakness(Contract):
    id: Identifier
    observed_behavior: Summary
    hypothesized_mechanism: Summary
    evidence: Annotated[tuple[EvidenceRef, ...], Field(min_length=1, max_length=32)]
    editable_paths: Annotated[tuple[str, ...], Field(min_length=1, max_length=6)]
    confidence: Literal["low", "medium", "high"]
    risks: Annotated[tuple[Risk, ...], Field(max_length=8)]
    limitations: Annotated[str, Field(max_length=2000)]

    @model_validator(mode="after")
    def scoped(self) -> Self:
        if not set(self.editable_paths) <= set(EDITABLE_PATHS):
            raise ValueError("weakness outside editable policy")
        return self


class MiningResult(Contract):
    schema_version: Literal[1]
    disposition: Literal[
        "actionable", "no_actionable_weakness", "insufficient_evidence", "out_of_scope"
    ]
    weaknesses: Annotated[tuple[Weakness, ...], Field(max_length=5)]
    summary: Summary

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.disposition == "actionable") != bool(self.weaknesses):
            raise ValueError("disposition and weaknesses disagree")
        if len({w.id for w in self.weaknesses}) != len(self.weaknesses):
            raise ValueError("duplicate weakness identifier")
        return self

    def validate_evidence(self, view: DevelopmentView) -> None:
        for weakness in self.weaknesses:
            if any(ref not in view.evidence_refs for ref in weakness.evidence):
                raise ValueError("evidence outside development view")


class ProposalResult(Contract):
    schema_version: Literal[1]
    selected_weakness_id: Identifier
    summary: Summary
    claimed_paths: Annotated[
        tuple[Annotated[str, Field(max_length=512)], ...], Field(max_length=6)
    ]
    risks: Annotated[tuple[Risk, ...], Field(max_length=8)]


class RoleExecution(Contract):
    role_id: UUID
    job_id: UUID
    experiment_id: UUID
    round_id: UUID
    kind: RoleKind
    accepted_snapshot_id: UUID
    source_digest: Sha256
    input_digest: Sha256
    input_ref: Annotated[str, Field(max_length=512)]
    output_digest: Sha256 | None = None
    output_ref: Annotated[str, Field(max_length=512)] | None = None
    status: Literal["pending", "running", "complete", "invalid", "interrupted", "error"]
    termination: Annotated[str, Field(max_length=100)] | None = None
    usage_ref: UUID | None = None
    error_code: Annotated[str, Field(max_length=200)] | None = None


class CaseCount(Contract):
    case_id: Annotated[str, Field(min_length=1, max_length=200)]
    split: Literal["development", "selection-validation"]
    baseline: Annotated[int, Field(ge=0, le=3)]
    candidate: Annotated[int, Field(ge=0, le=3)]


class SelectionDecision(Contract):
    schema_version: Literal[1]
    verdict: Literal["promote", "reject", "inconclusive"]
    reason: Literal[
        "strict_development_gain",
        "case_regression",
        "no_development_gain",
        "scope_rejected",
        "boundary_rejected",
        "no_change",
        "invalid_proposal",
        "incomplete_evidence",
        "infrastructure_error",
    ]
    baseline_source_digest: Sha256
    baseline_export_digest: Sha256
    candidate_source_digest: Sha256
    candidate_export_digest: Sha256
    trial_ids: Annotated[tuple[UUID, ...], Field(max_length=49152)]
    counts: Annotated[tuple[CaseCount, ...], Field(max_length=4096)]
    boundary_result_digest: Sha256
    policy_digest: Sha256


class RoleDecisionFeedback(Contract):
    verdict: Literal["promote", "reject", "inconclusive"]
    reason: Annotated[str, Field(max_length=100)]
    development: Annotated[tuple[CaseCount, ...], Field(max_length=4096)]
    hidden_gate_rejected: bool


def role_feedback(decision: SelectionDecision) -> RoleDecisionFeedback:
    development = tuple(c for c in decision.counts if c.split == "development")
    hidden = any(
        c.candidate < c.baseline
        for c in decision.counts
        if c.split == "selection-validation"
    )
    return RoleDecisionFeedback(
        verdict=decision.verdict,
        reason="hidden_gate_rejected"
        if hidden and not any(c.candidate < c.baseline for c in development)
        else decision.reason,
        development=development,
        hidden_gate_rejected=hidden,
    )


def parse_mining_output(raw: str, evidence: tuple[EvidenceRef, ...]) -> MiningResult:
    if len(raw.encode()) > 1_048_576:
        raise ValueError("role output exceeds cap")
    result = MiningResult.model_validate_json(raw)
    if any(
        ref not in evidence
        for weakness in result.weaknesses
        for ref in weakness.evidence
    ):
        raise ValueError("evidence outside development view")
    return result


class RoleRunner:
    def __init__(
        self,
        store: "ExperimentStore",
        root: Path,
        supervisor: "WorkerSupervisor | None",
        views: "DevelopmentViewBuilder",
    ) -> None:
        self.store, self.root, self.supervisor, self.views = (
            store,
            root,
            supervisor,
            views,
        )

    async def mine(
        self, round_id: UUID, view: DevelopmentView, correction_of: UUID | None = None
    ) -> RoleExecution:
        round = await self.store.round(round_id)
        previous = await self.store.roles(round_id)
        correction = None
        if correction_of is not None:
            original = next(
                (r.execution for r in previous if r.execution.role_id == correction_of),
                None,
            )
            if (
                original is None
                or original.kind != "mining"
                or original.status != "invalid"
                or any(r.execution.kind == "format_correction" for r in previous)
            ):
                raise ValueError(
                    "correction requires invalid mining and no previous correction"
                )
            from crucible.improvement.trials import owned_artifact
            from crucible.improvement.worker_supervisor import read_regular_artifact

            assert original.output_ref is not None
            output = read_regular_artifact(
                owned_artifact(self.root, original.output_ref), 1_048_576
            )
            if hashlib.sha256(output).hexdigest() != original.output_digest:
                raise ValueError("original mining output digest drift")
            correction = {
                "original_output": output.decode(),
                "validation_error": read_regular_artifact(
                    self.root
                    / "roles"
                    / str(original.role_id)
                    / "validation-errors.txt",
                    4096,
                ).decode(),
            }
        elif previous:
            raise ValueError("mining already scheduled; no behavioral retry")
        return await self._run(
            round,
            view,
            "format_correction" if correction else "mining",
            {
                "correction": correction,
                "output_schema": MiningResult.model_json_schema(),
            },
        )

    @staticmethod
    def _git_registry(workspace: Path) -> dict[str, list[object]]:
        from crucible.improvement.source import safe_path
        from crucible.improvement.worker_supervisor import read_regular_artifact

        registry: dict[str, list[object]] = {}
        for path in (workspace / ".git").rglob("*"):
            safe_path(path)
            if path.is_file():
                registry[path.relative_to(workspace).as_posix()] = [
                    0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                    hashlib.sha256(
                        read_regular_artifact(path, 2 * 1024 * 1024)
                    ).hexdigest(),
                ]
        return registry

    async def recover_mining(
        self, execution: RoleExecution, view: DevelopmentView
    ) -> RoleExecution:
        """Continue the same undispatched job after explicit lifecycle recovery."""
        from crucible.improvement.contracts import HarnessSnapshot
        from crucible.improvement.trials import owned_artifact
        from crucible.improvement.worker_protocol import RoleJob
        from crucible.improvement.worker_supervisor import read_regular_artifact

        round = await self.store.round(execution.round_id)
        directory = self.root / "roles" / str(execution.role_id)
        workspace = owned_artifact(self.root, execution.input_ref).parent
        encoded = read_regular_artifact(workspace / "input.json", 1_048_576)
        if (
            hashlib.sha256(encoded).hexdigest() != execution.input_digest
            or json.loads(encoded)["view_index_digest"] != view.index_digest
        ):
            raise ValueError("mining recovery identity drift")
        # A job directory is created before credentials or reservations. Its presence
        # requires reconciliation, never a second execution of a dispatched role.
        if (self.root / "jobs" / str(execution.job_id)).exists():
            raise ValueError("dispatched mining requires execution reconciliation")
        row = await self.store.snapshot(
            execution.experiment_id, execution.accepted_snapshot_id
        )
        snapshot_path = owned_artifact(self.root, str(row["artifact_ref"]))
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(snapshot_path, 65536)
        )
        run = await self.store.require_search_open(execution.experiment_id)
        job = self._job(
            run.manifest,
            snapshot,
            round.id,
            execution.kind,
            execution.role_id,
            execution.job_id,
            execution.input_digest,
        )
        job_path = directory / "job.json"
        if (
            job_path.exists()
            and RoleJob.model_validate_json(read_regular_artifact(job_path, 65536))
            != job
        ):
            raise ValueError("mining recovery job drift")
        from crucible.improvement.source import safe_path, verify_harness_export

        verify_harness_export(snapshot_path.parent / "harness", snapshot)
        self.views.export_root(view)
        expected = json.loads(
            read_regular_artifact(
                directory / "workspace-original.json", 40 * 1024 * 1024
            )
        )
        actual = {}
        for path in workspace.rglob("*"):
            safe_path(path)
            if path.is_file() and path.relative_to(workspace).parts[0] != ".git":
                actual[path.relative_to(workspace).as_posix()] = [
                    0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                    read_regular_artifact(path, 2 * 1024 * 1024).hex(),
                ]
        if actual != expected:
            raise ValueError("mining recovery workspace drift")
        git_registry = directory / "mining-git.json"
        if git_registry.exists():
            if self._git_registry(workspace) != json.loads(
                read_regular_artifact(git_registry, 1_048_576)
            ):
                raise ValueError("mining recovery Git metadata drift")
        else:
            # Historical jobs have no Git checksum registry. Preserve their metadata
            # privately and replace it with a fresh repository containing no history.
            from crucible.improvement.source import git_bytes

            safe_path(workspace / ".git")
            shutil.move(
                workspace / ".git", directory / ("archived-git-" + str(uuid4()))
            )
            git_bytes(workspace, "init", "-q")
        execution = await self.store.recover_undispatched_role(execution)
        return await self._execute_role(
            round,
            execution,
            job,
            snapshot,
            snapshot_path,
            workspace,
            json.loads(encoded),
            view,
        )

    async def propose(
        self, round_id: UUID, mining_result: MiningResult, view: DevelopmentView
    ) -> RoleExecution:
        round = await self.store.round(round_id)
        mining_result.validate_evidence(view)
        if mining_result.disposition != "actionable":
            raise ValueError("proposal requires actionable mining")
        previous = await self.store.roles(round_id)
        if any(r.execution.kind == "proposal" for r in previous) or not any(
            r.execution.kind in ("mining", "format_correction")
            and r.execution.status == "complete"
            for r in previous
        ):
            raise ValueError("proposal requires completed mining and one opportunity")
        from crucible.improvement.trials import owned_artifact
        from crucible.improvement.worker_supervisor import read_regular_artifact

        original = [
            r.execution
            for r in previous
            if r.execution.kind in ("mining", "format_correction")
            and r.execution.status == "complete"
        ][-1]
        if original.output_ref is None:
            raise ValueError("recorded mining output unavailable")
        raw = read_regular_artifact(
            owned_artifact(self.root, original.output_ref), 1_048_576
        )
        raw_input = read_regular_artifact(
            owned_artifact(self.root, original.input_ref), 1_048_576
        )
        if (
            hashlib.sha256(raw).hexdigest() != original.output_digest
            or hashlib.sha256(raw_input).hexdigest() != original.input_digest
            or json.loads(raw_input)["view_index_digest"] != view.index_digest
            or parse_mining_output(raw.decode(), view.evidence_refs) != mining_result
        ):
            raise ValueError("proposal does not match recorded mining evidence")
        return await self._run(
            round,
            view,
            "proposal",
            {
                "mining": mining_result.model_dump(mode="json"),
                "output_schema": ProposalResult.model_json_schema(),
            },
        )

    @staticmethod
    def _job(
        manifest: "ExperimentManifest",
        snapshot: "HarnessSnapshot",
        round_id: UUID,
        kind: RoleKind,
        role_id: UUID,
        job_id: UUID,
        input_digest: str,
    ) -> "RoleJob":
        from crucible.improvement.worker_protocol import RoleJob

        return RoleJob(
            schema_version=2,
            protocol_version=2,
            job_id=job_id,
            role_id=role_id,
            round_id=round_id,
            experiment_id=manifest.experiment_id,
            job_kind=kind,
            harness_digest=snapshot.source_digest,
            export_digest=snapshot.export_digest,
            model=manifest.model,
            limits=manifest.role_limits["proposal" if kind == "proposal" else "mining"],
            input_digest=input_digest,
            workspace="/workspace",
            command_image=manifest.worker_image,
            command_platform=manifest.worker_platform,
            output_limit_bytes=min(
                manifest.command_policy.max_output_bytes_per_call, 8 * 1024 * 1024
            ),
            wall_deadline_seconds=manifest.role_limits[
                "proposal" if kind == "proposal" else "mining"
            ].wall_timeout_seconds,
        )

    async def _run(
        self,
        round: "RoundRecord",
        view: DevelopmentView,
        kind: RoleKind,
        role_input: dict[str, object],
    ) -> RoleExecution:
        from crucible.improvement.contracts import HarnessSnapshot
        from crucible.improvement.source import git_bytes, verify_harness_export
        from crucible.improvement.trials import owned_artifact
        from crucible.improvement.worker_supervisor import read_regular_artifact
        from crucible.repair.identity import canonical_json

        run = await self.store.get(round.experiment_id)
        if (
            run.stop_requested
            or run.status not in ("ready", "running")
            or round.status in ("decided", "interrupted", "error")
            or view.baseline_snapshot_id != round.baseline_snapshot_id
        ):
            raise ValueError("role requires active matching round")
        source_row = await self.store.snapshot(run.id, round.baseline_snapshot_id)
        snapshot_path = owned_artifact(self.root, str(source_row["artifact_ref"]))
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(snapshot_path, 65536)
        )
        if snapshot.source_digest != view.source_digest:
            raise ValueError("role accepted source mismatch")
        verify_harness_export(snapshot_path.parent / "harness", snapshot)
        view_root = self.views.export_root(view)
        role_id = uuid4()
        directory = self.root / "roles" / str(role_id)
        directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        workspace = directory / "workspace"
        shutil.copytree(view_root, workspace)
        if kind == "proposal":
            shutil.rmtree(workspace / "source")
            shutil.copytree(snapshot_path.parent / "harness", workspace / "source")
        # A fresh local repository supports ordinary workspace tools, with no history.
        git_bytes(workspace, "init", "-q")
        encoded = canonical_json(
            {
                "kind": kind,
                "view_index_digest": view.index_digest,
                "index": "index.json",
                "allowed_paths": EDITABLE_PATHS,
                **role_input,
            }
        )
        if len(encoded) > 1_048_576:
            raise ValueError("role input exceeds cap")
        (workspace / "input.json").write_bytes(encoded)
        job = self._job(
            run.manifest,
            snapshot,
            round.id,
            kind,
            role_id,
            uuid4(),
            hashlib.sha256(encoded).hexdigest(),
        )
        # This private checksum registry is outside the role-readable workspace.
        original_files = {}
        for path in workspace.rglob("*"):
            if path.is_file() and ".git" not in path.relative_to(workspace).parts:
                original_files[path.relative_to(workspace).as_posix()] = (
                    0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                    path.read_bytes(),
                )
        (directory / "workspace-original.json").write_bytes(
            canonical_json(
                {
                    name: [mode, data.hex()]
                    for name, (mode, data) in original_files.items()
                }
            )
        )
        git_bytes(workspace, "add", "--", *sorted(original_files))
        git_bytes(
            workspace,
            "-c",
            "user.name=Crucible",
            "-c",
            "user.email=crucible@localhost",
            "commit",
            "-qm",
            "Role input",
        )
        if kind != "proposal":
            (directory / "mining-git.json").write_bytes(
                canonical_json(self._git_registry(workspace))
            )
        execution = RoleExecution(
            role_id=role_id,
            job_id=job.job_id,
            round_id=round.id,
            experiment_id=run.id,
            kind=kind,
            accepted_snapshot_id=snapshot.snapshot_id,
            source_digest=snapshot.source_digest,
            input_digest=job.input_digest,
            input_ref=str((workspace / "input.json").relative_to(self.root)),
            status="running",
            usage_ref=role_id,
        )
        attempt = 1 if kind == "format_correction" else 0
        await self.store.save_role(execution, attempt, run.revision)
        await self.store.record_event(
            run.id,
            "role_usage_group",
            {"usage_ref": str(role_id), "job_id": str(job.job_id)},
        )
        (directory / "job.json").write_text(job.model_dump_json())
        return await self._execute_role(
            round, execution, job, snapshot, snapshot_path, workspace, role_input, view
        )

    async def _execute_role(
        self,
        round: "RoundRecord",
        execution: RoleExecution,
        job: "RoleJob",
        snapshot: "HarnessSnapshot",
        snapshot_path: Path,
        workspace: Path,
        role_input: dict[str, object],
        view: DevelopmentView,
    ) -> RoleExecution:
        from crucible.improvement.trials import owned_artifact
        from crucible.improvement.worker_supervisor import read_regular_artifact
        from crucible.repair.identity import canonical_json

        run = await self.store.get(execution.experiment_id)
        directory = self.root / "roles" / str(execution.role_id)
        kind = execution.kind
        attempt = 1 if kind == "format_correction" else 0
        try:
            if self.supervisor is None:
                raise ValueError("role supervisor unavailable")
            result = await self.supervisor.execute(
                job,
                snapshot,
                workspace,
                harness_export=snapshot_path.parent / "harness",
            )
            if not result.cleanup_confirmed:
                raise ValueError("role cleanup uncertain")
            execution = execution.model_copy(
                update={"termination": result.observation.termination}
            )
            current = await self.store.get(run.id)
            if current.stop_requested or result.observation.termination not in (
                "completed",
                "budget_exhausted",
            ):
                raise ValueError("role stopped or infrastructure failure")
            raw = read_regular_artifact(
                owned_artifact(
                    result.state_root, result.observation.assistant_output_ref
                ),
                min(job.output_limit_bytes, 1_048_576),
            )
            trace = read_regular_artifact(
                owned_artifact(result.state_root, result.observation.trace_ref),
                job.output_limit_bytes,
            )
            (directory / "assistant.txt").write_bytes(raw)
            (directory / "trace.json").write_bytes(trace)
            output_ref = str((directory / "assistant.txt").relative_to(self.root))
            execution = execution.model_copy(
                update={
                    "output_ref": output_ref,
                    "output_digest": hashlib.sha256(raw).hexdigest(),
                }
            )
            status: Literal["complete", "invalid"] = "complete"
            error = None
            try:
                if kind == "proposal":
                    proposal = ProposalResult.model_validate_json(raw)
                    mining = MiningResult.model_validate_json(
                        canonical_json(role_input["mining"])
                    )
                    if proposal.selected_weakness_id not in {
                        w.id for w in mining.weaknesses
                    }:
                        raise ValueError("unknown selected weakness")
                else:
                    parse_mining_output(raw.decode(), view.evidence_refs)
            except ValueError as validation_error:
                (directory / "validation-errors.txt").write_bytes(
                    str(validation_error).encode()[:4096]
                )
                status, error = "invalid", "invalid_output"
            if (
                status == "invalid"
                and result.observation.termination == "budget_exhausted"
            ):
                raise ValueError("role budget exhausted without valid output")
            execution = execution.model_copy(
                update={
                    "status": status,
                    "output_ref": output_ref,
                    "output_digest": hashlib.sha256(raw).hexdigest(),
                    "termination": result.observation.termination,
                    "error_code": error,
                }
            )
        except BaseException as failure:
            import asyncio

            interrupted = isinstance(failure, asyncio.CancelledError)
            execution = execution.model_copy(
                update={
                    "status": "interrupted" if interrupted else "error",
                    "error_code": "role_interrupted"
                    if interrupted
                    else "role_infrastructure_error",
                }
            )
            # Infrastructure failures are not schema-correction opportunities.
            revision = (await self.store.get(run.id)).revision
            try:
                await self.store.save_role(execution, attempt, revision)
            finally:
                await self.store.fail_run(
                    run.id,
                    execution.error_code or "role_error",
                    interrupted=interrupted,
                )
            if interrupted or not isinstance(failure, Exception):
                raise
            return execution
        await self.store.save_role(
            execution, attempt, (await self.store.get(run.id)).revision
        )
        return execution

    async def collect_proposal(
        self, execution: RoleExecution, owned_worktree: "OwnedWorktree"
    ) -> "HarnessSnapshot":
        from crucible.improvement.source import (
            ScopeRejected,
            SourceInspection,
            _patch,
            atomic_source_file,
            safe_path,
            stage_harness,
        )
        from crucible.improvement.worker_supervisor import read_regular_artifact
        from crucible.repair.identity import canonical_json

        if execution.kind != "proposal" or execution.status not in (
            "complete",
            "invalid",
        ):
            raise ValueError("proposal output not available")
        roles = await self.store.roles(execution.round_id)
        if not any(r.execution == execution for r in roles):
            raise ValueError("unregistered proposal")
        run = await self.store.require_search_open(execution.experiment_id)
        if run.stop_requested or run.status not in ("ready", "running"):
            raise ValueError("proposal transfer stopped")
        expected_path = self.root / "worktrees" / str(run.id)
        if (
            owned_worktree.path != expected_path
            or owned_worktree.branch != "codex/improve/" + str(run.id)
            or owned_worktree.root != self.root / "ownership" / str(run.id)
            or owned_worktree.parent_commit != run.last_accepted_commit
        ):
            raise ValueError("proposal worktree ownership mismatch")
        directory = self.root / "roles" / str(execution.role_id)
        workspace = directory / "workspace"
        original = json.loads(
            read_regular_artifact(
                directory / "workspace-original.json", 64 * 1024 * 1024
            )
        )
        old = {
            name: (value[0], bytes.fromhex(value[1]))
            for name, value in original.items()
        }
        current: dict[str, tuple[int, bytes]] = {}
        total = 0
        for path in workspace.rglob("*"):
            if ".git" in path.relative_to(workspace).parts:
                continue
            safe_path(path)
            if path.is_dir():
                continue
            raw = read_regular_artifact(path, 2 * 1024 * 1024)
            total += len(raw)
            if total > 32 * 1024 * 1024 or len(current) >= 4096:
                raise ScopeRejected("proposal workspace exceeds limits")
            current[path.relative_to(workspace).as_posix()] = (
                0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                raw,
            )
        changes = tuple(
            sorted(
                name
                for name in set(old) | set(current)
                if old.get(name) != current.get(name)
            )
        )
        before = {
            n.removeprefix("source/"): v
            for n, v in old.items()
            if n.startswith("source/")
        }
        after = {
            n.removeprefix("source/"): v
            for n, v in current.items()
            if n.startswith("source/")
        }
        source_changes = tuple(
            n.removeprefix("source/") for n in changes if n.startswith("source/")
        )
        patch = _patch(SourceInspection("", source_changes, after, before))
        (directory / "candidate.diff").write_bytes(patch)
        protected = tuple(
            n for n in changes if n not in {"source/" + p for p in EDITABLE_PATHS}
        )
        (directory / "scope.json").write_bytes(
            canonical_json({"changes": changes, "protected": protected})
        )
        if protected:
            raise ScopeRejected("protected proposal changes", protected)
        if not changes:
            raise ScopeRejected("no_change")
        if execution.status == "invalid":
            raise ScopeRejected("invalid_proposal")
        inspection = await owned_worktree.inspect()
        if inspection.source_digest != execution.source_digest:
            raise ScopeRejected("accepted worktree changed before proposal transfer")
        from crucible.benchmarks.evidence import atomic_json

        before_hashes = {
            name: [mode, hashlib.sha256(raw).hexdigest()]
            for name, (mode, raw) in inspection.files.items()
        }
        after_hashes = {
            name: [after[name][0], hashlib.sha256(after[name][1]).hexdigest()]
            if name in after
            else None
            for name in source_changes
        }
        intent = {"state": "intent", "before": before_hashes, "after": after_hashes}
        intent_path = owned_worktree.root / "proposal-transfer.json"
        atomic_json(intent_path, intent)
        await self.store.record_event(
            run.id, "proposal_transfer_intent", {"role_id": str(execution.role_id)}
        )
        for name in source_changes:
            current_run = await self.store.require_search_open(run.id)
            if current_run.stop_requested or current_run.status not in (
                "ready",
                "running",
            ):
                raise ValueError("proposal transfer stopped")
            path = owned_worktree.path / name
            safe_path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_source_file(
                path, after.get(name), owned_worktree.root / "transfer-staging"
            )
        await owned_worktree.checkpoint_owned_edits()
        atomic_json(intent_path, {**intent, "state": "complete"})
        snapshot = stage_harness(
            owned_worktree.path,
            owned_worktree.parent_commit,
            directory / "candidate",
            EDITABLE_PATHS,
        )
        for existing in await self.store.snapshots(execution.experiment_id):
            if (existing["source_digest"], existing["export_digest"]) == (
                snapshot.source_digest,
                snapshot.export_digest,
            ):
                from crucible.improvement.contracts import HarnessSnapshot
                from crucible.improvement.source import verify_harness_export
                from crucible.improvement.trials import owned_artifact

                if existing["status"] != "verified":
                    raise ValueError("candidate content requires explicit recovery")
                artifact = owned_artifact(self.root, str(existing["artifact_ref"]))
                canonical = HarnessSnapshot.model_validate_json(
                    read_regular_artifact(artifact, 65536)
                )
                verify_harness_export(artifact.parent / "harness", canonical)
                return canonical
        await self.store.register_snapshot(
            execution.experiment_id,
            snapshot,
            str((directory / "candidate/snapshot.json").relative_to(self.root)),
        )
        return snapshot
