"""Controller-owned, positive exports of completed development evidence only."""

import hashlib
import json
from collections import Counter
from collections.abc import Iterator
from itertools import islice
from pathlib import Path
from typing import Literal, cast
from uuid import UUID, uuid4

from crucible.benchmarks.evidence import sanitize
from crucible.domain.tools import ToolResultStatus
from crucible.improvement.case_mining import MAX_PACKAGES, PinnedPackage
from crucible.improvement.contracts import SourceIdentity
from crucible.improvement.roles import DevelopmentView, EvidenceRef, role_feedback
from crucible.improvement.source import safe_path
from crucible.improvement.store import ExperimentStore
from crucible.improvement.trajectory_contracts import (
    ExportLimits,
    TrialSelection,
    VerifiedDevelopmentInputs,
    VerifiedTrial,
)
from crucible.improvement.trajectory_export import export_trajectory_package
from crucible.improvement.verified_evidence import (
    owned_artifact as _owned,
)
from crucible.improvement.verified_evidence import (
    verified_artifact,
    verified_snapshot,
    verified_trial_result,
)
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json

MAX_ARTIFACT = 1_048_576
MAX_VIEW = 16 * MAX_ARTIFACT
MARKER = b"\n[TRUNCATED: original digest retained in evidence reference]\n"
MAX_FAILURE_DETAILS = 16
MAX_FAILURE_DETAIL_BYTES = 8_192
MAX_FAILURE_VIEW_BYTES = 512 * 1024


def _bounded(raw: bytes, maximum: int = MAX_ARTIFACT) -> bytes:
    if len(raw) <= maximum:
        return raw
    prefix = raw[: maximum - len(MARKER)].decode("utf-8", errors="ignore").encode()
    return prefix + MARKER


def _failed_tool_calls(
    trace: str,
) -> Iterator[tuple[dict[str, object], dict[str, object]]]:
    """Link structured results to calls within one sanitized solver trace."""
    try:
        data = json.loads(trace)
    except (ValueError, RecursionError):
        return
    if isinstance(data, dict) and data.get("trust") == "untrusted-worker-observation":
        data = data.get("data")
    if not isinstance(data, dict) or data.get("schema_version") != 2:
        return
    calls, results = data.get("tool_calls"), data.get("tool_results")
    if not isinstance(calls, list) or not isinstance(results, list):
        return
    call_counts = Counter(
        call["id"]
        for call in calls
        if isinstance(call, dict) and isinstance(call.get("id"), str)
    )
    result_counts = Counter(
        result["tool_call_id"]
        for result in results
        if isinstance(result, dict) and isinstance(result.get("tool_call_id"), str)
    )
    by_call = {
        result["tool_call_id"]: result
        for result in results
        if isinstance(result, dict) and isinstance(result.get("tool_call_id"), str)
    }
    failure_statuses = set(ToolResultStatus) - {ToolResultStatus.SUCCEEDED}
    for call in calls:
        if not isinstance(call, dict) or not isinstance(call.get("id"), str):
            continue
        call_id = call["id"]
        if call_counts[call_id] != 1 or result_counts[call_id] != 1:
            continue
        result = by_call[call_id]
        if (
            isinstance(call.get("name"), str)
            and isinstance(call.get("arguments"), dict)
            and isinstance(result.get("status"), str)
            and result["status"] in failure_statuses
        ):
            yield call, result


def _failure_detail(value: object) -> object:
    raw = canonical_json(value)
    limit = MAX_FAILURE_DETAIL_BYTES // 2 - 256
    if len(raw) <= limit:
        return value
    # Keep the detail document valid JSON and preserve both sides of the exchange.
    return {
        "truncated": True,
        "json_prefix": raw[: limit // 2].decode("utf-8", errors="ignore"),
    }


def tool_failure_files(trace: str, trial_id: UUID, byte_limit: int) -> dict[str, bytes]:
    """Build a small summary and linked detail files from a sanitized trace."""
    files: dict[str, bytes] = {}
    summary = bytearray()
    total = 0
    base = f"evidence/{trial_id}"
    for ordinal, (call, failure) in enumerate(
        islice(_failed_tool_calls(trace), MAX_FAILURE_DETAILS), start=1
    ):
        path = f"{base}/failure-{ordinal}.json"
        detail = canonical_json(
            {
                "tool_call_id": call["id"],
                "tool_name": call["name"],
                "arguments": _failure_detail(call["arguments"]),
                "result": _failure_detail(failure),
            }
        )
        row = (
            canonical_json(
                {
                    "tool": str(call["name"])[:80],
                    "error": str(failure.get("error_code") or "")[:80],
                    "message": str(failure.get("display_text") or "")[:160],
                    "file": path,
                }
            )
            + b"\n"
        )
        if len(detail) > MAX_FAILURE_DETAIL_BYTES:
            continue
        total += len(detail) + len(row)
        if total > byte_limit:
            break
        files[path] = detail
        summary.extend(row)
    if summary:
        files[f"{base}/tool-failures.jsonl"] = bytes(summary)
    return files


class DevelopmentViewBuilder:
    def __init__(self, store: ExperimentStore, root: Path) -> None:
        safe_path(root)
        self.store, self.root = store, root
        self._roots: dict[UUID, Path] = {}
        self._files: dict[UUID, dict[str, str]] = {}
        self._views: dict[
            UUID, tuple[DevelopmentView, dict[EvidenceRef, tuple[Path, str]]]
        ] = {}

    def export_root(self, view: DevelopmentView) -> Path:
        registered, _ = self._views[view.view_id]
        if registered != view:
            raise ValueError("development view identity drift")
        root = self._roots[view.view_id]
        if (
            hashlib.sha256(
                read_regular_artifact(root / "index.json", MAX_ARTIFACT)
            ).hexdigest()
            != view.index_digest
        ):
            raise ValueError("development index digest drift")
        expected = self._files[view.view_id]
        actual = {
            p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
        }
        if actual != set(expected):
            raise ValueError("development view file registry drift")
        for name, sha in expected.items():
            if (
                hashlib.sha256(
                    read_regular_artifact(root / name, MAX_ARTIFACT)
                ).hexdigest()
                != sha
            ):
                raise ValueError("development view file digest drift")
        for ref in view.evidence_refs:
            self.resolve_evidence(view, ref)
        return root

    def resolve_evidence(self, view: DevelopmentView, ref: EvidenceRef) -> Path:
        registered, refs = self._views[view.view_id]
        if registered != view or ref not in refs or ref not in view.evidence_refs:
            raise ValueError("evidence outside registered development view")
        path, expected = refs[ref]
        if (
            hashlib.sha256(read_regular_artifact(path, MAX_ARTIFACT)).hexdigest()
            != expected
        ):
            raise ValueError("exported evidence digest drift")
        return path

    async def build_case_packages(
        self, experiment_id: UUID, view: DevelopmentView, output_dir: Path
    ) -> tuple[PinnedPackage, ...]:
        """Export complete failed development families from this round's ledger."""
        self.export_root(view)
        run = await self.store.require_search_open(experiment_id)
        row = await self.store.snapshot(experiment_id, view.baseline_snapshot_id)
        snapshot, harness = verified_snapshot(
            self.root, row, await self.store.accepted_snapshot_id(experiment_id)
        )
        if snapshot.source_digest != view.source_digest:
            raise ValueError("case mining source drift")
        development = {
            case.case_id: case
            for case in run.manifest.cases
            if case.split == "development"
        }
        selected = [
            trial
            for trial in await self.store.selected_trials(experiment_id)
            if trial["snapshot"] == str(view.baseline_snapshot_id)
            and str(trial["case_id"]) in development
        ]
        failed_cases = {
            str(trial["case_id"])
            for trial in selected
            if trial["grade_verdict"] == "fail"
        }
        if not 1 <= len(failed_cases) <= MAX_PACKAGES:
            raise ValueError("case mining requires 1–10 failed development families")
        safe_path(output_dir)
        output_dir.mkdir(mode=0o700)
        source = SourceIdentity(
            commit=snapshot.parent_commit,
            source_digest=snapshot.source_digest,
            export_digest=snapshot.export_digest,
        )
        source_files = {
            path.relative_to(harness).as_posix(): (
                0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                read_regular_artifact(path, 2 * MAX_ARTIFACT),
            )
            for path in sorted(harness.rglob("*"))
            if path.is_file()
        }
        refs = {(ref.trial_id, ref.artifact): ref for ref in view.evidence_refs}
        packages = []
        for index, case_id in enumerate(sorted(failed_cases)):
            trials = []
            family = sorted(
                (t for t in selected if t["case_id"] == case_id),
                key=lambda t: int(str(t["repeat_index"])),
            )
            if [trial["repeat_index"] for trial in family] != [0, 1, 2]:
                raise ValueError("complete development family required")
            for trial in family:
                result, artifacts, directory = verified_trial_result(
                    self.root, trial, development[case_id], snapshot, run.manifest
                )
                assert result.solver is not None and result.grade is not None
                identity = result.identity
                trial_id = identity.trial_id
                trials.append(
                    VerifiedTrial(
                        trial_id,
                        case_id,
                        cast(Literal[0, 1, 2], identity.repeat_index),
                        result.solver.run_id,
                        result.solver.task_id,
                        cast(Literal["pass", "fail"], result.grade.verdict),
                        cast(
                            Literal["completed", "budget_exhausted", "behavior_error"],
                            result.solver.termination,
                        ),
                        source,
                        refs[trial_id, "trace"],
                        verified_artifact(
                            directory, artifacts, "trace.json", 8 * MAX_ARTIFACT
                        ),
                        refs[trial_id, "solution_diff"],
                        verified_artifact(
                            directory, artifacts, "solution.diff", 8 * MAX_ARTIFACT
                        ),
                        None,
                    )
                )
            ids = tuple(trial.trial_id for trial in trials)
            path = output_dir / str(index)
            package = export_trajectory_package(
                VerifiedDevelopmentInputs(
                    tuple(trials),
                    source,
                    source_files,
                    ids,
                    experiment_id,
                    view.baseline_snapshot_id,
                    TrialSelection(mode="explicit-three-repeat", trial_ids=ids),
                    (self.root, harness),
                ),
                path,
                ExportLimits(),
            )
            packages.append(PinnedPackage(path, package.bundle_digest))
        return tuple(packages)

    async def build_development_view(
        self, experiment_id: UUID, baseline_snapshot_id: UUID, output_dir: Path
    ) -> DevelopmentView:
        run = await self.store.require_search_open(experiment_id)
        if run.manifest.schema_version != 2 or run.stop_requested:
            raise ValueError("v2 active search required")
        row = await self.store.snapshot(experiment_id, baseline_snapshot_id)
        snapshot, harness = verified_snapshot(
            self.root, row, await self.store.accepted_snapshot_id(experiment_id)
        )
        development = {
            case.case_id: case
            for case in run.manifest.cases
            if case.split == "development"
        }
        selected = {}
        for trial in await self.store.selected_trials(experiment_id):
            if (
                trial["snapshot"] == str(baseline_snapshot_id)
                and trial["case_id"] in development
            ):
                key = (str(trial["case_id"]), int(str(trial["repeat_index"])))
                if key in selected:
                    raise ValueError("ambiguous development trial generations")
                selected[key] = trial
        expected = {(case, repeat) for case in development for repeat in range(3)}
        if set(selected) != expected or any(
            t["status"] != "complete" for t in selected.values()
        ):
            raise ValueError(
                "complete development baseline required; "
                "infrastructure evidence unavailable"
            )
        safe_path(output_dir)
        if output_dir.is_relative_to(self.root) or self.root.is_relative_to(output_dir):
            raise ValueError("view overlaps private state")
        output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        total = 0
        refs: dict[EvidenceRef, tuple[Path, str]] = {}

        def write(name: str, raw: bytes) -> None:
            nonlocal total
            total += len(raw)
            if total > MAX_VIEW:
                raise ValueError("development view exceeds total byte limit")
            path = _owned(output_dir, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)

        # Only files from the verified positive runtime export; never its siblings.
        for source in sorted(harness.rglob("*")):
            if source.is_file():
                write(
                    "source/" + source.relative_to(harness).as_posix(),
                    _bounded(read_regular_artifact(source, 2 * MAX_ARTIFACT)),
                )
        index: dict[tuple[str, int], dict[str, object]] = {}
        linked_failures: dict[tuple[str, int], dict[str, bytes]] = {}
        detail_bytes = 0
        # Spend the bounded failure index on failed trials first.
        for (case_id, repeat), trial in sorted(
            selected.items(),
            key=lambda item: (item[1]["grade_verdict"] != "fail", item[0]),
        ):
            result, artifact_hashes, directory = verified_trial_result(
                self.root, trial, development[case_id], snapshot, run.manifest
            )
            identity = result.identity
            assert (
                result.grade is not None
                and result.solver is not None
                and result.patch is not None
            )
            trial_refs = []
            failure_files: dict[str, bytes] = {}
            artifacts: tuple[tuple[Literal["trace", "solution_diff"], str], ...] = (
                ("trace", "trace.json"),
                ("solution_diff", "solution.diff"),
            )
            for artifact, filename in artifacts:
                original = verified_artifact(
                    directory, artifact_hashes, filename, 8 * MAX_ARTIFACT
                )
                original_digest = hashlib.sha256(original).hexdigest()
                if (
                    artifact == "solution_diff"
                    and result.patch.sha256 != original_digest
                ):
                    raise ValueError("solution digest drift")
                # Trace and patch predate grading. No grading files are exported.
                cleaned = str(sanitize(original.decode("utf-8"))).replace(
                    str(self.root), "[controller-state]"
                )
                exported = _bounded(cleaned.encode())
                ref = EvidenceRef(
                    trial_id=identity.trial_id,
                    artifact=artifact,
                    sha256=original_digest,
                )
                filename = f"evidence/{identity.trial_id}/{artifact}.txt"
                write(filename, exported)
                refs[ref] = (
                    output_dir / filename,
                    hashlib.sha256(exported).hexdigest(),
                )
                trial_refs.append(
                    {
                        "ref": ref,
                        "file": filename,
                        "truncated": len(cleaned.encode()) > MAX_ARTIFACT,
                    }
                )
                if artifact == "trace" and detail_bytes < MAX_FAILURE_VIEW_BYTES:
                    failure_files = tool_failure_files(
                        cleaned,
                        identity.trial_id,
                        MAX_FAILURE_VIEW_BYTES - detail_bytes,
                    )
                    detail_bytes += sum(map(len, failure_files.values()))
            # Official feedback may contain protected tests and private paths.
            # Export only the independent verdict.
            key = (case_id, repeat)
            index[key] = {
                "case_id": case_id,
                "repeat_index": repeat,
                "trial_id": identity.trial_id,
                "termination": result.solver.termination,
                "verdict": result.grade.verdict,
                "grade_feedback": "unavailable: protected grader details excluded",
                "evidence": trial_refs,
            }
            if failure_files:
                linked_failures[key] = failure_files
        prior = canonical_json(
            [
                role_feedback(decision)
                for decision in await self.store.decisions(experiment_id)
            ]
        )
        write("prior-decisions.json", prior)
        # Leave room for the required index even when the raw evidence fills the view.
        for key, failures in linked_failures.items():
            entry = index[key]
            summary_path = f"evidence/{entry['trial_id']}/tool-failures.jsonl"
            if total + sum(map(len, failures.values())) > MAX_VIEW - MAX_ARTIFACT:
                continue
            for path, content in failures.items():
                write(path, content)
            entry["tool_failures"] = summary_path
        encoded = canonical_json(
            {
                "trials": [index[key] for key in sorted(index)],
                "source": "source/",
                "prior_decisions": "prior-decisions.json",
            }
        )
        if len(encoded) > MAX_ARTIFACT:
            raise ValueError("development index exceeds byte limit")
        write("index.json", encoded)
        view = DevelopmentView(
            view_id=uuid4(),
            baseline_snapshot_id=baseline_snapshot_id,
            index_digest=hashlib.sha256(encoded).hexdigest(),
            evidence_refs=tuple(refs),
            source_digest=snapshot.source_digest,
            prior_decisions_digest=hashlib.sha256(prior).hexdigest(),
        )
        self._views[view.view_id] = (view, refs)
        self._roots[view.view_id] = output_dir
        self._files[view.view_id] = {
            p.relative_to(output_dir).as_posix(): hashlib.sha256(
                p.read_bytes()
            ).hexdigest()
            for p in output_dir.rglob("*")
            if p.is_file()
        }
        return view
