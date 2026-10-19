"""Controller-owned, positive exports of completed development evidence only."""

import hashlib
import json
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from pydantic import TypeAdapter

from crucible.benchmarks.contracts import TrialResult
from crucible.benchmarks.evidence import sanitize
from crucible.improvement.contracts import HarnessSnapshot
from crucible.improvement.roles import DevelopmentView, EvidenceRef, role_feedback
from crucible.improvement.source import safe_path, verify_harness_export
from crucible.improvement.store import ExperimentStore
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json, digest

MAX_ARTIFACT = 1_048_576
MAX_VIEW = 16 * MAX_ARTIFACT
MARKER = b"\n[TRUNCATED: original digest retained in evidence reference]\n"


def _owned(root: Path, ref: str) -> Path:
    name = Path(ref)
    if name.is_absolute() or not name.parts or ".." in name.parts:
        raise ValueError("invalid controller artifact reference")
    path = root / name
    safe_path(path)
    return path


def _bounded(raw: bytes, maximum: int = MAX_ARTIFACT) -> bytes:
    if len(raw) <= maximum:
        return raw
    prefix = raw[: maximum - len(MARKER)].decode("utf-8", errors="ignore").encode()
    return prefix + MARKER


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

    async def build_development_view(
        self, experiment_id: UUID, baseline_snapshot_id: UUID, output_dir: Path
    ) -> DevelopmentView:
        run = await self.store.get(experiment_id)
        if run.manifest.schema_version != 2 or run.stop_requested:
            raise ValueError("v2 active search required")
        row = await self.store.snapshot(experiment_id, baseline_snapshot_id)
        snapshot_file = _owned(self.root, str(row["artifact_ref"]))
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(snapshot_file, 65536)
        )
        if (
            (
                str(snapshot.snapshot_id),
                snapshot.source_digest,
                snapshot.export_digest,
                snapshot.parent_commit,
            )
            != (
                str(row["id"]),
                row["source_digest"],
                row["export_digest"],
                row["parent_commit"],
            )
            or row["status"] != "verified"
            or snapshot.snapshot_id
            != await self.store.accepted_snapshot_id(experiment_id)
        ):
            raise ValueError("accepted snapshot registry drift")
        harness = snapshot_file.parent / "harness"
        verify_harness_export(harness, snapshot)
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
        index = []
        for (case_id, repeat), trial in sorted(selected.items()):
            reference = json.loads(str(trial["artifact_ref"]))
            result_path = _owned(self.root, reference["path"])
            raw = read_regular_artifact(result_path, 2 * MAX_ARTIFACT)
            if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
                raise ValueError("trial evidence digest drift")
            envelope = json.loads(raw)
            result = TypeAdapter(TrialResult).validate_json(
                canonical_json(envelope["result"]), strict=True
            )
            identity = result.identity
            if (
                (
                    str(identity.trial_id),
                    identity.repeat_index,
                    identity.case_digest,
                    identity.harness_tree_digest,
                    identity.model_config_digest,
                    identity.limits_digest,
                )
                != (
                    trial["id"],
                    repeat,
                    development[case_id].case_digest,
                    snapshot.source_digest,
                    digest(run.manifest.model),
                    digest(run.manifest.trial_limits),
                )
                or result.status != "complete"
                or result.grade is None
                or result.grade.verdict not in ("pass", "fail")
                or result.solver is None
                or result.patch is None
                or result.solver.termination
                not in ("completed", "budget_exhausted", "behavior_error")
            ):
                raise ValueError("incomplete or incomparable development evidence")
            trial_refs = []
            artifacts: tuple[tuple[Literal["trace", "solution_diff"], str], ...] = (
                ("trace", "trace.json"),
                ("solution_diff", "solution.diff"),
            )
            for artifact, filename in artifacts:
                original = read_regular_artifact(
                    result_path.parent / filename, 8 * MAX_ARTIFACT
                )
                original_digest = hashlib.sha256(original).hexdigest()
                if envelope["artifacts"].get(filename) != original_digest:
                    raise ValueError("original artifact digest drift")
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
            # Official feedback may contain protected tests and private paths.
            # Export only the independent verdict.
            index.append(
                {
                    "case_id": case_id,
                    "repeat_index": repeat,
                    "trial_id": identity.trial_id,
                    "termination": result.solver.termination,
                    "verdict": result.grade.verdict,
                    "grade_feedback": "unavailable: protected grader details excluded",
                    "evidence": trial_refs,
                }
            )
        prior = canonical_json(
            [
                role_feedback(decision)
                for decision in await self.store.decisions(experiment_id)
            ]
        )
        write("prior-decisions.json", prior)
        encoded = canonical_json(
            {
                "trials": index,
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
