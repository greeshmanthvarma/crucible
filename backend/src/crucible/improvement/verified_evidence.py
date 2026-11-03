"""Positive evidence verification, including migration-free historical reads."""

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import Literal, cast
from uuid import UUID

from pydantic import TypeAdapter

from crucible.benchmarks.contracts import TrialResult
from crucible.improvement.contracts import (
    ExperimentManifest,
    FrozenCaseRef,
    HarnessSnapshot,
    SourceIdentity,
)
from crucible.improvement.manifest import manifest_digest
from crucible.improvement.roles import EvidenceRef
from crucible.improvement.source import _digest, safe_path, verify_harness_export
from crucible.improvement.trajectory import TrajectoryError
from crucible.improvement.trajectory_contracts import (
    TrialSelection,
    VerifiedDevelopmentInputs,
    VerifiedTrial,
)
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.gateway import TrustedUsage
from crucible.repair.identity import canonical_json, digest


def owned_artifact(root: Path, name: str) -> Path:
    path = Path(name)
    if (
        path.is_absolute()
        or not path.parts
        or ".." in path.parts
        or path.as_posix() != name
    ):
        raise TrajectoryError("unsafe_path")
    path = root / path
    try:
        safe_path(path)
    except ValueError as error:
        raise TrajectoryError("unsafe_path") from error
    return path


def verified_snapshot(
    root: Path, row: Mapping[str, object], accepted: UUID
) -> tuple[HarnessSnapshot, Path]:
    path = owned_artifact(root, str(row["artifact_ref"]))
    snapshot = HarnessSnapshot.model_validate_json(read_regular_artifact(path, 65536))
    if (
        (
            str(snapshot.snapshot_id),
            snapshot.source_digest,
            snapshot.export_digest,
            snapshot.parent_commit,
            snapshot.kind,
            snapshot.patch_digest,
        )
        != (
            str(row["id"]),
            row["source_digest"],
            row["export_digest"],
            row["parent_commit"],
            row["kind"],
            row["patch_digest"],
        )
        or row["status"] != "verified"
        or snapshot.snapshot_id != accepted
    ):
        raise TrajectoryError("artifact_drift", "accepted snapshot registry")
    verify_harness_export(path.parent / "harness", snapshot)
    return snapshot, path.parent / "harness"


def verified_trial_result(
    root: Path,
    row: Mapping[str, object],
    case: FrozenCaseRef,
    snapshot: HarnessSnapshot,
    manifest: ExperimentManifest,
) -> tuple[TrialResult, dict[str, object], Path]:
    reference = json.loads(str(row["artifact_ref"]))
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise TrajectoryError("artifact_drift", "result reference")
    path = owned_artifact(root, reference["path"])
    raw = read_regular_artifact(path, 2 * 1048576)
    if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
        raise TrajectoryError("artifact_drift", "trial result digest")
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
            identity.harness_commit,
            identity.harness_tree_digest,
            identity.model_config_digest,
            identity.limits_digest,
        )
        != (
            row["id"],
            row["repeat_index"],
            case.case_digest,
            snapshot.parent_commit,
            snapshot.source_digest,
            digest(manifest.model),
            digest(manifest.trial_limits),
        )
        or row["status"] != "complete"
        or result.status != "complete"
        or result.grade is None
        or result.grade.verdict not in ("pass", "fail")
        or result.solver is None
        or result.patch is None
        or result.solver.termination
        not in ("completed", "budget_exhausted", "behavior_error")
        or row["grade_verdict"] != result.grade.verdict
        or row["solver_termination"] != result.solver.termination
    ):
        raise TrajectoryError("identity_mismatch", "incomparable development trial")
    if not isinstance(envelope.get("artifacts"), dict):
        raise TrajectoryError("artifact_drift", "artifact registry")
    return result, envelope["artifacts"], path.parent


def verified_artifact(
    directory: Path, artifacts: Mapping[str, object], name: str, maximum: int
) -> bytes:
    raw = read_regular_artifact(owned_artifact(directory, name), maximum)
    if hashlib.sha256(raw).hexdigest() != artifacts.get(name):
        raise TrajectoryError("artifact_drift", "original artifact digest")
    return raw


@contextmanager
def archive_connection(root: Path) -> Iterator[sqlite3.Connection]:
    """Read a stable private DB/WAL copy; SQLite never opens original files.

    Two identical reads plus unchanged inode/size/mtime/ctime and membership
    seal capture. Concurrent writes fail closed without automatic retries.
    The private WAL is replayed by SQLite normally, never with immutable=1.
    """
    names = (
        "experiments.sqlite3",
        "experiments.sqlite3-wal",
        "experiments.sqlite3-journal",
    )

    def identity(info: os.stat_result) -> tuple[int, ...]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    def fence() -> dict[str, tuple[int, ...]]:
        result = {}
        for name in names:
            path = owned_artifact(root, name)
            if path.exists():
                result[name] = identity(path.stat())
        return result

    def capture() -> dict[str, tuple[tuple[int, ...], bytes]]:
        result = {}
        for name in names:
            path = owned_artifact(root, name)
            if not path.exists():
                continue
            before = path.stat()
            raw = read_regular_artifact(path, 256 * 1048576)
            after = path.stat()

            if identity(before) != identity(after):
                raise TrajectoryError(
                    "artifact_drift", "archive changed during snapshot"
                )
            if name.endswith("-journal") and raw:
                raise TrajectoryError(
                    "artifact_drift", "rollback journal requires offline reconciliation"
                )
            result[name] = (identity(after), raw)
        if "experiments.sqlite3" not in result:
            raise TrajectoryError("artifact_drift", "missing historical ledger")
        return result

    first = capture()
    with tempfile.TemporaryDirectory(prefix="crucible-archive-") as temporary:
        directory = Path(temporary)
        for name, (_, raw) in first.items():
            (directory / name).write_bytes(raw)
            (directory / name).chmod(0o600)
        if capture() != first or fence() != {
            name: info for name, (info, _) in first.items()
        }:
            raise TrajectoryError("artifact_drift", "archive changed during snapshot")
        conn = sqlite3.connect(
            (directory / "experiments.sqlite3").as_uri() + "?mode=ro", uri=True
        )
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            yield conn
        finally:
            conn.close()


class DevelopmentArchiveReader:
    """No write store, migration, lifecycle transition or external dispatch."""

    def __init__(self, root: Path) -> None:
        safe_path(root)
        self.root = root

    def load(
        self, experiment_id: UUID, snapshot_id: UUID, selection: TrialSelection
    ) -> VerifiedDevelopmentInputs:
        try:
            return self._load(experiment_id, snapshot_id, selection)
        except TrajectoryError:
            raise
        except (ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
            raise TrajectoryError(
                "artifact_drift", "archive preparation failed"
            ) from error

    def _load(
        self, experiment_id: UUID, snapshot_id: UUID, selection: TrialSelection
    ) -> VerifiedDevelopmentInputs:
        with archive_connection(self.root) as conn:
            run = conn.execute(
                "SELECT * FROM experiment_runs WHERE id=?", (str(experiment_id),)
            ).fetchone()
            if run is None:
                raise TrajectoryError("identity_mismatch", "unknown experiment")
            manifest = ExperimentManifest.model_validate_json(run["manifest_json"])
            if (
                manifest_digest(manifest) != run["manifest_hash"]
                or manifest.experiment_id != experiment_id
                or manifest.initial_commit != run["initial_commit"]
            ):
                raise TrajectoryError("artifact_drift", "frozen manifest")
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            promoted = (
                list(
                    conn.execute(
                        "SELECT candidate_snapshot FROM experiment_promotions "
                        "WHERE experiment=? AND state='complete' AND commit_sha=?",
                        (str(experiment_id), run["last_accepted_commit"]),
                    )
                )
                if "experiment_promotions" in tables
                else []
            )
            if promoted:
                if len(promoted) != 1 or promoted[0][0] != str(snapshot_id):
                    raise TrajectoryError("identity_mismatch", "accepted lineage")
            elif run["last_accepted_commit"] != run["initial_commit"]:
                raise TrajectoryError("identity_mismatch", "missing accepted lineage")
            if not promoted:
                baselines = list(
                    conn.execute(
                        "SELECT id FROM experiment_snapshots WHERE experiment=? "
                        "AND kind='baseline' AND source_digest=? AND parent_commit=? "
                        "AND status='verified'",
                        (
                            str(experiment_id),
                            manifest.initial_source_digest,
                            manifest.initial_commit,
                        ),
                    )
                )
                if len(baselines) != 1 or baselines[0][0] != str(snapshot_id):
                    raise TrajectoryError(
                        "identity_mismatch", "ambiguous accepted baseline"
                    )
            row = conn.execute(
                "SELECT * FROM experiment_snapshots WHERE experiment=? AND id=?",
                (str(experiment_id), str(snapshot_id)),
            ).fetchone()
            if row is None:
                raise TrajectoryError("identity_mismatch", "unknown snapshot")
            if not promoted and (
                row["kind"] != "baseline"
                or row["source_digest"] != manifest.initial_source_digest
                or row["parent_commit"] != manifest.initial_commit
            ):
                raise TrajectoryError("identity_mismatch", "initial snapshot")
            snapshot, harness = verified_snapshot(self.root, dict(row), snapshot_id)
            source = SourceIdentity(
                commit=snapshot.parent_commit,
                source_digest=snapshot.source_digest,
                export_digest=snapshot.export_digest,
            )
            development = {
                c.case_id: c for c in manifest.cases if c.split == "development"
            }
            rows: list[dict[str, object]] = []
            for case_id in sorted(development):
                rows.extend(
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM experiment_trials WHERE experiment=? "
                        "AND snapshot=? AND case_id=?",
                        (str(experiment_id), str(snapshot_id), case_id),
                    )
                )
            by_id = {str(r["id"]): r for r in rows}
            removed: set[str] = set()
            for event in conn.execute(
                "SELECT payload_json FROM experiment_events WHERE experiment=? "
                "AND type='trial_superseded' ORDER BY sequence",
                (str(experiment_id),),
            ):
                payload = json.loads(event[0])
                old_id, new_id = (
                    str(payload["original_trial_id"]),
                    str(payload["replacement_trial_id"]),
                )
                if old_id not in by_id and new_id not in by_id:
                    continue
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
                    raise TrajectoryError("identity_mismatch", "invalid supersession")
                removed.add(old_id)
            selected_rows = [r for r in rows if str(r["id"]) not in removed]
            keys = [(r["case_id"], r["repeat_index"]) for r in selected_rows]
            if len(keys) != len(set(keys)):
                raise TrajectoryError("identity_mismatch", "ambiguous generations")
            eligible = [
                r
                for r in selected_rows
                if r["status"] == "complete"
                and r["grade_verdict"] in ("pass", "fail")
                and r["solver_termination"]
                in ("completed", "budget_exhausted", "behavior_error")
            ]
            if selection.mode == "failed-family-v1":
                failures = sorted(
                    (r for r in eligible if r["grade_verdict"] == "fail"),
                    key=lambda r: (
                        str(r["case_id"]),
                        int(str(r["repeat_index"])),
                        str(r["id"]),
                    ),
                )
                if not failures:
                    raise TrajectoryError("no_failed_development_trial")
                seed = failures[0]
                family = sorted(
                    (
                        r
                        for r in eligible
                        if r["case_id"] == seed["case_id"] and r["id"] != seed["id"]
                    ),
                    key=lambda r: (
                        r["grade_verdict"] != "pass",
                        int(str(r["repeat_index"])),
                        str(r["id"]),
                    ),
                )
                ids = tuple(UUID(str(r["id"])) for r in [seed, *family][:3])
                if selection.trial_ids and selection.trial_ids != ids:
                    raise TrajectoryError(
                        "identity_mismatch", "frozen automatic selection"
                    )
                selection = TrialSelection(mode=selection.mode, trial_ids=ids)
            wanted = set(map(str, selection.trial_ids))
            chosen = {str(r["id"]): r for r in eligible if str(r["id"]) in wanted}
            if set(chosen) != wanted or not chosen:
                raise TrajectoryError("identity_mismatch", "ineligible selected trial")
            if len({r["case_id"] for r in chosen.values()}) != 1 or not any(
                r["grade_verdict"] == "fail" for r in chosen.values()
            ):
                raise TrajectoryError("no_failed_development_trial")
            if selection.mode == "explicit-three-repeat" and {
                r["repeat_index"] for r in chosen.values()
            } != {0, 1, 2}:
                raise TrajectoryError("identity_mismatch", "three repeats required")
            trials = []
            for trial_id in selection.trial_ids:
                trial_row = chosen[str(trial_id)]
                result, artifacts, directory = verified_trial_result(
                    self.root,
                    trial_row,
                    development[str(trial_row["case_id"])],
                    snapshot,
                    manifest,
                )
                assert (
                    result.patch is not None
                    and result.solver is not None
                    and result.grade is not None
                )
                trace = verified_artifact(directory, artifacts, "trace.json", 8388608)
                solution = verified_artifact(
                    directory, artifacts, "solution.diff", 8388608
                )
                if hashlib.sha256(solution).hexdigest() != result.patch.sha256:
                    raise TrajectoryError("artifact_drift", "solution digest")
                usage = None
                if "usage.json" in artifacts:
                    data = json.loads(
                        verified_artifact(directory, artifacts, "usage.json", 1048576)
                    )
                    if data.get("trust") != "protected-host":
                        raise TrajectoryError("artifact_drift", "usage provenance")
                    usage = TypeAdapter(TrustedUsage).validate_json(
                        canonical_json(data["usage"]), strict=True
                    )
                    if min(usage.requests, usage.input_tokens, usage.output_tokens) < 0:
                        raise TrajectoryError("artifact_drift", "usage amounts")
                trials.append(
                    VerifiedTrial(
                        trial_id,
                        str(trial_row["case_id"]),
                        cast("Literal[0, 1, 2]", trial_row["repeat_index"]),
                        result.solver.run_id,
                        result.solver.task_id,
                        cast('Literal["pass", "fail"]', result.grade.verdict),
                        cast(
                            Literal["completed", "budget_exhausted", "behavior_error"],
                            result.solver.termination,
                        ),
                        source,
                        EvidenceRef(
                            trial_id=trial_id,
                            artifact="trace",
                            sha256=hashlib.sha256(trace).hexdigest(),
                        ),
                        trace,
                        EvidenceRef(
                            trial_id=trial_id,
                            artifact="solution_diff",
                            sha256=hashlib.sha256(solution).hexdigest(),
                        ),
                        solution,
                        usage,
                    )
                )
            source_files = {
                p.relative_to(harness).as_posix(): (
                    0o100755 if p.stat().st_mode & 0o111 else 0o100644,
                    read_regular_artifact(p, 2 * 1048576),
                )
                for p in sorted(harness.rglob("*"))
                if p.is_file()
            }
            if _digest(source_files) != source.export_digest:
                raise TrajectoryError("artifact_drift", "source changed during read")
            return VerifiedDevelopmentInputs(
                tuple(trials),
                source,
                MappingProxyType(source_files),
                selection.trial_ids,
                experiment_id,
                snapshot_id,
                selection,
                (self.root, harness)
                + ((Path(run["worktree_path"]),) if run["worktree_path"] else ()),
            )
