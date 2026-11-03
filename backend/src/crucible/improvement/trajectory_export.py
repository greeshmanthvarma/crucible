"""Offline positive export with a sealed file registry and atomic publication."""

import hashlib
import json
import os
import re
import tempfile
from dataclasses import replace
from pathlib import Path
from uuid import UUID

from pydantic import TypeAdapter

from crucible.benchmarks.evidence import sanitize
from crucible.improvement.source import (
    MAX_EXPORT,
    MAX_FILE,
    _digest,
    safe_path,
    solver_runtime_path,
)
from crucible.improvement.trajectory import (
    TrajectoryError,
    build_trajectory,
    decode_solver_trace,
)
from crucible.improvement.trajectory_contracts import (
    DetailEntry,
    EvidencePackage,
    ExportLimits,
    TrajectoryRecord,
    TrialOverview,
    VerifiedDevelopmentInputs,
)
from crucible.improvement.verified_evidence import owned_artifact
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json, digest


def _clean(value: object, roots: tuple[Path, ...]) -> object:
    if isinstance(value, str):
        text = str(sanitize(value))
        for root in sorted(roots, key=lambda p: len(str(p)), reverse=True):
            text = text.replace(str(root), "[controller-state]")
        text = re.sub(
            r"(?i)(?:bearer\s+)[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", text
        )
        text = re.sub(
            r'(?i)(?:sk-[A-Za-z0-9_-]{12,}|(?:api[_-]?key|token|password|secret)\s*[=:]\s*[^\s,"\'}]+)',
            "[REDACTED]",
            text,
        )
        text = re.sub(
            r'(?:file://[^\s"\']+|/(?:Users|home|private|tmp|var)/[^\s"\']+)',
            "[host-path]",
            text,
        )
        return text
    if isinstance(value, dict):
        result = {
            str(_clean(str(k), roots)): (
                "[REDACTED]"
                if str(k).lower()
                in {
                    "api_key",
                    "apikey",
                    "password",
                    "secret",
                    "credential",
                    "authorization",
                    "access_token",
                    "refresh_token",
                }
                else _clean(v, roots)
            )
            for k, v in value.items()
        }
        if len(result) != len(value):
            raise TrajectoryError("identity_mismatch", "sanitized key collision")
        return result
    if isinstance(value, list):
        return [_clean(v, roots) for v in value]
    return value


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def export_trajectory_package(
    inputs: VerifiedDevelopmentInputs, output: Path, limits: ExportLimits
) -> EvidencePackage:
    try:
        return _export(inputs, output, limits)
    except TrajectoryError:
        raise
    except (ValueError, TypeError, KeyError, OSError, RecursionError) as error:
        raise TrajectoryError("artifact_drift", "package preparation failed") from error


def _export(
    inputs: VerifiedDevelopmentInputs, output: Path, limits: ExportLimits
) -> EvidencePackage:
    safe_path(output)
    if output.exists() or any(
        output.is_relative_to(root) or root.is_relative_to(output)
        for root in inputs.private_roots
    ):
        raise TrajectoryError(
            "unsafe_path", "output exists or overlaps private source/state"
        )
    if (
        tuple(t.trial_id for t in inputs.trials) != inputs.selected_trial_ids
        or inputs.selection.trial_ids != inputs.selected_trial_ids
        or not 1 <= len(inputs.trials) <= 3
        or len(set(inputs.selected_trial_ids)) != len(inputs.trials)
        or any(t.source != inputs.source for t in inputs.trials)
    ):
        raise TrajectoryError("identity_mismatch", "frozen package inputs")
    if _digest(dict(inputs.source_files)) != inputs.source.export_digest:
        raise TrajectoryError("artifact_drift", "source export digest")
    files: dict[str, bytes] = {}
    modes: dict[str, int] = {}
    overviews: dict[UUID, str] = {}
    locators = []
    # Decode every complete original before any derivation or publication.
    for trial in inputs.trials:
        if (
            trial.trace_ref.trial_id != trial.trial_id
            or trial.solution_ref.trial_id != trial.trial_id
            or trial.trace_ref.artifact != "trace"
            or trial.solution_ref.artifact != "solution_diff"
            or _sha(trial.trace_bytes) != trial.trace_ref.sha256
            or _sha(trial.solution_bytes) != trial.solution_ref.sha256
        ):
            raise TrajectoryError("artifact_drift", "original input digest")
        decode_solver_trace(
            trial.trace_bytes, trial=trial, maximum=limits.input_trace_bytes
        )
    for trial in inputs.trials:
        cleaned = canonical_json(
            _clean(json.loads(trial.trace_bytes), inputs.private_roots)
        )
        solution = str(
            _clean(trial.solution_bytes.decode("utf-8"), inputs.private_roots)
        ).encode("utf-8")
        clean_trial = replace(trial, trace_bytes=cleaned, solution_bytes=solution)
        trace = decode_solver_trace(
            cleaned, trial=clean_trial, maximum=limits.input_trace_bytes
        )
        bundle = build_trajectory(trace, clean_trial, limits)
        prefix = f"trials/{trial.trial_id}/"
        for name, raw in bundle.files.items():
            files[prefix + name] = raw
        overviews[trial.trial_id] = prefix + "overview.json"
        entries = TypeAdapter(tuple[DetailEntry, ...]).validate_json(
            bundle.files["details-manifest.json"], strict=True
        )
        for entry in entries:
            locators.append(
                {
                    "trial_id": trial.trial_id,
                    "original_artifact": entry.original_artifact,
                    "kind": entry.kind,
                    "original_record_id": entry.original_record_id,
                    "original_json_pointer": entry.original_json_pointer,
                    "detail_ref": prefix + entry.ref,
                    "detail_sha256": _sha(bundle.files[entry.ref]),
                    "truncated": entry.truncated,
                }
            )
    files["record-locator.json"] = canonical_json(locators)
    files["index.json"] = canonical_json(
        {
            "schema_version": 1,
            "source": inputs.source,
            "source_ref": "source/",
            "selected_trial_ids": inputs.selected_trial_ids,
            "trial_overview_refs": {str(k): v for k, v in overviews.items()},
            "record_locator_ref": "record-locator.json",
        }
    )
    for name, (mode, raw) in inputs.source_files.items():
        owned_artifact(output, "source/" + name)
        if (
            not solver_runtime_path(name)
            or mode not in (0o100644, 0o100755)
            or len(raw) > MAX_FILE
        ):
            raise TrajectoryError("unsafe_path", "source registry")
        files["source/" + name] = raw
        modes["source/" + name] = mode & 0o777
    if (
        sum(len(raw) for name, raw in files.items() if name.startswith("source/"))
        > MAX_EXPORT
    ):
        raise TrajectoryError("export_limit", "source export")
    package = EvidencePackage(
        schema_version=1,
        bundle_digest="0" * 64,
        source=inputs.source,
        source_experiment_id=inputs.source_experiment_id,
        source_snapshot_id=inputs.source_snapshot_id,
        selected_trial_ids=inputs.selected_trial_ids,
        selection=inputs.selection,
        limits=limits,
        files={name: _sha(raw) for name, raw in sorted(files.items())},
        trial_overview_refs=overviews,
        record_locator_ref="record-locator.json",
    )
    package = package.model_copy(
        update={
            "bundle_digest": digest(
                package.model_dump(mode="json", exclude={"bundle_digest"})
            )
        }
    )
    files["package-manifest.json"] = canonical_json(package)
    if (
        len(files["package-manifest.json"]) > limits.detail_bytes
        or any(
            len(raw) > limits.detail_bytes
            for name, raw in files.items()
            if name in ("index.json", "record-locator.json")
        )
        or sum(
            len(raw) for name, raw in files.items() if not name.startswith("source/")
        )
        > limits.evidence_bytes
    ):
        raise TrajectoryError("export_limit", "package total or registry")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=".incomplete-trajectory-", dir=output.parent)
    )
    for name, raw in files.items():
        if name == "package-manifest.json":
            continue
        path = owned_artifact(staging, name)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_bytes(raw)
        path.chmod(modes.get(name, 0o600))
    # A complete manifest is written last; failed staging is never admissible.
    (staging / "package-manifest.json").write_bytes(files["package-manifest.json"])
    try:
        verify_trajectory_package(staging)
        # Atomically claim the fresh destination before replacing our own empty
        # reservation. Competing exporters cannot pass this exclusive mkdir.
        # A failed/crashed publication leaves an incomplete empty reservation for
        # explicit operator cleanup; never remove a possibly populated output.
        output.mkdir(mode=0o700, exist_ok=False)
        os.rename(staging, output)
    except BaseException:
        (staging / "package-manifest.json").unlink(missing_ok=True)
        raise
    return package


def verify_trajectory_package(output: Path) -> EvidencePackage:
    try:
        return _verify(output)
    except TrajectoryError:
        raise
    except (ValueError, TypeError, KeyError, OSError, RecursionError) as error:
        raise TrajectoryError(
            "artifact_drift", "package verification failed"
        ) from error


def _verify(output: Path) -> EvidencePackage:
    safe_path(output)
    raw = read_regular_artifact(output / "package-manifest.json", 1048576)
    package = EvidencePackage.model_validate_json(raw)
    limits = package.limits
    if (
        package.bundle_digest
        != digest(package.model_dump(mode="json", exclude={"bundle_digest"}))
        or package.selected_trial_ids != package.selection.trial_ids
        or set(package.trial_overview_refs) != set(package.selected_trial_ids)
        or len(set(package.selected_trial_ids)) != len(package.selected_trial_ids)
        or package.record_locator_ref != "record-locator.json"
    ):
        raise TrajectoryError("artifact_drift", "package seal or selection")
    actual = set()
    for path in output.rglob("*"):
        safe_path(path)
        if not path.is_dir():
            actual.add(path.relative_to(output).as_posix())
    if (
        actual != set(package.files) | {"package-manifest.json"}
        or "package-manifest.json" in package.files
    ):
        raise TrajectoryError("artifact_drift", "file registry membership")
    files: dict[str, bytes] = {}
    source_files = {}
    total = len(raw)
    if len(raw) > limits.detail_bytes:
        raise TrajectoryError("export_limit", "package manifest")
    for name, sha in package.files.items():
        path = owned_artifact(output, name)
        maximum = MAX_FILE if name.startswith("source/") else limits.detail_bytes
        data = read_regular_artifact(path, maximum)
        if _sha(data) != sha:
            raise TrajectoryError("artifact_drift", "registered file digest")
        files[name] = data
        if name.startswith("source/"):
            relative = name.removeprefix("source/")
            if not solver_runtime_path(relative):
                raise TrajectoryError("unsafe_path", "source file")
            source_files[relative] = (
                0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                data,
            )
        else:
            total += len(data)
    if (
        total > limits.evidence_bytes
        or sum(len(v[1]) for v in source_files.values()) > MAX_EXPORT
    ):
        raise TrajectoryError("export_limit", "package total")
    if _digest(source_files) != package.source.export_digest:
        raise TrajectoryError("artifact_drift", "source bytes or modes")
    expected_locators = []
    expected_files = {"index.json", "record-locator.json"} | {
        "source/" + name for name in source_files
    }
    for trial_id in package.selected_trial_ids:
        prefix = f"trials/{trial_id}/"
        overview_name = prefix + "overview.json"
        if (
            package.trial_overview_refs[trial_id] != overview_name
            or len(files[overview_name]) > limits.overview_bytes
        ):
            raise TrajectoryError("artifact_drift", "overview reference")
        overview = TrialOverview.model_validate_json(files[overview_name])
        if (
            overview.trial_id != trial_id
            or overview.source != package.source
            or overview.index_ref != "trajectory.jsonl"
            or overview.details_manifest_ref != "details-manifest.json"
        ):
            raise TrajectoryError("identity_mismatch", "trial overview")
        index_raw = files[prefix + "trajectory.jsonl"]
        if len(index_raw) > limits.index_bytes or (
            index_raw and not index_raw.endswith(b"\n")
        ):
            raise TrajectoryError("export_limit", "trajectory index")
        records = []
        for line in index_raw.splitlines(keepends=True):
            if len(line) > limits.index_record_bytes:
                raise TrajectoryError("export_limit", "index record")
            records.append(TrajectoryRecord.model_validate_json(line))
        if (
            [r.sequence for r in records] != list(range(1, len(records) + 1))
            or len({r.record_id for r in records}) != len(records)
            or tuple(
                sum(r.kind == kind for r in records)
                for kind in ("message", "tool_call", "tool_result")
            )
            != (overview.message_count, overview.call_count, overview.result_count)
        ):
            raise TrajectoryError("identity_mismatch", "index accounting")
        entries = TypeAdapter(tuple[DetailEntry, ...]).validate_json(
            files[prefix + "details-manifest.json"], strict=True
        )
        refs = {e.ref for e in entries}
        if len(refs) != len(entries):
            raise TrajectoryError("identity_mismatch", "duplicate detail")
        by_ref = {e.ref: e for e in entries}
        for record in records:
            if record.detail_ref not in refs:
                raise TrajectoryError("artifact_drift", "unresolved detail")
            entry = by_ref[record.detail_ref]
            if (
                entry.original_record_id != record.record_id
                or entry.kind
                != {
                    "message": "message",
                    "tool_call": "arguments",
                    "tool_result": "result",
                }[record.kind]
            ):
                raise TrajectoryError("identity_mismatch", "record detail identity")
        for entry in entries:
            if (
                entry.original_artifact.trial_id != trial_id
                or entry.original_artifact.artifact not in ("trace", "solution_diff")
                or entry.files[0] != entry.ref
                or len(set(entry.files)) != len(entry.files)
            ):
                raise TrajectoryError("identity_mismatch", "detail provenance")
            data_files = [files[prefix + name] for name in entry.files]
            if (
                sum(map(len, data_files)) != entry.exported_bytes
                or entry.exported_bytes > limits.detail_bytes
            ):
                raise TrajectoryError("export_limit", "detail combined size")
            envelope = json.loads(files[prefix + entry.ref])
            if (
                envelope["kind"] != entry.kind
                or envelope["original_record_id"]
                != (str(entry.original_record_id) if entry.original_record_id else None)
                or envelope["content_sha256"] != entry.content_sha256
                or envelope["original_bytes"] != entry.original_bytes
                or envelope["truncated"] != entry.truncated
            ):
                raise TrajectoryError("identity_mismatch", "detail envelope")
            if not entry.truncated:
                content = canonical_json(envelope["content"])
                if (
                    len(content) != entry.original_bytes
                    or _sha(content) != entry.content_sha256
                ):
                    raise TrajectoryError("artifact_drift", "derived content digest")
            elif (
                len(entry.files) != 2
                or envelope["excerpt_ref"] != entry.files[1]
                or envelope["content"] is not None
            ):
                raise TrajectoryError("artifact_drift", "truncation reference")
            for name in entry.files:
                owned_artifact(output, prefix + name)
                expected_files.add(prefix + name)
            expected_locators.append(
                {
                    "trial_id": trial_id,
                    "original_artifact": entry.original_artifact,
                    "kind": entry.kind,
                    "original_record_id": entry.original_record_id,
                    "original_json_pointer": entry.original_json_pointer,
                    "detail_ref": prefix + entry.ref,
                    "detail_sha256": package.files[prefix + entry.ref],
                    "truncated": entry.truncated,
                }
            )
        if (
            overview.task_instruction_ref is not None
            and overview.task_instruction_ref not in refs
            or overview.completeness.truncated_detail_refs
            != tuple(e.ref for e in entries if e.truncated)
        ):
            raise TrajectoryError("artifact_drift", "overview detail references")
        expected_files.update(
            {
                overview_name,
                prefix + "trajectory.jsonl",
                prefix + "details-manifest.json",
            }
        )
    if set(files) != expected_files or files[
        package.record_locator_ref
    ] != canonical_json(expected_locators):
        raise TrajectoryError("artifact_drift", "record locator membership")
    expected_index = {
        "schema_version": 1,
        "source": package.source,
        "source_ref": "source/",
        "selected_trial_ids": package.selected_trial_ids,
        "trial_overview_refs": {
            str(k): v for k, v in package.trial_overview_refs.items()
        },
        "record_locator_ref": package.record_locator_ref,
    }
    if files["index.json"] != canonical_json(expected_index):
        raise TrajectoryError("artifact_drift", "package index")
    return package
