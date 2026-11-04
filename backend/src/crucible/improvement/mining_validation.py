"""Host-only mechanical report validation against verified frozen exports.

The snapshot is loaded by the existing exporter verifier. It never parses a
historical trace or imports an old ledger. Receipts attest returned bytes under
the protected harness, not the truth or causal strength of model claims.
"""

import hashlib
from pathlib import Path
from typing import Literal, Mapping
from uuid import UUID

from pydantic import PrivateAttr, TypeAdapter

from crucible.improvement.mining_contracts import (
    AnalystAssignment,
    InspectionReceipt,
    RecordCitation,
    SourceCitation,
    SynthesisReport,
    TrialAnalysisReport,
)
from crucible.improvement.mining_inspection import line_span
from crucible.improvement.trajectory_contracts import DetailEntry, EvidencePackage
from crucible.improvement.trajectory_export import verify_trajectory_package
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json

ValidationCode = Literal[
    "invalid_report",
    "identity_mismatch",
    "invalid_citation",
    "uninspected_support",
    "report_limit",
    "incomplete_analysis",
]


class MiningValidationError(ValueError):
    def __init__(self, code: ValidationCode) -> None:
        self.code = code
        super().__init__(code)


class InspectionPackage(EvidencePackage):
    """Same package wire shape, bound host-locally to bytes and primary job."""

    _files: dict[str, bytes] = PrivateAttr()
    _job_id: UUID = PrivateAttr()
    _entries: dict[str, DetailEntry] = PrivateAttr()
    _workspace_files: dict[str, bytes] = PrivateAttr()


def load_inspection_package(
    root: Path,
    *,
    job_id: UUID,
    workspace_files: Mapping[str, bytes] | None = None,
) -> EvidencePackage:
    metadata = verify_trajectory_package(root)
    package = InspectionPackage.model_validate_json(metadata.model_dump_json())
    files = {}
    for name, expected in package.files.items():
        raw = read_regular_artifact(root / name, 2 * 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise MiningValidationError("invalid_citation")
        files[name] = raw
    entries = {}
    for trial in package.selected_trial_ids:
        prefix = f"trials/{trial}/"
        for entry in TypeAdapter(tuple[DetailEntry, ...]).validate_json(
            files[prefix + "details-manifest.json"], strict=True
        ):
            entries[prefix + entry.ref] = entry
    package._files, package._job_id, package._entries = files, job_id, entries
    # Input, compact reports and catalogs are job-view artifacts, not support.
    # The coordinator supplies their frozen positive registry when they are read.
    package._workspace_files = dict(workspace_files or {})
    return package


def _snapshot(package: EvidencePackage) -> InspectionPackage:
    if not isinstance(package, InspectionPackage):
        raise MiningValidationError("invalid_citation")
    return package


def _receipts(
    package: InspectionPackage,
    receipts: tuple[InspectionReceipt, ...],
    trial_id: UUID | None = None,
) -> None:
    if len(receipts) > 4096 or len(canonical_json(receipts)) > 1_048_576:
        raise MiningValidationError("report_limit")
    for receipt in receipts:
        try:
            InspectionReceipt.model_validate_json(receipt.model_dump_json())
        except ValueError:
            raise MiningValidationError("invalid_citation") from None
        if receipt.job_id != package._job_id:
            raise MiningValidationError("identity_mismatch")
        path = receipt.path
        if path.startswith("trial/"):
            if trial_id is None:
                raise MiningValidationError("invalid_citation")
            path = f"trials/{trial_id}/" + path.removeprefix("trial/")
        if (
            trial_id is not None
            and path.startswith("trials/")
            and not path.startswith(f"trials/{trial_id}/")
        ):
            raise MiningValidationError("invalid_citation")
        if (
            trial_id is not None
            and path != "input.json"
            and not path.startswith(("source/", "catalog/", f"trials/{trial_id}/"))
        ):
            raise MiningValidationError("invalid_citation")
        raw = package._files.get(path)
        if raw is None and (
            path == "input.json" or path.startswith(("reports/", "catalog/"))
        ):
            raw = package._workspace_files.get(path)
        if (
            raw is None
            or hashlib.sha256(raw).hexdigest() != receipt.sha256
            or receipt.end_byte > len(raw)
        ):
            raise MiningValidationError("invalid_citation")


def _covered(
    package: InspectionPackage,
    path: str,
    sha: str,
    start: int,
    end: int,
    receipts: tuple[InspectionReceipt, ...],
    trial_id: UUID | None = None,
) -> None:
    aliases = {path}
    if trial_id is not None:
        aliases.add("trial/" + path.removeprefix(f"trials/{trial_id}/"))
    cursor = start
    for receipt in sorted(receipts, key=lambda r: r.start_byte):
        if receipt.path not in aliases or receipt.sha256 != sha:
            continue
        if receipt.end_byte > len(package._files[path]):
            raise MiningValidationError("invalid_citation")
        if receipt.start_byte <= cursor:
            cursor = max(cursor, receipt.end_byte)
        if cursor >= end:
            return
    raise MiningValidationError("uninspected_support")


def _record(
    citation: RecordCitation,
    package: InspectionPackage,
    receipts: tuple[InspectionReceipt, ...],
    trial_id: UUID | None,
) -> None:
    trial = citation.original.trial_id
    if trial not in package.selected_trial_ids or (
        trial_id is not None and trial != trial_id
    ):
        raise MiningValidationError("invalid_citation")
    entry = package._entries.get(citation.record_key)
    kinds = {
        "message": "message",
        "task_instruction": "message",
        "arguments": "tool_call",
        "result": "tool_result",
        "solution_diff": "solution_diff",
    }
    if (
        entry is None
        or entry.original_artifact != citation.original
        or entry.original_record_id != citation.record_id
        or kinds[entry.kind] != citation.record_kind
    ):
        raise MiningValidationError("invalid_citation")
    prefix = f"trials/{trial}/"
    # A truncated envelope describes missing bytes; only its retained excerpt
    # can supply original content support. Never treat missing bytes as read.
    candidates = entry.files[1:] if entry.truncated else entry.files
    paths = [
        prefix + name
        for name in candidates
        if package.files.get(prefix + name) == citation.derived_sha256
    ]
    if not paths or citation.end_byte > len(package._files[paths[0]]):
        raise MiningValidationError("invalid_citation")
    _covered(
        package,
        paths[0],
        citation.derived_sha256,
        citation.start_byte,
        citation.end_byte,
        receipts,
        trial,
    )


def _source(
    citation: SourceCitation,
    package: InspectionPackage,
    receipts: tuple[InspectionReceipt, ...],
) -> None:
    path = "source/" + citation.path
    raw = package._files.get(path)
    if raw is None or package.files[path] != citation.sha256:
        raise MiningValidationError("invalid_citation")
    start, end = line_span(raw, citation.start_line, citation.end_line)
    # An end line beyond EOF must not be clamped into a valid citation.
    last_start, last_end = line_span(raw, citation.end_line, citation.end_line)
    if start == end or last_start == last_end:
        raise MiningValidationError("invalid_citation")
    _covered(package, path, citation.sha256, start, end, receipts)


def validate_analysis_report(
    raw: bytes,
    assignment: AnalystAssignment,
    package: EvidencePackage,
    receipts: tuple[InspectionReceipt, ...],
) -> TrialAnalysisReport:
    if len(raw) > 16384:
        raise MiningValidationError("report_limit")
    try:
        report = TrialAnalysisReport.model_validate_json(raw)
    except ValueError:
        raise MiningValidationError("invalid_report") from None
    if (
        report.trial_id != assignment.trial_id
        or assignment.trial_id not in package.selected_trial_ids
        or report.package_digest != assignment.package_digest
        or report.package_digest != package.bundle_digest
        or report.investigation_source_digest != assignment.investigation_source_digest
        or report.investigation_source_digest != package.source.source_digest
    ):
        raise MiningValidationError("identity_mismatch")
    bound = _snapshot(package)
    _receipts(bound, receipts, assignment.trial_id)
    for observation in report.observations:
        for citation in observation.evidence + observation.contradictory_evidence:
            _record(citation, bound, receipts, assignment.trial_id)
        for source in observation.source:
            _source(source, bound, receipts)
    # Preserve a structurally valid incomplete report for the caller to record;
    # investigation_complete=False is explicitly ineligible for synthesis.
    return report


def validate_synthesis_report(
    raw: bytes, package: EvidencePackage, receipts: tuple[InspectionReceipt, ...]
) -> SynthesisReport:
    if len(raw) > 1_048_576:
        raise MiningValidationError("report_limit")
    try:
        report = SynthesisReport.model_validate_json(raw)
    except ValueError:
        raise MiningValidationError("invalid_report") from None
    if report.package_digest != package.bundle_digest:
        raise MiningValidationError("identity_mismatch")
    bound = _snapshot(package)
    _receipts(bound, receipts)
    for support in report.support:
        for citation in support.evidence:
            _record(citation, bound, receipts, None)
        for source in support.source:
            _source(source, bound, receipts)
    return report
