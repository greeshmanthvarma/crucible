"""Data-only diagnostic inputs and reports, safe for isolated worker imports.

Membership, frozen hashes, retained byte bounds and successful inspection are
host validation concerns; structural validity alone is not evidence validation.
"""

from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from crucible.benchmarks.contracts import Sha256, TrialLimits
from crucible.improvement.contracts import (
    Contract,
    MiningDiagnosticManifest,
    Nonnegative,
    SourceIdentity,
    UsageAmounts,
)
from crucible.improvement.roles import (
    EvidenceRef,
    Identifier,
    MiningResult,
    Risk,
    Summary,
)


def workspace_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or any(p in ("", ".", "..") for p in value.split("/"))
        or "\\" in value
        or ":" in value
        or "\x00" in value
    ):
        raise ValueError("expected a workspace-relative path")
    return value


class MiningDiagnosticConfig(Contract):
    schema_version: Literal[1]
    repository: Path
    package_dir: Path
    manifest: MiningDiagnosticManifest


class PreparedMiningDiagnostic(Contract):
    diagnostic_id: UUID
    config_digest: Sha256
    package_digest: Sha256
    executing_source_digest: Sha256
    root: Path


class AnalystAssignment(Contract):
    slot_id: UUID
    trial_id: UUID
    package_digest: Sha256
    investigation_source_digest: Sha256

    def validate_manifest(self, manifest: MiningDiagnosticManifest) -> None:
        if (
            self.trial_id not in manifest.selected_trial_ids
            or self.package_digest != manifest.package_digest
            or self.investigation_source_digest
            != manifest.investigation_source.source_digest
        ):
            raise ValueError("assignment outside diagnostic manifest")


class ValidatedAnalysisRef(Contract):
    trial_id: UUID
    path: Annotated[str, Field(max_length=512)]
    sha256: Sha256

    _path = field_validator("path")(workspace_path)


class SynthesisAssignment(Contract):
    slot_id: UUID
    selected_trial_ids: tuple[UUID, UUID, UUID]
    package_digest: Sha256
    investigation_source_digest: Sha256
    reports: tuple[ValidatedAnalysisRef, ValidatedAnalysisRef, ValidatedAnalysisRef]

    @model_validator(mode="after")
    def selected_reports(self) -> Self:
        if (
            len(set(self.selected_trial_ids)) != 3
            or tuple(r.trial_id for r in self.reports) != self.selected_trial_ids
        ):
            raise ValueError(
                "synthesis reports must match distinct selected trials in order"
            )
        if len({r.path for r in self.reports}) != 3:
            raise ValueError("duplicate synthesis report path")
        return self

    def validate_manifest(self, manifest: MiningDiagnosticManifest) -> None:
        if (
            self.selected_trial_ids != manifest.selected_trial_ids
            or self.package_digest != manifest.package_digest
            or self.investigation_source_digest
            != manifest.investigation_source.source_digest
        ):
            raise ValueError("assignment outside diagnostic manifest")


class RecordCitation(Contract):
    original: EvidenceRef
    record_kind: Literal["message", "tool_call", "tool_result", "solution_diff"]
    record_id: UUID | None
    record_key: Annotated[str, Field(min_length=1, max_length=512)]
    derived_sha256: Sha256
    start_byte: Nonnegative
    end_byte: Annotated[int, Field(gt=0, le=2**53)]

    @model_validator(mode="after")
    def record_span(self) -> Self:
        if self.end_byte <= self.start_byte:
            raise ValueError("empty or reversed record span")
        if (self.record_id is None) != (self.record_kind == "solution_diff"):
            raise ValueError("record ID required except for whole solution diff")
        expected = "solution_diff" if self.record_kind == "solution_diff" else "trace"
        if self.original.artifact != expected:
            raise ValueError("record kind and original artifact disagree")
        return self


class SourceCitation(Contract):
    path: Annotated[str, Field(max_length=512)]
    sha256: Sha256
    start_line: Annotated[int, Field(ge=1)]
    end_line: Annotated[int, Field(ge=1)]

    _path = field_validator("path")(workspace_path)

    @model_validator(mode="after")
    def line_span(self) -> Self:
        if self.end_line < self.start_line:
            raise ValueError("reversed source line span")
        return self


class AnalysisObservation(Contract):
    id: Identifier
    observed_behavior: Summary
    evidence: Annotated[tuple[RecordCitation, ...], Field(min_length=1, max_length=8)]
    hypothesized_mechanism: Summary | None
    alternatives: Annotated[tuple[Risk, ...], Field(max_length=4)]
    contradictory_evidence: Annotated[tuple[RecordCitation, ...], Field(max_length=8)]
    source: Annotated[tuple[SourceCitation, ...], Field(max_length=8)]
    limitations: Annotated[str, Field(max_length=2000)]


class TrialAnalysisReport(Contract):
    schema_version: Literal[1]
    trial_id: UUID
    package_digest: Sha256
    investigation_source_digest: Sha256
    investigation_complete: bool
    summary: Summary
    observations: Annotated[tuple[AnalysisObservation, ...], Field(max_length=5)]
    open_questions: Annotated[tuple[Risk, ...], Field(max_length=8)]
    limitations: Annotated[tuple[Risk, ...], Field(max_length=8)]


class WeaknessSupport(Contract):
    weakness_id: Identifier
    evidence: Annotated[tuple[RecordCitation, ...], Field(min_length=1, max_length=32)]
    source: Annotated[tuple[SourceCitation, ...], Field(min_length=1, max_length=8)]


class SynthesisReport(Contract):
    schema_version: Literal[1]
    package_digest: Sha256
    result: MiningResult
    support: Annotated[tuple[WeaknessSupport, ...], Field(max_length=5)]
    limitations: Annotated[tuple[Risk, ...], Field(max_length=8)]

    @model_validator(mode="after")
    def exact_support(self) -> Self:
        by_id = {s.weakness_id: s for s in self.support}
        if len(by_id) != len(self.support) or set(by_id) != {
            w.id for w in self.result.weaknesses
        }:
            raise ValueError("support must match weakness IDs exactly")
        for weakness in self.result.weaknesses:
            if set(c.original for c in by_id[weakness.id].evidence) != set(
                weakness.evidence
            ):
                raise ValueError("support original refs must match weakness evidence")
        return self


class InspectionReceipt(Contract):
    job_id: UUID
    path: Annotated[str, Field(max_length=512)]
    sha256: Sha256
    start_byte: Nonnegative
    end_byte: Nonnegative
    tool_call_id: UUID

    _path = field_validator("path")(workspace_path)

    @model_validator(mode="after")
    def byte_span(self) -> Self:
        if self.end_byte < self.start_byte:
            raise ValueError("reversed inspection span")
        return self


SlotStatus = Literal[
    "pending",
    "running",
    "complete",
    "invalid",
    "incomplete",
    "budget_exhausted",
    "infrastructure_error",
    "interrupted",
    "correcting",
]


class MiningAttemptStatus(Contract):
    attempt_id: UUID
    job_id: UUID
    kind: Literal["primary", "format_correction"]
    status: SlotStatus
    error_code: Identifier | None


class MiningSlotStatus(Contract):
    slot_id: UUID
    ordinal: Annotated[int, Field(ge=0, le=3)]
    kind: Literal["trial_analysis", "mining_synthesis"]
    trial_id: UUID | None
    status: SlotStatus
    attempts: tuple[MiningAttemptStatus, ...]
    limits: TrialLimits
    spent: UsageAmounts
    reserved: UsageAmounts
    retained: UsageAmounts
    remaining: UsageAmounts


class MiningDiagnosticStatus(Contract):
    diagnostic_id: UUID
    status: Literal["ready", "running", "blocked", "interrupted", "complete", "closed"]
    phase: Literal["prepared", "analyzing", "synthesizing", "finished"]
    selected_trial_ids: tuple[UUID, UUID, UUID]
    slots: tuple[MiningSlotStatus, ...]
    next_action: Annotated[str, Field(max_length=200)]
    final_disposition: (
        Literal[
            "actionable",
            "no_actionable_weakness",
            "insufficient_evidence",
            "out_of_scope",
        ]
        | None
    )
    cleanup_confirmed: bool
    config_digest: Sha256
    investigation_source: SourceIdentity
    executing_source: SourceIdentity
