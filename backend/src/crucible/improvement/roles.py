"""Bounded data contracts for advisory development analysis and proposals."""

from typing import Annotated, Literal, Self
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
