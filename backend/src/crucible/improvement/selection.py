"""Pure, complete-schedule promotion policy. No model or filesystem authority."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from crucible.benchmarks.contracts import Sha256
from crucible.improvement.checks import CandidateCheckResult
from crucible.improvement.contracts import Contract, FrozenCaseRef
from crucible.improvement.roles import CaseCount, SelectionDecision, role_feedback
from crucible.repair.identity import digest

__all__ = ["EvaluationSet", "ScoredTrial", "decide_candidate", "role_feedback"]


class ScoredTrial(Contract):
    case_id: Annotated[str, Field(min_length=1, max_length=200)]
    split: Literal["development", "selection-validation", "final-test"]
    repeat_index: Annotated[int, Field(ge=0, le=2)]
    original_trial_id: UUID
    verdict: Literal["pass", "fail", "error", "interrupted"]
    evidence_digest: Sha256


class EvaluationSet(Contract):
    source_digest: Sha256
    export_digest: Sha256
    config_policy_digest: Sha256
    required_cases: Annotated[tuple[FrozenCaseRef, ...], Field(max_length=4096)]
    trials: Annotated[tuple[ScoredTrial, ...], Field(max_length=12288)]


def selection_counts(evaluation: EvaluationSet) -> dict[str, int] | None:
    required = {case.case_id: case for case in evaluation.required_cases}
    if len(required) != len(evaluation.required_cases) or {
        case.split for case in required.values()
    } != {"development", "selection-validation"}:
        return None
    expected = {(case_id, repeat) for case_id in required for repeat in range(3)}
    observed: set[tuple[str, int]] = set()
    trial_ids: set[UUID] = set()
    counts = dict.fromkeys(required, 0)
    for trial in evaluation.trials:
        key = trial.case_id, trial.repeat_index
        case = required.get(trial.case_id)
        if (
            key not in expected
            or key in observed
            or trial.original_trial_id in trial_ids
            or case is None
            or trial.split != case.split
            or trial.verdict not in ("pass", "fail")
        ):
            return None
        observed.add(key)
        trial_ids.add(trial.original_trial_id)
        counts[trial.case_id] += trial.verdict == "pass"
    return counts if observed == expected else None


def decide_candidate(
    baseline: EvaluationSet, candidate: EvaluationSet, checks: CandidateCheckResult
) -> SelectionDecision:
    """Only verified host evidence may construct these inputs."""
    baseline = EvaluationSet.model_validate_json(baseline.model_dump_json())
    candidate = EvaluationSet.model_validate_json(candidate.model_dump_json())
    common = dict(
        schema_version=1,
        baseline_source_digest=baseline.source_digest,
        baseline_export_digest=baseline.export_digest,
        candidate_source_digest=candidate.source_digest,
        candidate_export_digest=candidate.export_digest,
        trial_ids=tuple(
            t.original_trial_id for t in (*baseline.trials, *candidate.trials)
        ),
        boundary_result_digest=digest(checks),
        policy_digest=baseline.config_policy_digest,
    )

    def decision(
        verdict: str, reason: str, counts: tuple[CaseCount, ...] = ()
    ) -> SelectionDecision:
        return SelectionDecision.model_validate(
            {**common, "verdict": verdict, "reason": reason, "counts": counts}
        )

    if (checks.snapshot_digest, checks.export_digest) != (
        candidate.source_digest,
        candidate.export_digest,
    ):
        return decision("inconclusive", "incomplete_evidence")
    if checks.verdict == "error" or not checks.cleanup_confirmed:
        return decision("inconclusive", "infrastructure_error")
    if checks.verdict != "pass":
        return decision(
            "reject",
            "scope_rejected" if "scope" in checks.reason else "boundary_rejected",
        )
    baseline_counts, candidate_counts = (
        selection_counts(baseline),
        selection_counts(candidate),
    )
    if (
        baseline_counts is None
        or candidate_counts is None
        or baseline.config_policy_digest != candidate.config_policy_digest
        or {c.case_id: c for c in baseline.required_cases}
        != {c.case_id: c for c in candidate.required_cases}
    ):
        return decision("inconclusive", "incomplete_evidence")
    shared_ids = {t.original_trial_id for t in baseline.trials} & {
        t.original_trial_id for t in candidate.trials
    }
    before_ids = {t.original_trial_id: t for t in baseline.trials}
    after_ids = {t.original_trial_id: t for t in candidate.trials}
    if shared_ids and (
        (baseline.source_digest, baseline.export_digest)
        != (candidate.source_digest, candidate.export_digest)
        or any(before_ids[id] != after_ids[id] for id in shared_ids)
    ):
        return decision("inconclusive", "incomplete_evidence")
    counts = tuple(
        CaseCount(
            case_id=case.case_id,
            split=case.split,
            baseline=baseline_counts[case.case_id],
            candidate=candidate_counts[case.case_id],
        )
        for case in sorted(baseline.required_cases, key=lambda c: c.case_id)
        if case.split != "final-test"
    )
    if any(c.candidate < c.baseline for c in counts):
        return decision("reject", "case_regression", counts)
    if (baseline.source_digest, baseline.export_digest) == (
        candidate.source_digest,
        candidate.export_digest,
    ):
        return decision("reject", "no_change", counts)
    if any(c.split == "development" and c.candidate > c.baseline for c in counts):
        return decision("promote", "strict_development_gain", counts)
    return decision("reject", "no_development_gain", counts)
