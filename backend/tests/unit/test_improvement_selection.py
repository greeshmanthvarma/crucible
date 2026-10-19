"""Promotion cannot trade an individual case regression for aggregate gain."""

from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.improvement.checks import CandidateCheckResult
from crucible.improvement.contracts import FrozenCaseRef
from crucible.improvement.selection import EvaluationSet, ScoredTrial, decide_candidate


def evals(dev, validation, *, candidate=False):
    cases = tuple(
        FrozenCaseRef(
            case_id=f"dev-{i}",
            split="development",
            case_digest="a" * 64,
            admission_digest="b" * 64,
        )
        for i in range(len(dev))
    ) + tuple(
        FrozenCaseRef(
            case_id=f"HIDDEN-{i}",
            split="selection-validation",
            case_digest="c" * 64,
            admission_digest="d" * 64,
        )
        for i in range(len(validation))
    )
    counts = (*dev, *validation)
    return EvaluationSet(
        source_digest=("e" if candidate else "a") * 64,
        export_digest=("f" if candidate else "b") * 64,
        config_policy_digest="c" * 64,
        required_cases=cases,
        trials=tuple(
            ScoredTrial(
                case_id=case.case_id,
                split=case.split,
                repeat_index=repeat,
                original_trial_id=uuid4(),
                verdict="pass" if repeat < count else "fail",
                evidence_digest="d" * 64,
            )
            for case, count in zip(cases, counts, strict=True)
            for repeat in range(3)
        ),
    )


def checks_pass():
    return CandidateCheckResult(
        "e" * 64, "f" * 64, "a" * 64, "b" * 64, "pass", "checks_passed", "c" * 64, True
    )


def test_one_case_regression_rejects_aggregate_gain():
    decision = decide_candidate(
        evals([1, 2], [2]), evals([3, 1], [3], candidate=True), checks_pass()
    )
    assert decision.verdict == "reject"
    assert decision.reason == "case_regression"


@pytest.mark.parametrize(
    "before,after,verdict,reason",
    [
        (([0], [2]), ([1], [2]), "promote", "strict_development_gain"),
        (([1], [2]), ([1], [3]), "reject", "no_development_gain"),
        (([2], [3]), ([2], [3]), "reject", "no_development_gain"),
        (([1], [2]), ([3], [1]), "reject", "case_regression"),
    ],
)
def test_per_case_counts(before, after, verdict, reason):
    decision = decide_candidate(
        evals(*before), evals(*after, candidate=True), checks_pass()
    )
    assert (decision.verdict, decision.reason) == (verdict, reason)


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "duplicate",
        "error",
        "interrupted",
        "config",
        "boundary_source",
        "same_trial_other_source",
        "missing_case_both",
        "different_schedule",
    ],
)
def test_missing_or_error_trial_is_inconclusive(corruption):
    baseline, candidate, checks = (
        evals([1, 1], [2]),
        evals([2, 1], [2], candidate=True),
        checks_pass(),
    )
    if corruption == "missing":
        candidate = candidate.model_copy(update={"trials": candidate.trials[:-1]})
    elif corruption == "duplicate":
        candidate = candidate.model_copy(
            update={"trials": (*candidate.trials[:-1], candidate.trials[0])}
        )
    elif corruption in ("error", "interrupted"):
        candidate = candidate.model_copy(
            update={
                "trials": (
                    candidate.trials[0].model_copy(update={"verdict": corruption}),
                    *candidate.trials[1:],
                )
            }
        )
    elif corruption == "config":
        candidate = candidate.model_copy(update={"config_policy_digest": "f" * 64})
    elif corruption == "boundary_source":
        checks = replace(checks, snapshot_digest="c" * 64)
    elif corruption == "same_trial_other_source":
        candidate = candidate.model_copy(
            update={
                "trials": (
                    candidate.trials[0].model_copy(
                        update={
                            "original_trial_id": baseline.trials[0].original_trial_id
                        }
                    ),
                    *candidate.trials[1:],
                )
            }
        )
    elif corruption == "missing_case_both":
        baseline = baseline.model_copy(
            update={"trials": tuple(t for t in baseline.trials if t.case_id != "dev-1")}
        )
        candidate = candidate.model_copy(
            update={
                "trials": tuple(t for t in candidate.trials if t.case_id != "dev-1")
            }
        )
    elif corruption == "different_schedule":
        candidate = candidate.model_copy(
            update={
                "required_cases": (
                    *candidate.required_cases[:-1],
                    candidate.required_cases[-1].model_copy(
                        update={"admission_digest": "f" * 64}
                    ),
                )
            }
        )
    assert decide_candidate(baseline, candidate, checks).verdict == "inconclusive"


@pytest.mark.parametrize(
    "verdict,cleanup,expected",
    [
        ("reject", True, "reject"),
        ("error", True, "inconclusive"),
        ("pass", False, "inconclusive"),
    ],
)
def test_protected_boundary_is_required(verdict, cleanup, expected):
    checks = replace(checks_pass(), verdict=verdict, cleanup_confirmed=cleanup)
    assert (
        decide_candidate(
            evals([0], [1]), evals([3], [3], candidate=True), checks
        ).verdict
        == expected
    )


def test_final_cases_forbidden_in_selection():
    baseline, candidate = evals([1], [1]), evals([2], [1], candidate=True)
    final = baseline.required_cases[-1].model_copy(update={"split": "final-test"})
    baseline = baseline.model_copy(
        update={"required_cases": (*baseline.required_cases[:-1], final)}
    )
    candidate = candidate.model_copy(update={"required_cases": baseline.required_cases})
    assert (
        decide_candidate(baseline, candidate, checks_pass()).verdict == "inconclusive"
    )


def test_feedback_contains_no_hidden_counts_or_identities():
    from crucible.improvement.selection import role_feedback

    decision = decide_candidate(
        evals([0], [3]), evals([1], [2], candidate=True), checks_pass()
    )
    feedback = role_feedback(decision)
    assert feedback.hidden_gate_rejected
    assert "HIDDEN" not in feedback.model_dump_json()
    assert len(feedback.development) == 1
    assert feedback.development[0].candidate == 1


def test_reused_trial_cannot_change_its_evidence_or_verdict():
    baseline = evals([1], [1])
    candidate = baseline.model_copy(
        update={
            "trials": (
                baseline.trials[0].model_copy(update={"verdict": "fail"}),
                *baseline.trials[1:],
            )
        }
    )
    checks = replace(
        checks_pass(),
        snapshot_digest=baseline.source_digest,
        export_digest=baseline.export_digest,
    )
    assert decide_candidate(baseline, candidate, checks).verdict == "inconclusive"
