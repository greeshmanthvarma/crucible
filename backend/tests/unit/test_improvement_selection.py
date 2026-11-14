"""Promotion cannot trade an individual case regression for aggregate gain."""

from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.improvement.checks import CandidateCheckResult
from crucible.improvement.contracts import FrozenCaseRef
from crucible.improvement.selection import EvaluationSet, ScoredTrial, decide_candidate


def evals(dev, validation, *, candidate=False, repeats=3):
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
            for repeat in range(repeats)
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


def test_selection_counts_requires_exact_repeats():
    from crucible.improvement.selection import selection_counts

    six = evals([4], [5], repeats=6)
    assert selection_counts(six, repeats=6) == {"dev-0": 4, "HIDDEN-0": 5}
    assert selection_counts(six) is None
    assert selection_counts(evals([2], [2]), repeats=6) is None


def pooled_reference(dev, validation):
    base = evals([0] * len(dev), [0] * len(validation), repeats=6)
    counts = {
        case.case_id: count
        for case, count in zip(base.required_cases, (*dev, *validation), strict=True)
    }
    return base.model_copy(
        update={
            "trials": tuple(
                t.model_copy(
                    update={
                        "verdict": "pass"
                        if t.repeat_index % 3 < counts[t.case_id][t.repeat_index // 3]
                        else "fail"
                    }
                )
                for t in base.trials
            )
        }
    )


@pytest.mark.parametrize(
    "total,verdict,reason",
    [(19, "promote", "pooled_margin_gain"), (18, "reject", "insufficient_gain")],
)
def test_pooled_decision_boundaries(total, verdict, reason):
    from crucible.improvement.selection import decide_pooled_candidate, noise_margin

    reference = pooled_reference([(1, 2)] * 5, [(2, 2), (2, 2), (2, 1), (2, 1), (1, 1)])
    assert sum(t.verdict == "pass" for t in reference.trials) == 31
    margin = noise_margin(reference)
    assert (margin.half_a, margin.half_b, margin.margin) == (14, 17, 3)
    candidate = evals([2] * 5, [2] * 4 + [total - 18], candidate=True)
    decision = decide_pooled_candidate(reference, candidate, checks_pass(), margin)
    assert (decision.verdict, decision.reason) == (verdict, reason)


@pytest.mark.parametrize(
    "a,b,margin,noisy",
    [(15, 15, 2, False), (14, 15, 2, False), (14, 17, 3, False), (12, 18, 4, True)],
)
def test_noise_margin_clamps_and_flags(a, b, margin, noisy):
    from crucible.improvement.selection import noise_margin

    halves = [(min(3, max(0, a - 3 * i)), min(3, max(0, b - 3 * i))) for i in range(10)]
    reference = pooled_reference(halves[:5], halves[5:])
    result = noise_margin(reference)
    assert (result.half_a, result.half_b, result.gap, result.margin, result.noisy) == (
        a,
        b,
        abs(a - b),
        margin,
        noisy,
    )
    assert (
        noise_margin(reference.model_copy(update={"trials": reference.trials[:-1]}))
        is None
    )


@pytest.mark.parametrize(
    "count,reason", [(6, "case_regression"), (3, "insufficient_gain")]
)
def test_pooled_rejects_drop_of_two_allows_one_and_a_half(count, reason):
    from crucible.improvement.selection import decide_pooled_candidate, noise_margin

    reference = pooled_reference([(min(count, 3), max(count - 3, 0))], [(0, 0)])
    result = decide_pooled_candidate(
        reference,
        evals([1 if count == 6 else 0], [0], candidate=True),
        checks_pass(),
        noise_margin(reference),
    )
    assert result.reason == reason


@pytest.mark.parametrize(
    "val,reason", [(1, "validation_regression"), (2, "pooled_margin_gain")]
)
def test_pooled_rejects_validation_loss_allows_equal(val, reason):
    from crucible.improvement.selection import decide_pooled_candidate, noise_margin

    reference = pooled_reference([(0, 0)] * 2, [(2, 2)])
    result = decide_pooled_candidate(
        reference,
        evals([3, 3], [val], candidate=True),
        checks_pass(),
        noise_margin(reference),
    )
    assert result.reason == reason


def test_per_case_decisions_unchanged():
    import sqlite3
    from pathlib import Path

    from crucible.improvement.roles import SelectionDecision
    from crucible.repair.identity import digest

    database = Path("../data/self-improvement/full-run-v1/runtime/experiments.sqlite3")
    if not database.exists():
        pytest.skip("private v1 ledger absent")
    with sqlite3.connect(
        database.resolve().as_uri() + "?mode=ro&immutable=1", uri=True
    ) as connection:
        rows = connection.execute(
            "SELECT decision_ref, decision_digest FROM experiment_rounds "
            "WHERE decision_ref IS NOT NULL"
        ).fetchall()
    assert rows
    for raw, expected in rows:
        decision = SelectionDecision.model_validate_json(raw)
        assert digest(decision) == expected


def test_six_repeat_scoring_accepts_maximum_manifest_case_count():
    from crucible.improvement.selection import selection_counts

    evaluation = evals([6] * 2048, [6] * 2048, repeats=6)
    counts = selection_counts(evaluation, 6)
    assert counts is not None and len(counts) == 4096
    assert set(counts.values()) == {6}
