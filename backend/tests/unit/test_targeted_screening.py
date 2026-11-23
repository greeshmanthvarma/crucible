import json
from dataclasses import replace

import pytest

from scripts.targeted_screening import (
    Classification,
    ScreeningTrial,
    classification_record,
    classify,
    pick,
)


@pytest.fixture
def failed_trial() -> ScreeningTrial:
    return ScreeningTrial(
        case_id="sympy__sympy-1",
        project="sympy",
        verdict="fail",
        termination="completed",
        read_paths=(),
        commands=(("pytest",),),
        tool_error_count=0,
        patch_files=(),
        reference_files=(),
    )


def test_fail_without_signatures_is_ineligible(failed_trial: ScreeningTrial) -> None:
    result = classify(failed_trial)
    assert result.signatures == ()
    assert not result.eligible
    assert result.reason == "no_signature"


def test_repeated_reads_threshold_matches_miner(failed_trial: ScreeningTrial) -> None:
    two_repeats = replace(failed_trial, read_paths=("a", "b", "a", "b"))
    three_repeats = replace(two_repeats, read_paths=(*two_repeats.read_paths, "a"))
    assert classify(two_repeats).signatures == ()
    result = classify(three_repeats)
    assert result.signatures == ("repeated_reads",)
    assert result.eligible
    assert result.reason == "targeted"


@pytest.mark.parametrize(
    ("argv", "has_test_run"),
    [
        (("python", "-m", "pytest"), True),
        (("pytest",), True),
        (("/bin/py.test",), True),
        (("python", "tests/runtests.py"), True),
        (("bin/test",), True),
        (("python", "-m", "unittest"), True),
        (("python", "-c", "print(1)"), False),
        (("python", "-m", "example", "unittest"), False),
    ],
)
def test_no_test_run_detects_pytest_variants(
    failed_trial: ScreeningTrial, argv: tuple[str, ...], has_test_run: bool
) -> None:
    result = classify(replace(failed_trial, commands=(argv,)))
    assert ("no_test_run" not in result.signatures) == has_test_run


def test_pass_is_never_eligible(failed_trial: ScreeningTrial) -> None:
    result = classify(
        replace(
            failed_trial,
            verdict="pass",
            termination="budget_exhausted",
            tool_error_count=2,
        )
    )
    assert result.signatures == ("budget_exhaustion", "failing_tool_calls")
    assert not result.eligible
    assert result.reason == "passed"


def test_pick_caps_per_project_and_is_deterministic() -> None:
    multi = Classification(
        "sympy-1",
        "sympy",
        ("budget_exhaustion", "no_test_run"),
        False,
        True,
        "targeted",
    )
    overlap = replace(
        multi, case_id="sympy-2", signatures=("no_test_run",), overlaps_reference=True
    )
    items = [
        replace(overlap, case_id="sympy-3"),
        replace(overlap, case_id="sphinx-1", project="sphinx"),
        replace(
            overlap, case_id="pytest-1", project="pytest", overlaps_reference=False
        ),
        replace(
            multi,
            case_id="astropy-1",
            project="astropy",
            eligible=False,
            reason="passed",
        ),
        multi,
        overlap,
    ]
    expected = ("sympy-1", "sphinx-1", "sympy-2", "pytest-1")
    assert pick(items, seed="fixed") == expected
    assert pick(list(reversed(items)), seed="fixed") == expected
    assert pick(items, seed="fixed", limit=2) == expected[:2]
    assert pick(items, seed="fixed", limit=0) == ()


def test_pick_output_has_no_reference_content(failed_trial: ScreeningTrial) -> None:
    private_path = "operator-only-reference-path.py"
    classification = classify(
        replace(
            failed_trial,
            commands=(),
            patch_files=(private_path,),
            reference_files=(private_path,),
        )
    )
    result = pick([classification], seed="fixed")
    record = classification_record(classification)
    assert json.loads(json.dumps(result)) == [failed_trial.case_id]
    assert record == {
        "case_id": failed_trial.case_id,
        "project": failed_trial.project,
        "signatures": ("no_test_run",),
        "overlaps_reference": True,
        "eligible": True,
        "reason": "targeted",
    }
    assert private_path not in json.dumps([result, record])
