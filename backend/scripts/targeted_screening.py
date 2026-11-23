"""Classify one-trial evidence for targeted development case selection."""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from typing import Literal

Signature = Literal[
    "budget_exhaustion", "repeated_reads", "no_test_run", "failing_tool_calls"
]


@dataclass(frozen=True)
class ScreeningTrial:
    case_id: str
    project: str
    verdict: Literal["pass", "fail"]
    termination: str
    read_paths: tuple[str, ...]
    commands: tuple[tuple[str, ...], ...]
    tool_error_count: int
    patch_files: tuple[str, ...]
    reference_files: tuple[str, ...]


@dataclass(frozen=True)
class Classification:
    case_id: str
    project: str
    signatures: tuple[Signature, ...]
    overlaps_reference: bool
    eligible: bool
    reason: Literal["passed", "no_signature", "targeted"]


def _is_test_command(argv: tuple[str, ...]) -> bool:
    return any(
        token.endswith(("pytest", "py.test", "runtests.py", "bin/test"))
        for token in argv
    ) or ("-m", "unittest") in pairwise(argv)


def classify(trial: ScreeningTrial) -> Classification:
    signatures: list[Signature] = []
    if trial.termination == "budget_exhausted":
        signatures.append("budget_exhaustion")
    if len(trial.read_paths) - len(set(trial.read_paths)) >= 3:
        signatures.append("repeated_reads")
    if not any(_is_test_command(argv) for argv in trial.commands):
        signatures.append("no_test_run")
    if trial.tool_error_count >= 2:
        signatures.append("failing_tool_calls")

    reason: Literal["passed", "no_signature", "targeted"]
    if trial.verdict == "pass":
        reason = "passed"
    elif signatures:
        reason = "targeted"
    else:
        reason = "no_signature"
    return Classification(
        case_id=trial.case_id,
        project=trial.project,
        signatures=tuple(sorted(signatures)),
        overlaps_reference=bool(
            set(trial.patch_files).intersection(trial.reference_files)
        ),
        eligible=reason == "targeted",
        reason=reason,
    )


def pick(
    items: Sequence[Classification], *, seed: str, limit: int = 6, per_project: int = 2
) -> tuple[str, ...]:
    ranked = sorted(
        (item for item in items if item.eligible),
        key=lambda item: (
            -len(item.signatures),
            not item.overlaps_reference,
            sha256((seed + item.case_id).encode()).hexdigest(),
        ),
    )
    counts: Counter[str] = Counter()
    chosen: list[str] = []
    for item in ranked:
        if len(chosen) >= limit:
            break
        if counts[item.project] >= per_project:
            continue
        chosen.append(item.case_id)
        counts[item.project] += 1
    return tuple(chosen)


def classification_record(c: Classification) -> dict[str, object]:
    return {
        "case_id": c.case_id,
        "project": c.project,
        "signatures": c.signatures,
        "overlaps_reference": c.overlaps_reference,
        "eligible": c.eligible,
        "reason": c.reason,
    }


def v4_proposal(
    v3_manifest: Mapping[str, object],
    v3_dev_counts: Mapping[str, int],
    targeted: Sequence[str],
) -> dict[str, object]:
    if len(targeted) < 3:
        raise ValueError("insufficient targeted evidence")
    if len(set(targeted)) != len(targeted) or len(targeted) > 8:
        raise ValueError("targeted cases must be distinct and fit the development cap")

    v3_cases: dict[str, list[str]] = {
        "development": [],
        "selection-validation": [],
        "final-test": [],
    }
    rows = v3_manifest.get("cases")
    if not isinstance(rows, (list, tuple)):
        raise ValueError("v3 manifest requires case records")
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("invalid v3 case record")
        split, case_id = row.get("split"), row.get("case_id")
        if not isinstance(case_id, str) or split not in v3_cases:
            raise ValueError("invalid v3 case id or split")
        v3_cases[split].append(case_id)

    old_development = v3_cases["development"]
    held_out = set(v3_cases["selection-validation"]) | set(v3_cases["final-test"])
    if held_out.intersection([*old_development, *targeted]):
        raise ValueError("development cases must be disjoint from the v3 held-out sets")
    if set(old_development).intersection(targeted):
        raise ValueError("targeted cases must be new to development")

    passes = {case_id: v3_dev_counts.get(case_id, 0) for case_id in old_development}
    ranked = sorted(
        (case_id for case_id in old_development if passes[case_id] < 3),
        key=passes.__getitem__,
    )
    retained = set(ranked[: 8 - len(targeted)])
    proposal: dict[str, object] = {
        "development": [
            *(case_id for case_id in old_development if case_id in retained),
            *targeted,
        ],
        "selection_validation": list(v3_cases["selection-validation"]),
        "final_test": list(v3_cases["final-test"]),
        "dropped_development": [
            case_id for case_id in old_development if case_id not in retained
        ],
        "rule": (
            "Keep all targeted cases and fill remaining development slots to eight "
            "with v3 cases below three passes, prioritizing fewer passes and using "
            "manifest order for ties. Copy both v3 held-out lists verbatim."
        ),
    }
    assert proposal["selection_validation"] == v3_cases["selection-validation"]
    assert proposal["final_test"] == v3_cases["final-test"]
    return proposal
