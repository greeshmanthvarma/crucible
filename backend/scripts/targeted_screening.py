"""Classify one-trial evidence for targeted development case selection."""

from collections import Counter
from collections.abc import Sequence
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
