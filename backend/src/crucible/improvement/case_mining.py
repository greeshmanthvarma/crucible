"""Build a grouped mining report from pinned development evidence."""

from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal
from uuid import UUID

from crucible.engine.gateway import ModelGateway
from crucible.improvement.case_diagnosis import (
    CASE_TOKEN_CAP,
    CaseDiagnosisResult,
    Diagnosis,
    run_case_diagnosis,
)
from crucible.improvement.case_evidence import (
    CasePacket,
    PreparedCasePacket,
    assemble_failed_case_packets,
)
from crucible.improvement.case_grouping import (
    DiagnosedAttempt,
    UnresolvedDiagnosis,
    group_tool_failures,
)

MAX_PACKAGES = 10


@dataclass(frozen=True)
class PinnedPackage:
    root: Path
    bundle_digest: str


@dataclass(frozen=True)
class MiningAttempt:
    case_id: str
    trial_id: UUID
    bundle_digest: str
    outcome: Literal["grouped", "unresolved", "failed", "skipped"]
    error_code: str | None
    requests_sent: int
    reserved_tokens: int
    disclosed_refs: tuple[str, ...]


@dataclass(frozen=True)
class MiningGroup:
    label: str
    attempt_count: int
    distinct_case_count: int
    representative_trial_id: UUID
    attempts: tuple[DiagnosedAttempt, ...]


@dataclass(frozen=True)
class CaseMiningReport:
    schema_version: Literal[1]
    model: str
    batch_token_cap: int
    reserved_tokens: int
    bundle_digests: tuple[str, ...]
    attempts: tuple[MiningAttempt, ...]
    groups: tuple[MiningGroup, ...]
    unresolved: tuple[UnresolvedDiagnosis, ...]


def _prepare_cases(
    packages: Sequence[PinnedPackage],
) -> dict[UUID, tuple[str, PreparedCasePacket]]:
    cases: dict[UUID, tuple[str, PreparedCasePacket]] = {}
    for package in packages:
        for prepared in assemble_failed_case_packets(
            package.root, expected_bundle_digest=package.bundle_digest
        ):
            trial_id = prepared.packet.failed_trial_id
            if trial_id in cases:
                raise ValueError("duplicate_trial")
            cases[trial_id] = (package.bundle_digest, prepared)
    return cases


async def mine_development_cases(
    packages: Sequence[PinnedPackage],
    *,
    gateway: ModelGateway,
    model: str,
    batch_token_cap: int,
) -> CaseMiningReport:
    """Run the bounded diagnosis exchange once per failed attempt, then group."""
    if not 1 <= len(packages) <= MAX_PACKAGES:
        raise ValueError("invalid_package_count")
    if type(batch_token_cap) is not int or batch_token_cap < 1:
        raise ValueError("invalid_batch_token_cap")
    if not model.strip():
        raise ValueError("invalid_model")

    cases = _prepare_cases(packages)
    outcomes: list[MiningAttempt] = []
    diagnosed: list[tuple[CasePacket, Diagnosis]] = []
    remaining = batch_token_cap
    for trial_id, (bundle_digest, prepared) in sorted(cases.items()):
        result = (
            await run_case_diagnosis(
                prepared,
                gateway=gateway,
                model=model,
                total_token_cap=min(remaining, CASE_TOKEN_CAP),
            )
            if remaining
            else CaseDiagnosisResult(None, "budget_exceeded", 0, 0, ())
        )
        remaining -= result.total_reserved_tokens
        if result.diagnosis is not None:
            diagnosed.append((prepared.packet, result.diagnosis))
        outcome: Literal["unresolved", "skipped", "failed"] = "failed"
        if result.diagnosis is not None:
            outcome = "unresolved"
        elif result.error_code == "budget_exceeded" and result.requests_sent == 0:
            outcome = "skipped"
        outcomes.append(
            MiningAttempt(
                prepared.packet.case_id,
                trial_id,
                bundle_digest,
                outcome,
                result.error_code,
                result.requests_sent,
                result.total_reserved_tokens,
                result.disclosed_refs,
            )
        )

    grouped = group_tool_failures(diagnosed)
    groups = tuple(
        MiningGroup(
            group.label,
            len(group.attempts),
            len({attempt.case_id for attempt in group.attempts}),
            min(
                group.attempts,
                key=lambda attempt: (
                    attempt.diagnosis.relationship not in {"linked", "contributing"},
                    attempt.trial_id,
                ),
            ).trial_id,
            group.attempts,
        )
        for group in grouped.groups
    )
    grouped_ids = {attempt.trial_id for group in groups for attempt in group.attempts}
    return CaseMiningReport(
        1,
        model,
        batch_token_cap,
        batch_token_cap - remaining,
        tuple(sorted({package.bundle_digest for package in packages})),
        tuple(
            replace(attempt, outcome="grouped")
            if attempt.trial_id in grouped_ids
            else attempt
            for attempt in outcomes
        ),
        groups,
        grouped.unresolved,
    )
