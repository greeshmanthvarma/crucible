"""Group validated development diagnoses by their cited tool failures."""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from crucible.domain.tools import ToolResultStatus
from crucible.improvement.case_diagnosis import Diagnosis
from crucible.improvement.case_evidence import CasePacket

_TOOL_LABELS = {
    "list_files": "list_files_failure",
    "search_files": "search_files_failure",
    "read_file": "read_file_failure",
    "write_file": "write_file_failure",
    "apply_patch": "apply_patch_failure",
    "workspace_status": "workspace_status_failure",
    "workspace_diff": "workspace_diff_failure",
    "execute_command": "execute_command_failure",
}
_FAILURE_STATUSES = frozenset(
    {ToolResultStatus.FAILED, ToolResultStatus.REJECTED, ToolResultStatus.TIMED_OUT}
)


@dataclass(frozen=True)
class CitedToolResult:
    id: str
    tool: str | None
    status: str
    error_code: str | None
    excerpt: str | None


@dataclass(frozen=True)
class DiagnosedAttempt:
    case_id: str
    trial_id: UUID
    diagnosis: Diagnosis
    cited_results: tuple[CitedToolResult, ...]


@dataclass(frozen=True)
class ToolFailureGroup:
    label: str
    attempts: tuple[DiagnosedAttempt, ...]


@dataclass(frozen=True)
class UnresolvedDiagnosis:
    attempt: DiagnosedAttempt
    reason: Literal["no_eligible_failure", "multiple_tools"]
    candidate_labels: tuple[str, ...]


@dataclass(frozen=True)
class ToolFailureReport:
    groups: tuple[ToolFailureGroup, ...]
    unresolved: tuple[UnresolvedDiagnosis, ...]


def _cited_attempt(packet: CasePacket, diagnosis: Diagnosis) -> DiagnosedAttempt:
    cited_ids = frozenset(diagnosis.evidence_refs)
    if not cited_ids.issubset(event.id for event in packet.events):
        raise ValueError("Diagnosis cites invisible evidence")
    results = tuple(
        CitedToolResult(
            event.id, event.tool, event.status, event.error_code, event.excerpt
        )
        for event in packet.events
        if event.kind == "tool_result" and event.id in cited_ids
    )
    return DiagnosedAttempt(packet.case_id, packet.failed_trial_id, diagnosis, results)


def _candidate_labels(attempt: DiagnosedAttempt) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                _TOOL_LABELS[result.tool]
                for result in attempt.cited_results
                if result.tool is not None
                and result.tool in _TOOL_LABELS
                and result.status in _FAILURE_STATUSES
                and result.error_code != "budget_exhausted"
                # An ordinary exit can report a test failure or a useful result.
                and not (
                    result.tool == "execute_command"
                    and result.error_code == "command_exit_nonzero"
                )
            }
        )
    )


def group_tool_failures(
    diagnosed_cases: Iterable[tuple[CasePacket, Diagnosis]],
) -> ToolFailureReport:
    """Assign a label only when the cited failures identify exactly one tool."""
    groups: defaultdict[str, list[DiagnosedAttempt]] = defaultdict(list)
    unresolved: list[UnresolvedDiagnosis] = []
    seen: set[UUID] = set()
    for packet, diagnosis in sorted(
        diagnosed_cases, key=lambda item: item[0].failed_trial_id
    ):
        if packet.failed_trial_id in seen:
            raise ValueError("Duplicate diagnosed trial")
        seen.add(packet.failed_trial_id)
        attempt = _cited_attempt(packet, diagnosis)
        candidates = _candidate_labels(attempt)
        if len(candidates) == 1:
            groups[candidates[0]].append(attempt)
        else:
            unresolved.append(
                UnresolvedDiagnosis(
                    attempt,
                    "multiple_tools" if candidates else "no_eligible_failure",
                    candidates,
                )
            )
    return ToolFailureReport(
        tuple(
            ToolFailureGroup(label, tuple(attempts))
            for label, attempts in sorted(groups.items())
        ),
        tuple(unresolved),
    )
