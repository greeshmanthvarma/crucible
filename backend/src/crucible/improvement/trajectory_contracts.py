"""Worker-safe normalized evidence contracts; no storage or execution policy."""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from crucible.domain.conversation import Message
from crucible.domain.tools import ToolCall, ToolResult
from crucible.improvement.contracts import Contract, SourceIdentity
from crucible.improvement.roles import EvidenceRef
from crucible.repair.gateway import TrustedUsage


class TraceCompleteness(Contract):
    missing_result_call_ids: tuple[UUID, ...] = ()
    unresolved_part_refs: tuple[UUID, ...] = ()
    truncated_detail_refs: tuple[str, ...] = ()
    unavailable_fields: tuple[
        Literal["steps", "context_manifests", "compactions", "per_step_usage"], ...
    ] = ("steps", "context_manifests", "compactions", "per_step_usage")


@dataclass(frozen=True)
class VerifiedTrial:
    trial_id: UUID
    case_id: str
    repeat_index: Literal[0, 1, 2]
    run_id: UUID
    task_id: UUID
    verdict: Literal["pass", "fail"]
    termination: Literal["completed", "budget_exhausted", "behavior_error"]
    source: SourceIdentity
    trace_ref: EvidenceRef
    trace_bytes: bytes
    solution_ref: EvidenceRef
    solution_bytes: bytes
    usage: TrustedUsage | None


class TraceWire(Contract):
    schema_version: Literal[2]
    messages: tuple[Message, ...]
    tool_calls: tuple[ToolCall, ...]
    tool_results: tuple[ToolResult, ...]


@dataclass(frozen=True)
class SolverTrace:
    schema_version: Literal[2]
    messages: tuple[Message, ...]
    tool_calls: tuple[ToolCall, ...]
    tool_results: tuple[ToolResult, ...]
    completeness: TraceCompleteness


class TrajectoryRecord(Contract):
    sequence: int
    kind: Literal["message", "tool_call", "tool_result"]
    record_id: UUID
    step_id: UUID | None
    conversation_sequence: int | None
    call_sequence: int | None
    completion_sequence: int | None
    call_id: UUID | None
    result_id: UUID | None
    tool: str | None
    status: str
    error_code: str | None
    execution_mode: Literal["parallel", "sequential"] | None
    detail_ref: str | None
    linked_ids: tuple[UUID, ...]
