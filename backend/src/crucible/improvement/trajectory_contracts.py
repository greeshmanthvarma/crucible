"""Worker-safe normalized evidence contracts; no storage or execution policy."""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Mapping, Self
from uuid import UUID

from pydantic import Field, model_validator

from crucible.benchmarks.contracts import Sha256
from crucible.domain.conversation import Message
from crucible.domain.tools import ToolCall, ToolResult
from crucible.improvement.contracts import Contract, Positive, SourceIdentity
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
    original_pointer_prefix: Literal["", "/data"] = ""


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


class ExportLimits(Contract):
    overview_bytes: Positive = 8192
    index_record_bytes: Positive = 1024
    index_bytes: Positive = 262144
    detail_bytes: Positive = 1048576
    evidence_bytes: Positive = 16777216
    input_trace_bytes: Positive = 8388608

    @model_validator(mode="after")
    def bounded(self) -> Self:
        ceilings = (8192, 1024, 262144, 1048576, 16777216, 8388608)
        if any(
            getattr(self, name) > cap
            for name, cap in zip(type(self).model_fields, ceilings, strict=True)
        ):
            raise ValueError("export limits cannot widen frozen defaults")
        return self


class DetailEntry(Contract):
    ref: str
    kind: Literal["message", "arguments", "result", "task_instruction", "solution_diff"]
    original_record_id: UUID | None
    original_artifact: EvidenceRef
    original_json_pointer: str | None
    content_sha256: Sha256
    original_bytes: int
    exported_bytes: int
    truncated: bool
    files: Annotated[tuple[str, ...], Field(min_length=1, max_length=2)]


class TrialOverview(Contract):
    trial_id: UUID
    case_id: str
    repeat_index: Literal[0, 1, 2]
    run_id: UUID
    task_id: UUID
    source: SourceIdentity
    verdict: Literal["pass", "fail"]
    termination: Literal["completed", "budget_exhausted", "behavior_error"]
    task_instruction_ref: str | None
    usage: TrustedUsage | None
    usage_available: bool
    task_instruction_available: bool
    ordering_basis: Literal[
        "conversation-anchors", "conversation-anchors-with-unanchored-source-order"
    ]
    message_count: int
    call_count: int
    result_count: int
    error_result_count: int
    exact_repeated_call_count: int
    completeness: TraceCompleteness
    index_ref: str
    details_manifest_ref: str


@dataclass(frozen=True)
class TrajectoryBundle:
    trial_id: UUID
    files: Mapping[str, bytes]
    overview: TrialOverview
    original_refs: tuple[EvidenceRef, ...]


class TrialSelection(Contract):
    mode: Literal["explicit-three-repeat", "failed-family-v1"]
    trial_ids: Annotated[tuple[UUID, ...], Field(max_length=3)]

    @model_validator(mode="after")
    def distinct(self) -> Self:
        if len(set(self.trial_ids)) != len(self.trial_ids):
            raise ValueError("duplicate selected trial")
        if self.mode == "explicit-three-repeat" and len(self.trial_ids) != 3:
            raise ValueError("explicit selection requires three repeats")
        return self


@dataclass(frozen=True)
class VerifiedDevelopmentInputs:
    trials: tuple[VerifiedTrial, ...]
    source: SourceIdentity
    source_files: Mapping[str, tuple[int, bytes]]
    selected_trial_ids: tuple[UUID, ...]
    source_experiment_id: UUID
    source_snapshot_id: UUID
    selection: TrialSelection
    private_roots: tuple[Path, ...] = ()


class EvidencePackage(Contract):
    schema_version: Literal[1]
    bundle_digest: Sha256
    source: SourceIdentity
    source_experiment_id: UUID
    source_snapshot_id: UUID
    selected_trial_ids: Annotated[tuple[UUID, ...], Field(min_length=1, max_length=3)]
    selection: TrialSelection
    limits: ExportLimits
    files: dict[str, Sha256]
    trial_overview_refs: dict[UUID, str]
    record_locator_ref: str
