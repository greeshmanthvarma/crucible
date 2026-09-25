from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import (
    MessageId,
    RunId,
    StepId,
    TaskId,
    ToolCallId,
    ToolResultId,
)


class ToolExecutionMode(StrEnum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"


class ToolCallStatus(StrEnum):
    ADMITTED = "admitted"
    REJECTED = "rejected"
    RUNNING = "running"
    COMPLETED = "completed"


class ToolResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class ToolCall:
    id: ToolCallId
    task_id: TaskId
    run_id: RunId
    step_id: StepId
    assistant_message_id: MessageId
    call_sequence: int
    name: str
    arguments: Mapping[str, object]
    schema_version: int
    provider_correlation_id: str | None
    execution_mode: ToolExecutionMode
    created_at: datetime
    status: ToolCallStatus = ToolCallStatus.ADMITTED

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        if self.call_sequence < 1:
            raise ValueError("call_sequence must be positive")
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        object.__setattr__(self, "arguments", _freeze(self.arguments))


@dataclass(frozen=True)
class ToolResult:
    id: ToolResultId
    task_id: TaskId
    run_id: RunId
    step_id: StepId
    tool_call_id: ToolCallId
    status: ToolResultStatus
    result: Mapping[str, object]
    schema_version: int
    display_text: str
    error_code: str | None
    completion_sequence: int
    created_at: datetime
    completed_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
        if self.completion_sequence < 1:
            raise ValueError("completion_sequence must be positive")
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        object.__setattr__(self, "result", _freeze(self.result))


def _freeze(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(dict(value))
