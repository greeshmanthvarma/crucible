from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from crucible.domain.clock import require_utc
from crucible.domain.ids import (
    MessageId,
    MessagePartId,
    RunId,
    StepId,
    TaskId,
    ToolCallId,
    ToolResultId,
)


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class MessageStatus(StrEnum):
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class MessagePartKind(StrEnum):
    TEXT = "text"
    REASONING = "reasoning"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"


@dataclass(frozen=True)
class MessagePart:
    id: MessagePartId
    part_sequence: int
    kind: MessagePartKind
    text_content: str | None
    reasoning_content: str | None = None
    tool_call_id: ToolCallId | None = None
    tool_result_id: ToolResultId | None = None


@dataclass(frozen=True)
class Message:
    id: MessageId
    task_id: TaskId
    run_id: RunId | None
    step_id: StepId | None
    conversation_sequence: int
    role: MessageRole
    status: MessageStatus
    parts: tuple[MessagePart, ...]
    created_at: datetime
    completed_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
