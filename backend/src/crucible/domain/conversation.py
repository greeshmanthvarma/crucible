from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from crucible.domain.clock import require_utc
from crucible.domain.ids import MessageId, MessagePartId, RunId, TaskId


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"


class MessageStatus(StrEnum):
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class MessagePartKind(StrEnum):
    TEXT = "text"


@dataclass(frozen=True)
class MessagePart:
    id: MessagePartId
    part_sequence: int
    kind: MessagePartKind
    text_content: str


@dataclass(frozen=True)
class Message:
    id: MessageId
    task_id: TaskId
    run_id: RunId | None
    conversation_sequence: int
    role: MessageRole
    status: MessageStatus
    parts: tuple[MessagePart, ...]
    created_at: datetime
    completed_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
