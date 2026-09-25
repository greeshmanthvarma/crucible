from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, Protocol, TypeAlias

from crucible.domain.ids import RunId, StepId, ToolCallId


class ModelRole(StrEnum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass(frozen=True)
class ModelPart:
    kind: str
    text_content: str | None = None
    tool_call_id: ToolCallId | None = None
    tool_name: str | None = None
    arguments: Mapping[str, object] | None = None
    provider_correlation_id: str | None = None


@dataclass(frozen=True)
class ModelMessage:
    role: ModelRole
    parts: tuple[ModelPart, ...]


@dataclass(frozen=True)
class ModelToolDefinition:
    name: str
    description: str
    input_schema: Mapping[str, object]
    schema_version: int = 1


@dataclass(frozen=True)
class PreparedModelRequest:
    run_id: RunId
    step_id: StepId
    model: str
    messages: tuple[ModelMessage, ...]
    tools: tuple[ModelToolDefinition, ...]
    max_output_tokens: int


@dataclass(frozen=True)
class TextDelta:
    text: str


@dataclass(frozen=True)
class ReasoningDelta:
    text: str


@dataclass(frozen=True)
class CompleteToolCall:
    id: ToolCallId
    name: str
    arguments: Mapping[str, object]
    provider_correlation_id: str | None = None


@dataclass(frozen=True)
class ModelUsage:
    input_tokens: int | None
    output_tokens: int | None


class ModelStopReason(StrEnum):
    COMPLETE = "complete"
    TOOL_CALLS = "tool_calls"
    LENGTH = "length"


@dataclass(frozen=True)
class ModelStop:
    reason: ModelStopReason


@dataclass(frozen=True)
class ModelError:
    code: str
    detail: str


ModelStreamItem: TypeAlias = (
    TextDelta | ReasoningDelta | CompleteToolCall | ModelUsage | ModelStop | ModelError
)


class ModelGateway(Protocol):
    def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]: ...
