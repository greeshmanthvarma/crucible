"""Protected checks applied after editable context selection, before dispatch."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, Protocol

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import ToolCallId, ToolResultId
from crucible.domain.tools import ToolCall, ToolResult
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole

if TYPE_CHECKING:
    from crucible.context.selection import CompactionBoundary


class ContextInvariantError(ValueError):
    pass


class TokenEstimator(Protocol):
    def estimate(self, messages: tuple[ModelMessage, ...]) -> int: ...


class SimpleTokenEstimator:
    version = "characters-divided-by-four-v1"

    def estimate(self, messages: tuple[ModelMessage, ...]) -> int:
        characters = sum(
            len(part.text_content or "")
            for message in messages
            for part in message.parts
        )
        return max(1, (characters + 3) // 4)


def model_part(
    part: MessagePart,
    calls: Mapping[ToolCallId, ToolCall],
    results: Mapping[ToolResultId, ToolResult],
) -> ModelPart:
    call = calls.get(part.tool_call_id) if part.tool_call_id is not None else None
    result = (
        results.get(part.tool_result_id) if part.tool_result_id is not None else None
    )
    if call is None and result is not None:
        call = calls.get(result.tool_call_id)
    return ModelPart(
        part.kind.value,
        part.text_content or part.reasoning_content,
        tool_call_id=(
            call.id if call is not None else result.tool_call_id if result else None
        ),
        tool_name=call.name if call is not None else None,
        arguments=call.arguments if call is not None else None,
        provider_correlation_id=(
            call.provider_correlation_id if call is not None else None
        ),
    )


@dataclass(frozen=True)
class ContextSettings:
    input_limit: int
    output_reserve: int
    threshold: float
    estimator: TokenEstimator
    supplemental: tuple[ModelMessage, ...] = ()


@dataclass(frozen=True)
class ContextCheck:
    estimated_tokens: int
    capacity: int


def validate_prepared(
    messages: tuple[ModelMessage, ...],
    source_messages: tuple[Message, ...],
    calls: Mapping[ToolCallId, ToolCall],
    results: Mapping[ToolResultId, ToolResult],
    required_instructions: tuple[str, ...],
    settings: ContextSettings,
) -> ContextCheck:
    prefix = (
        tuple(
            ModelMessage(ModelRole.SYSTEM, (ModelPart("text", text),))
            for text in required_instructions
        )
        + settings.supplemental
    )
    if messages[: len(prefix)] != prefix:
        raise ContextInvariantError(
            "Required instructions/context were dropped or altered"
        )
    source = tuple(
        ModelMessage(
            ModelRole(message.role.value),
            tuple(model_part(part, calls, results) for part in message.parts),
        )
        for message in source_messages
    )
    sequences = [message.conversation_sequence for message in source_messages]
    if sequences != sorted(set(sequences)):
        raise ContextInvariantError(
            "Source messages must have unique ordered sequences"
        )
    cursor = 0
    seen_calls: set[ToolCallId] = set()
    seen_results: set[ToolCallId] = set()
    for message in messages[len(prefix) :]:
        while cursor < len(source) and source[cursor] != message:
            cursor += 1
        if cursor == len(source):
            raise ContextInvariantError(
                "Selected messages are not an ordered source subsequence"
            )
        selected_source = source_messages[cursor]
        for source_part in selected_source.parts:
            if (
                source_part.kind is MessagePartKind.TOOL_CALL
                and source_part.tool_call_id not in calls
            ):
                raise ContextInvariantError("Source references an unknown tool call")
            if (
                source_part.kind is MessagePartKind.TOOL_RESULT
                and source_part.tool_result_id not in results
            ):
                raise ContextInvariantError("Source references an unknown tool result")
        cursor += 1
        for part in message.parts:
            if part.kind == "tool_call":
                if (
                    part.tool_call_id is None
                    or part.tool_call_id not in calls
                    or part.tool_call_id in seen_calls
                ):
                    raise ContextInvariantError("Unknown/duplicate tool call")
                seen_calls.add(part.tool_call_id)
            elif part.kind == "tool_result":
                if (
                    part.tool_call_id not in seen_calls
                    or part.tool_call_id in seen_results
                ):
                    raise ContextInvariantError("Orphan/duplicate tool result")
                assert part.tool_call_id is not None
                seen_results.add(part.tool_call_id)
    if seen_calls != seen_results:
        raise ContextInvariantError(
            "Selected conversation contains an incomplete tool exchange"
        )
    estimate = settings.estimator.estimate(messages)
    capacity = int(settings.input_limit * settings.threshold) - settings.output_reserve
    return ContextCheck(estimate, capacity)


def unit_is_complete(
    messages: list[Message], result_to_call: Mapping[ToolResultId, ToolCallId]
) -> bool:
    if not messages or messages[0].role not in (
        MessageRole.USER,
        MessageRole.ASSISTANT,
    ):
        return False
    if any(message.status is not MessageStatus.COMPLETED for message in messages):
        return False
    calls: set[ToolCallId] = set()
    results: set[ToolCallId] = set()
    for message in messages:
        for part in message.parts:
            if part.kind is MessagePartKind.TOOL_CALL:
                if part.tool_call_id is None or part.tool_call_id in calls:
                    return False
                calls.add(part.tool_call_id)
            elif part.kind is MessagePartKind.TOOL_RESULT:
                call_id = (
                    result_to_call.get(part.tool_result_id)
                    if part.tool_result_id is not None
                    else None
                )
                if call_id is None or call_id not in calls or call_id in results:
                    return False
                results.add(call_id)
    has_assistant = any(message.role is MessageRole.ASSISTANT for message in messages)
    return has_assistant and calls == results


def validate_compaction_boundary(
    boundary: "CompactionBoundary",
    source_messages: tuple[Message, ...],
    result_to_call: Mapping[ToolResultId, ToolCallId],
) -> None:
    flattened = tuple(
        message for unit in boundary.source_units for message in unit.messages
    )
    source = tuple(
        message
        for message in source_messages
        if message.conversation_sequence <= boundary.source_end_sequence
    )
    if not flattened or flattened != source:
        raise ContextInvariantError("Compaction must cover an ordered source prefix")
    if boundary.source_start_sequence != flattened[0].conversation_sequence:
        raise ContextInvariantError("Compaction start does not match its source")
    if boundary.source_end_sequence != flattened[-1].conversation_sequence:
        raise ContextInvariantError("Compaction end does not match its source")
    retained = next(
        (
            message.conversation_sequence
            for message in source_messages
            if message.conversation_sequence > boundary.source_end_sequence
        ),
        boundary.source_end_sequence + 1,
    )
    if boundary.retained_tail_start_sequence != retained:
        raise ContextInvariantError("Compaction cannot skip retained messages")
    if any(
        not unit_is_complete(list(unit.messages), result_to_call)
        for unit in boundary.source_units
    ):
        raise ContextInvariantError("Compaction cannot consume incomplete units")
