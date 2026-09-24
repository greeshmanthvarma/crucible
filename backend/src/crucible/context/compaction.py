from dataclasses import dataclass
from typing import Mapping

from crucible.domain.conversation import (
    Message,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import ToolCallId, ToolResultId


@dataclass(frozen=True)
class ConversationUnit:
    messages: tuple[Message, ...]
    complete: bool

    @property
    def start_sequence(self) -> int:
        return self.messages[0].conversation_sequence

    @property
    def end_sequence(self) -> int:
        return self.messages[-1].conversation_sequence


@dataclass(frozen=True)
class CompactionBoundary:
    source_start_sequence: int
    source_end_sequence: int
    retained_tail_start_sequence: int
    source_units: tuple[ConversationUnit, ...]


def group_conversation_units(
    messages: tuple[Message, ...], result_to_call: Mapping[ToolResultId, ToolCallId]
) -> tuple[ConversationUnit, ...]:
    grouped: list[list[Message]] = []
    for message in sorted(messages, key=lambda item: item.conversation_sequence):
        if message.role is MessageRole.USER or not grouped:
            grouped.append([])
        grouped[-1].append(message)
    return tuple(
        ConversationUnit(tuple(group), _is_complete(group, result_to_call))
        for group in grouped
    )


def _is_complete(
    messages: list[Message], result_to_call: Mapping[ToolResultId, ToolCallId]
) -> bool:
    if not messages or messages[0].role is not MessageRole.USER:
        return False
    if any(message.status is not MessageStatus.COMPLETED for message in messages):
        return False
    calls = {
        part.tool_call_id
        for message in messages
        for part in message.parts
        if part.kind is MessagePartKind.TOOL_CALL and part.tool_call_id is not None
    }
    results: set[ToolCallId] = set()
    for message in messages:
        for part in message.parts:
            result_id = part.tool_result_id
            if part.kind is MessagePartKind.TOOL_RESULT and result_id is not None:
                call_id = result_to_call.get(result_id)
                if call_id is not None:
                    results.add(call_id)
    has_assistant = any(message.role is MessageRole.ASSISTANT for message in messages)
    return has_assistant and calls <= results


def select_compaction_boundary(
    units: tuple[ConversationUnit, ...], *, retain_complete_units: int
) -> CompactionBoundary | None:
    if retain_complete_units < 0:
        raise ValueError("retained unit count cannot be negative")
    complete_prefix: list[ConversationUnit] = []
    for unit in units:
        if not unit.complete:
            break
        complete_prefix.append(unit)
    compact_count = len(complete_prefix) - retain_complete_units
    if compact_count <= 0:
        return None
    source = tuple(complete_prefix[:compact_count])
    retained_index = compact_count
    retained_start = (
        units[retained_index].start_sequence
        if retained_index < len(units)
        else source[-1].end_sequence + 1
    )
    return CompactionBoundary(
        source[0].start_sequence, source[-1].end_sequence, retained_start, source
    )
