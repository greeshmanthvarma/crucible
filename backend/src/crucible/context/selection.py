"""Editable conversation assembly and compaction selection policy."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

from crucible.context.invariants import model_part, unit_is_complete
from crucible.domain.conversation import (
    Message,
    MessageRole,
)
from crucible.domain.ids import ToolCallId, ToolResultId
from crucible.domain.tools import ToolCall, ToolResult
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole

if TYPE_CHECKING:
    from crucible.domain.compaction import Compaction


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
        current_has_assistant = bool(grouped) and any(
            item.role is MessageRole.ASSISTANT for item in grouped[-1]
        )
        # A completed assistant/tool batch is a safe checkpoint even when no
        # new user message arrives. Pending parallel calls keep the batch open.
        next_exchange = (
            bool(grouped)
            and message.role is MessageRole.ASSISTANT
            and unit_is_complete(grouped[-1], result_to_call)
        )
        if (
            not grouped
            or (message.role is MessageRole.USER and current_has_assistant)
            or next_exchange
        ):
            grouped.append([])
        grouped[-1].append(message)
    return tuple(
        ConversationUnit(tuple(group), unit_is_complete(group, result_to_call))
        for group in grouped
    )


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


def assemble_visible_messages(
    messages: tuple[Message, ...],
    calls: Mapping[ToolCallId, ToolCall],
    results: Mapping[ToolResultId, ToolResult],
    required_instructions: tuple[str, ...],
    latest_compaction: "Compaction | None",
    supplemental: tuple[ModelMessage, ...] = (),
) -> tuple[ModelMessage, ...]:
    visible = (
        messages
        if latest_compaction is None
        else tuple(
            message
            for message in messages
            if message.conversation_sequence
            >= latest_compaction.retained_tail_start_sequence
        )
    )
    prefix = tuple(
        ModelMessage(ModelRole.SYSTEM, (ModelPart("text", text),))
        for text in required_instructions
    )
    compacted = (
        ()
        if latest_compaction is None
        else (
            ModelMessage(
                ModelRole.SYSTEM,
                (
                    ModelPart(
                        "text",
                        "Compacted Conversation context:\n"
                        + latest_compaction.rendered_summary,
                    ),
                ),
            ),
        )
    )
    return (
        prefix
        + compacted
        + supplemental
        + tuple(
            ModelMessage(
                ModelRole(message.role.value),
                tuple(model_part(part, calls, results) for part in message.parts),
            )
            for message in visible
        )
    )
