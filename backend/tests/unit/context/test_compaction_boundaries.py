from datetime import UTC, datetime

from crucible.context.compaction import (
    group_conversation_units,
    select_compaction_boundary,
)
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id

NOW = datetime(2026, 9, 23, tzinfo=UTC)


def message(sequence: int, role: MessageRole, *, call=None, result=None) -> Message:
    kind = (
        MessagePartKind.TOOL_CALL
        if call
        else MessagePartKind.TOOL_RESULT
        if result
        else MessagePartKind.TEXT
    )
    return Message(
        new_id(),
        new_id(),
        None,
        None,
        sequence,
        role,
        MessageStatus.COMPLETED,
        (
            MessagePart(
                new_id(),
                1,
                kind,
                "text" if not call and not result else None,
                tool_call_id=call,
                tool_result_id=result,
            ),
        ),
        NOW,
        NOW,
    )


def test_units_keep_user_exchange_and_tool_call_results_together() -> None:
    call, result = new_id(), new_id()
    messages = (
        message(1, MessageRole.USER),
        message(2, MessageRole.ASSISTANT, call=call),
        message(3, MessageRole.TOOL, result=result),
        message(4, MessageRole.ASSISTANT),
        message(5, MessageRole.USER),
    )
    units = group_conversation_units(messages, {result: call})
    assert [
        tuple(item.conversation_sequence for item in unit.messages) for unit in units
    ] == [(1, 2, 3, 4), (5,)]
    assert units[0].complete is True
    assert units[1].complete is False


def test_boundary_uses_largest_complete_prefix_and_retains_recent_units() -> None:
    messages = tuple(
        item
        for sequence in range(1, 5)
        for item in (
            message(sequence * 2 - 1, MessageRole.USER),
            message(sequence * 2, MessageRole.ASSISTANT),
        )
    )
    boundary = select_compaction_boundary(
        group_conversation_units(messages, {}), retain_complete_units=2
    )
    assert boundary is not None
    assert boundary.source_start_sequence == 1
    assert boundary.source_end_sequence == 4
    assert boundary.retained_tail_start_sequence == 5


def test_no_boundary_orphans_an_unfinished_exchange() -> None:
    units = group_conversation_units((message(1, MessageRole.USER),), {})
    assert select_compaction_boundary(units, retain_complete_units=0) is None


def test_steering_users_share_the_unit_closed_by_the_next_assistant() -> None:
    units = group_conversation_units(
        (
            message(1, MessageRole.USER),
            message(2, MessageRole.USER),
            message(3, MessageRole.ASSISTANT),
        ),
        {},
    )

    assert len(units) == 1
    assert units[0].complete
    assert units[0].start_sequence == 1
    assert units[0].end_sequence == 3
