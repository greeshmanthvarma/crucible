from datetime import UTC, datetime

from crucible.context.manager import _model_part
from crucible.domain.conversation import MessagePart, MessagePartKind
from crucible.domain.ids import new_id
from crucible.domain.tools import (
    ToolCall,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)


def test_context_replays_provider_call_id_for_call_and_result() -> None:
    now = datetime.now(UTC)
    task_id, run_id, step_id, message_id = (new_id() for _ in range(4))
    call = ToolCall(
        id=new_id(),
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        assistant_message_id=message_id,
        call_sequence=1,
        name="read_file",
        arguments={"path": "README.md"},
        schema_version=1,
        provider_correlation_id="call_provider_1",
        execution_mode=ToolExecutionMode.SEQUENTIAL,
        created_at=now,
    )
    result = ToolResult(
        id=new_id(),
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        tool_call_id=call.id,
        status=ToolResultStatus.SUCCEEDED,
        result={},
        schema_version=1,
        display_text="contents",
        error_code=None,
        completion_sequence=1,
        created_at=now,
        completed_at=now,
    )
    calls = {call.id: call}
    results = {result.id: result}

    model_call = _model_part(
        MessagePart(new_id(), 1, MessagePartKind.TOOL_CALL, None, tool_call_id=call.id),
        calls,
        results,
    )
    model_result = _model_part(
        MessagePart(
            new_id(),
            1,
            MessagePartKind.TOOL_RESULT,
            "contents",
            tool_result_id=result.id,
        ),
        calls,
        results,
    )

    assert model_call.provider_correlation_id == "call_provider_1"
    assert model_result.provider_correlation_id == "call_provider_1"
    assert model_result.tool_call_id == call.id
