from datetime import UTC, datetime

import pytest

from crucible.context.manifests import ContextManifest
from crucible.domain.ids import new_id
from crucible.domain.steps import Step, StepStatus
from crucible.domain.tools import (
    ToolCall,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)

NOW = datetime(2026, 9, 21, tzinfo=UTC)


def test_step_lifecycle_allows_only_ordered_transitions() -> None:
    step = Step.preparing(new_id(), new_id(), new_id(), 1, NOW)
    model_active = step.activate_model(NOW)
    tools_active = model_active.activate_tools(NOW)
    completed = tools_active.complete(NOW)

    assert completed.status is StepStatus.COMPLETED
    with pytest.raises(ValueError, match="cannot transition"):
        step.complete(NOW)


def test_tool_call_identity_order_and_terminal_result_are_immutable() -> None:
    call = ToolCall(
        id=new_id(),
        task_id=new_id(),
        run_id=new_id(),
        step_id=new_id(),
        assistant_message_id=new_id(),
        call_sequence=2,
        name="read_file",
        arguments={"path": "README.md"},
        schema_version=1,
        provider_correlation_id="provider-1",
        execution_mode=ToolExecutionMode.PARALLEL,
        created_at=NOW,
    )
    result = ToolResult(
        id=new_id(),
        task_id=call.task_id,
        run_id=call.run_id,
        step_id=call.step_id,
        tool_call_id=call.id,
        status=ToolResultStatus.SUCCEEDED,
        result={"content": "ok"},
        schema_version=1,
        display_text="ok",
        error_code=None,
        completion_sequence=1,
        created_at=NOW,
        completed_at=NOW,
    )

    assert call.call_sequence == 2
    assert result.tool_call_id == call.id
    with pytest.raises(ValueError, match="positive"):
        ToolResult(**{**result.__dict__, "completion_sequence": 0})


def test_context_manifest_copies_immutable_evidence() -> None:
    message_ids = [new_id()]
    manifest = ContextManifest(
        id=new_id(),
        task_id=new_id(),
        run_id=new_id(),
        step_id=new_id(),
        model="fixture-model",
        parameters={"temperature": 0},
        input_limit=1000,
        output_reserve=200,
        threshold=0.8,
        estimated_tokens=400,
        message_ids=message_ids,
        part_ids=[new_id()],
        instruction_digests={"harness": "abc"},
        tool_schema_digest="def",
        created_at=NOW,
    )
    message_ids.append(new_id())

    assert len(manifest.message_ids) == 1
    with pytest.raises(TypeError):
        manifest.parameters["temperature"] = 1  # type: ignore[index]
