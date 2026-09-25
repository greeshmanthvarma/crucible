import pytest

from crucible.domain.ids import new_id
from crucible.engine.fake_gateway import ScriptedExchange, ScriptedModelGateway
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelToolDefinition,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)


def prepared() -> PreparedModelRequest:
    return PreparedModelRequest(
        run_id=new_id(),
        step_id=new_id(),
        model="fixture",
        messages=(ModelMessage(ModelRole.USER, (ModelPart("text", "inspect"),)),),
        tools=(
            ModelToolDefinition(
                "read_file",
                "Read a workspace file",
                {"type": "object", "properties": {"path": {"type": "string"}}},
            ),
        ),
        max_output_tokens=200,
    )


async def test_scripted_gateway_preserves_text_calls_usage_and_stop() -> None:
    request = prepared()
    call = CompleteToolCall(new_id(), "read_file", {"path": "README.md"}, "p1")
    items = (
        TextDelta("I will inspect."),
        call,
        ModelUsage(10, 5),
        ModelStop(ModelStopReason.TOOL_CALLS),
    )
    gateway = ScriptedModelGateway((ScriptedExchange(request, items),))

    assert tuple([item async for item in gateway.stream(request)]) == items
    gateway.assert_exhausted()


async def test_scripted_gateway_rejects_wrong_or_missing_requests() -> None:
    request = prepared()
    gateway = ScriptedModelGateway((ScriptedExchange(request, ()),))
    wrong = PreparedModelRequest(
        **{**request.__dict__, "max_output_tokens": request.max_output_tokens + 1}
    )

    with pytest.raises(AssertionError, match="did not match"):
        _ = [item async for item in gateway.stream(wrong)]
    with pytest.raises(AssertionError, match="Missing model requests"):
        gateway.assert_exhausted()
