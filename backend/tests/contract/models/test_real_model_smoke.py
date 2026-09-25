import os

import pytest

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    ModelToolDefinition,
    PreparedModelRequest,
)
from crucible.models.litellm_gateway import LiteLLMModelGateway
from crucible.models.responses_gateway import LiteLLMResponsesGateway

REAL_MODEL = os.environ.get("CRUCIBLE_REAL_MODEL")


@pytest.mark.skipif(
    REAL_MODEL is None,
    reason="set CRUCIBLE_REAL_MODEL and its provider credential to opt in",
)
@pytest.mark.real_model
async def test_configured_real_model_stream_smoke() -> None:
    request = PreparedModelRequest(
        new_id(),
        new_id(),
        REAL_MODEL or "",
        (
            ModelMessage(
                ModelRole.SYSTEM,
                (ModelPart("text", "This is a read-only connectivity smoke test."),),
            ),
            ModelMessage(
                ModelRole.USER,
                (ModelPart("text", "Reply with the single word ready."),),
            ),
        ),
        (),
        16,
    )

    gateway = (
        LiteLLMResponsesGateway()
        if request.model.startswith("openai/")
        else LiteLLMModelGateway()
    )
    items = [item async for item in gateway.stream(request)]

    assert not [item for item in items if isinstance(item, ModelError)]
    assert any(isinstance(item, ModelStop) for item in items)


@pytest.mark.skipif(
    REAL_MODEL is None or not REAL_MODEL.startswith("openai/"),
    reason="requires an opted-in OpenAI Responses model",
)
@pytest.mark.real_model
async def test_real_responses_function_call_round_trip() -> None:
    gateway = LiteLLMResponsesGateway()
    user = ModelMessage(
        ModelRole.USER,
        (
            ModelPart(
                "text",
                "Call read_file for README.md. Then summarize the returned contents.",
            ),
        ),
    )
    tool = ModelToolDefinition(
        "read_file",
        "Read the named file and return its contents",
        {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    )
    first = PreparedModelRequest(
        new_id(), new_id(), REAL_MODEL or "", (user,), (tool,), 256
    )
    first_items = [item async for item in gateway.stream(first)]
    assert not [item for item in first_items if isinstance(item, ModelError)]
    call = next(item for item in first_items if isinstance(item, CompleteToolCall))
    assert call.name == "read_file"
    assert call.provider_correlation_id

    second = PreparedModelRequest(
        first.run_id,
        new_id(),
        first.model,
        (
            user,
            ModelMessage(
                ModelRole.ASSISTANT,
                (
                    ModelPart(
                        "tool_call",
                        tool_call_id=call.id,
                        tool_name=call.name,
                        arguments=call.arguments,
                        provider_correlation_id=call.provider_correlation_id,
                    ),
                ),
            ),
            ModelMessage(
                ModelRole.TOOL,
                (
                    ModelPart(
                        "tool_result",
                        "README.md contains: Crucible is a coding-agent harness.",
                        tool_call_id=call.id,
                        provider_correlation_id=call.provider_correlation_id,
                    ),
                ),
            ),
        ),
        (tool,),
        256,
    )
    second_items = [item async for item in gateway.stream(second)]
    assert not [item for item in second_items if isinstance(item, ModelError)]
    assert any(isinstance(item, ModelStop) for item in second_items)
