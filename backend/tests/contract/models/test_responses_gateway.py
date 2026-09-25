import json
from collections.abc import AsyncIterator

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
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
from crucible.models.responses_gateway import LiteLLMResponsesGateway


def request(*messages: ModelMessage) -> PreparedModelRequest:
    return PreparedModelRequest(
        new_id(),
        new_id(),
        "openai/gpt-6-luna",
        messages or (ModelMessage(ModelRole.USER, (ModelPart("text", "Inspect."),)),),
        (
            ModelToolDefinition(
                "read_file",
                "Read a workspace file",
                {"type": "object", "properties": {"path": {"type": "string"}}},
            ),
        ),
        100,
    )


async def test_streams_text_and_function_calls_with_provider_ids() -> None:
    captured: dict[str, object] = {}

    async def events() -> AsyncIterator[dict[str, object]]:
        yield {"type": "response.output_text.delta", "delta": "Reading."}
        yield {
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "call_id": "call_provider_1",
                "name": "read_file",
                "arguments": '{"path":"README.md"}',
            },
        }
        yield {
            "type": "response.completed",
            "response": {"usage": {"input_tokens": 12, "output_tokens": 4}},
        }

    async def response(**kwargs: object) -> object:
        captured.update(kwargs)
        return events()

    items = [item async for item in LiteLLMResponsesGateway(response).stream(request())]

    assert items[0] == TextDelta("Reading.")
    call = next(item for item in items if isinstance(item, CompleteToolCall))
    assert call.provider_correlation_id == "call_provider_1"
    assert call.arguments == {"path": "README.md"}
    assert ModelUsage(12, 4) in items
    assert items[-1] == ModelStop(ModelStopReason.TOOL_CALLS)
    assert captured["model"] == "openai/gpt-6-luna"
    assert captured["stream"] is True
    assert captured["store"] is False
    assert captured["input"] == [{"role": "user", "content": "Inspect."}]
    assert captured["tools"] == [
        {
            "type": "function",
            "name": "read_file",
            "description": "Read a workspace file",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
            },
            "strict": False,
        }
    ]


async def test_replays_function_call_and_result_with_matching_provider_id() -> None:
    captured: dict[str, object] = {}
    owned_id = new_id()

    async def events() -> AsyncIterator[dict[str, object]]:
        yield {"type": "response.completed", "response": {}}

    async def response(**kwargs: object) -> object:
        captured.update(kwargs)
        return events()

    prepared = request(
        ModelMessage(
            ModelRole.ASSISTANT,
            (
                ModelPart("text", "Reading."),
                ModelPart(
                    "tool_call",
                    tool_call_id=owned_id,
                    tool_name="read_file",
                    arguments={"path": "README.md"},
                    provider_correlation_id="call_provider_1",
                ),
            ),
        ),
        ModelMessage(
            ModelRole.TOOL,
            (
                ModelPart(
                    "tool_result",
                    "contents",
                    tool_call_id=owned_id,
                    provider_correlation_id="call_provider_1",
                ),
            ),
        ),
    )
    items = [item async for item in LiteLLMResponsesGateway(response).stream(prepared)]

    assert items == [ModelStop(ModelStopReason.COMPLETE)]
    assert captured["input"] == [
        {"role": "assistant", "content": "Reading."},
        {
            "type": "function_call",
            "call_id": "call_provider_1",
            "name": "read_file",
            "arguments": json.dumps({"path": "README.md"}, separators=(",", ":")),
        },
        {
            "type": "function_call_output",
            "call_id": "call_provider_1",
            "output": "contents",
        },
    ]


async def test_rejects_malformed_function_arguments() -> None:
    async def events() -> AsyncIterator[dict[str, object]]:
        yield {
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "call_id": "call_1",
                "name": "read_file",
                "arguments": "{broken",
            },
        }

    async def response(**_: object) -> object:
        return events()

    items = [item async for item in LiteLLMResponsesGateway(response).stream(request())]
    assert len(items) == 1
    assert isinstance(items[0], ModelError)
    assert items[0].code == "invalid_tool_arguments"


async def test_incomplete_response_maps_to_length_stop() -> None:
    async def events() -> AsyncIterator[dict[str, object]]:
        yield {"type": "response.incomplete", "response": {}}

    async def response(**_: object) -> object:
        return events()

    assert [
        item async for item in LiteLLMResponsesGateway(response).stream(request())
    ] == [ModelStop(ModelStopReason.LENGTH)]
