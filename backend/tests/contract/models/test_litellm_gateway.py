from collections.abc import AsyncIterator

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelMessage,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)
from crucible.models.litellm_gateway import LiteLLMModelGateway


def request() -> PreparedModelRequest:
    return PreparedModelRequest(
        new_id(),
        new_id(),
        "openai/fixture",
        (ModelMessage(ModelRole.USER, ()),),
        (),
        100,
    )


async def fixture_stream() -> AsyncIterator[dict[str, object]]:
    yield {
        "choices": [
            {
                "delta": {
                    "content": "Inspecting",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "provider-call",
                            "function": {
                                "name": "read_file",
                                "arguments": '{"path":',
                            },
                        }
                    ],
                },
                "finish_reason": None,
            }
        ]
    }
    yield {
        "choices": [
            {
                "delta": {
                    "tool_calls": [
                        {"index": 0, "function": {"arguments": '"README.md"}'}}
                    ]
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 12, "completion_tokens": 4},
    }


async def test_translates_fragmented_provider_chunks_to_owned_protocol() -> None:
    captured: dict[str, object] = {}

    async def completion(**kwargs: object) -> object:
        captured.update(kwargs)
        return fixture_stream()

    items = [item async for item in LiteLLMModelGateway(completion).stream(request())]

    assert items[0] == TextDelta("Inspecting")
    call = next(item for item in items if isinstance(item, CompleteToolCall))
    assert call.arguments == {"path": "README.md"}
    assert call.provider_correlation_id == "provider-call"
    assert ModelUsage(12, 4) in items
    assert ModelStop(ModelStopReason.TOOL_CALLS) in items
    assert captured["stream"] is True
