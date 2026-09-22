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


async def test_malformed_arguments_become_owned_error() -> None:
    async def stream() -> AsyncIterator[dict[str, object]]:
        yield {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {
                                    "name": "read_file",
                                    "arguments": "{not-json",
                                },
                            }
                        ]
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        }

    async def completion(**_: object) -> object:
        return stream()

    items = [item async for item in LiteLLMModelGateway(completion).stream(request())]

    assert isinstance(items[0], ModelError)
    assert items[0].code == "invalid_tool_arguments"
    assert items[-1] == ModelStop(ModelStopReason.TOOL_CALLS)


async def test_duplicate_provider_ids_receive_distinct_owned_ids() -> None:
    async def stream() -> AsyncIterator[dict[str, object]]:
        yield {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": index,
                                "id": "duplicate-provider-id",
                                "function": {
                                    "name": "read_file",
                                    "arguments": '{"path":"README.md"}',
                                },
                            }
                            for index in (0, 1)
                        ]
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        }

    async def completion(**_: object) -> object:
        return stream()

    items = [item async for item in LiteLLMModelGateway(completion).stream(request())]
    calls = [item for item in items if isinstance(item, CompleteToolCall)]

    assert len(calls) == 2
    assert calls[0].id != calls[1].id
    assert {call.provider_correlation_id for call in calls} == {"duplicate-provider-id"}


async def test_serializes_structured_tool_exchange_for_follow_up_request() -> None:
    call_id = new_id()
    captured: dict[str, object] = {}

    async def empty_stream() -> AsyncIterator[dict[str, object]]:
        if False:
            yield {}

    async def completion(**kwargs: object) -> object:
        captured.update(kwargs)
        return empty_stream()

    prepared = PreparedModelRequest(
        new_id(),
        new_id(),
        "openai/fixture",
        (
            ModelMessage(
                ModelRole.ASSISTANT,
                (
                    ModelPart("text", "Checking."),
                    ModelPart(
                        "tool_call",
                        tool_call_id=call_id,
                        tool_name="read_file",
                        arguments={"path": "README.md"},
                    ),
                ),
            ),
            ModelMessage(
                ModelRole.TOOL,
                (ModelPart("tool_result", "fixture\n", tool_call_id=call_id),),
            ),
        ),
        (),
        100,
    )

    assert [
        item async for item in LiteLLMModelGateway(completion).stream(prepared)
    ] == []
    assert captured["messages"] == [
        {
            "role": "assistant",
            "content": "Checking.",
            "tool_calls": [
                {
                    "id": str(call_id),
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": '{"path":"README.md"}',
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": str(call_id), "content": "fixture\n"},
    ]


async def test_normalizes_provider_request_and_stream_failures() -> None:
    async def failed_request(**_: object) -> object:
        raise RuntimeError("request boom")

    request_items = [
        item async for item in LiteLLMModelGateway(failed_request).stream(request())
    ]
    assert request_items == [ModelError("provider_request_failed", "request boom")]

    async def failed_stream() -> AsyncIterator[dict[str, object]]:
        raise RuntimeError("stream boom")
        yield {}

    async def completion(**_: object) -> object:
        return failed_stream()

    stream_items = [
        item async for item in LiteLLMModelGateway(completion).stream(request())
    ]
    assert stream_items == [ModelError("provider_stream_failed", "stream boom")]
