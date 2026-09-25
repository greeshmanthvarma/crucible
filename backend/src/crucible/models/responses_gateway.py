"""Normalize LiteLLM's Responses stream into Crucible's model protocol."""

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

from litellm import aresponses

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelMessage,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
    ReasoningDelta,
    TextDelta,
)

ResponseFunction = Callable[..., Awaitable[object]]


class LiteLLMResponsesGateway:
    def __init__(self, response: ResponseFunction | None = None) -> None:
        self._response = response or cast(ResponseFunction, aresponses)

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        try:
            response = await self._response(
                model=request.model,
                input=[
                    item for message in request.messages for item in _items(message)
                ],
                tools=[_tool(tool) for tool in request.tools] or None,
                max_output_tokens=request.max_output_tokens,
                stream=True,
                store=False,
            )
        except Exception as error:
            yield ModelError("provider_request_failed", str(error)[:1000])
            return

        saw_tool_call = False
        saw_terminal = False
        async for raw_event in _safe_events(cast(AsyncIterator[object], response)):
            if isinstance(raw_event, ModelError):
                yield raw_event
                return
            try:
                event = _mapping(raw_event)
            except TypeError as error:
                yield ModelError("invalid_provider_event", str(error))
                return
            event_type = event.get("type")
            if event_type == "response.output_text.delta":
                delta = event.get("delta")
                if isinstance(delta, str) and delta:
                    yield TextDelta(delta)
            elif event_type == "response.reasoning_summary_text.delta":
                delta = event.get("delta")
                if isinstance(delta, str) and delta:
                    yield ReasoningDelta(delta)
            elif event_type == "response.output_item.done":
                item = event.get("item")
                if not isinstance(item, dict) or item.get("type") != "function_call":
                    continue
                name = item.get("name")
                call_id = item.get("call_id")
                arguments = item.get("arguments")
                if not isinstance(name, str) or not isinstance(call_id, str):
                    yield ModelError(
                        "invalid_tool_call", "Function call lacks name or call_id"
                    )
                    return
                try:
                    parsed = (
                        json.loads(arguments) if isinstance(arguments, str) else None
                    )
                except json.JSONDecodeError as error:
                    yield ModelError("invalid_tool_arguments", str(error))
                    return
                if not isinstance(parsed, dict):
                    yield ModelError(
                        "invalid_tool_arguments", "Tool arguments must be an object"
                    )
                    return
                saw_tool_call = True
                yield CompleteToolCall(new_id(), name, parsed, call_id)
            elif event_type in ("response.completed", "response.incomplete"):
                result = event.get("response")
                if isinstance(result, dict):
                    usage = result.get("usage")
                    if isinstance(usage, dict):
                        yield ModelUsage(
                            _optional_int(usage.get("input_tokens")),
                            _optional_int(usage.get("output_tokens")),
                        )
                reason = (
                    ModelStopReason.LENGTH
                    if event_type == "response.incomplete"
                    else ModelStopReason.TOOL_CALLS
                    if saw_tool_call
                    else ModelStopReason.COMPLETE
                )
                saw_terminal = True
                yield ModelStop(reason)
                return
            elif event_type in ("response.failed", "error"):
                yield ModelError(
                    "provider_stream_failed", str(event.get("error"))[:1000]
                )
                return
        if not saw_terminal:
            yield ModelError("provider_stream_failed", "Responses stream ended early")


async def _safe_events(
    response: AsyncIterator[object],
) -> AsyncIterator[object | ModelError]:
    try:
        async for event in response:
            yield event
    except Exception as error:
        yield ModelError("provider_stream_failed", str(error)[:1000])


def _items(message: ModelMessage) -> list[dict[str, object]]:
    text = "".join(
        part.text_content or "" for part in message.parts if part.kind == "text"
    )
    items: list[dict[str, object]] = []
    if message.role is ModelRole.TOOL:
        for part in message.parts:
            if part.kind == "tool_result":
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": part.provider_correlation_id
                        or str(part.tool_call_id),
                        "output": part.text_content or "",
                    }
                )
        return items
    if text:
        items.append({"role": message.role.value, "content": text})
    for part in message.parts:
        if part.kind == "tool_call":
            items.append(
                {
                    "type": "function_call",
                    "call_id": part.provider_correlation_id or str(part.tool_call_id),
                    "name": part.tool_name or "",
                    "arguments": json.dumps(
                        dict(part.arguments or {}), separators=(",", ":")
                    ),
                }
            )
    return items


def _tool(tool: object) -> dict[str, object]:
    return {
        "type": "function",
        "name": getattr(tool, "name"),
        "description": getattr(tool, "description"),
        "parameters": dict(getattr(tool, "input_schema")),
        "strict": False,
    }


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        result = model_dump()
        if isinstance(result, dict):
            return result
    raise TypeError("Responses event is not mapping-compatible")


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None
