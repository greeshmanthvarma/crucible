import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

from litellm import acompletion

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
    ReasoningDelta,
    TextDelta,
)

CompletionFunction = Callable[..., Awaitable[object]]


class LiteLLMModelGateway:
    def __init__(self, completion: CompletionFunction | None = None) -> None:
        self._completion = completion or cast(CompletionFunction, acompletion)

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        response = await self._completion(
            model=request.model,
            messages=[_message(message) for message in request.messages],
            tools=[_tool(tool) for tool in request.tools] or None,
            max_tokens=request.max_output_tokens,
            stream=True,
            stream_options={"include_usage": True},
        )
        fragments: dict[int, dict[str, str]] = {}
        async for raw_chunk in cast(AsyncIterator[object], response):
            chunk = _mapping(raw_chunk)
            usage = chunk.get("usage")
            if isinstance(usage, dict):
                yield ModelUsage(
                    _optional_int(usage.get("prompt_tokens")),
                    _optional_int(usage.get("completion_tokens")),
                )
            choices = chunk.get("choices")
            if not isinstance(choices, list):
                continue
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                delta = choice.get("delta")
                if isinstance(delta, dict):
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        yield TextDelta(content)
                    reasoning = delta.get("reasoning_content") or delta.get("reasoning")
                    if isinstance(reasoning, str) and reasoning:
                        yield ReasoningDelta(reasoning)
                    for fragment in delta.get("tool_calls") or []:
                        if not isinstance(fragment, dict):
                            continue
                        index = int(fragment.get("index", 0))
                        current = fragments.setdefault(
                            index, {"id": "", "name": "", "arguments": ""}
                        )
                        if isinstance(fragment.get("id"), str):
                            current["id"] += fragment["id"]
                        function = fragment.get("function")
                        if isinstance(function, dict):
                            if isinstance(function.get("name"), str):
                                current["name"] += function["name"]
                            if isinstance(function.get("arguments"), str):
                                current["arguments"] += function["arguments"]
                finish = choice.get("finish_reason")
                if isinstance(finish, str):
                    for index in sorted(fragments):
                        fragment = fragments[index]
                        try:
                            arguments = json.loads(fragment["arguments"] or "{}")
                        except json.JSONDecodeError as error:
                            yield ModelError("invalid_tool_arguments", str(error))
                            continue
                        if not isinstance(arguments, dict):
                            yield ModelError(
                                "invalid_tool_arguments",
                                "Tool arguments must decode to an object",
                            )
                            continue
                        yield CompleteToolCall(
                            new_id(),
                            fragment["name"],
                            arguments,
                            fragment["id"] or None,
                        )
                    fragments.clear()
                    yield ModelStop(_stop_reason(finish))


def _message(message: object) -> dict[str, object]:
    role = getattr(message, "role")
    parts = getattr(message, "parts")
    content = "".join(getattr(part, "text_content") or "" for part in parts)
    return {"role": str(role), "content": content}


def _tool(tool: object) -> dict[str, object]:
    return {
        "type": "function",
        "function": {
            "name": getattr(tool, "name"),
            "description": getattr(tool, "description"),
            "parameters": dict(getattr(tool, "input_schema")),
        },
    }


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    method = getattr(value, "model_dump", None)
    if callable(method):
        dumped = method()
        if isinstance(dumped, dict):
            return dumped
    raise TypeError("LiteLLM stream chunk is not mapping-compatible")


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _stop_reason(value: str) -> ModelStopReason:
    if value in {"tool_calls", "function_call"}:
        return ModelStopReason.TOOL_CALLS
    if value == "length":
        return ModelStopReason.LENGTH
    return ModelStopReason.COMPLETE
