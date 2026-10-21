"""Shared Responses wire representation for dispatch and local token counting."""

import json

from crucible.engine.gateway import ModelMessage, ModelRole, ModelToolDefinition


def response_items(message: ModelMessage) -> list[dict[str, object]]:
    text = "".join(
        part.text_content or "" for part in message.parts if part.kind == "text"
    )
    items: list[dict[str, object]] = []
    if message.role is ModelRole.TOOL:
        return [
            {
                "type": "function_call_output",
                "call_id": part.provider_correlation_id or str(part.tool_call_id),
                "output": part.text_content or "",
            }
            for part in message.parts
            if part.kind == "tool_result"
        ]
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


def response_tool(tool: ModelToolDefinition) -> dict[str, object]:
    return {
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": dict(tool.input_schema),
        "strict": False,
    }
