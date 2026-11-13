"""Shorten older coding observations only in outgoing model requests."""

from dataclasses import replace

from crucible.context.invariants import TokenEstimator
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import ModelMessage, ModelRole

KEEP_RECENT_BATCHES = 2
SHORTEN_ABOVE_CHARS = 1_200
PREVIEW_CHARS = 400
SHORTENED_NOTICE = (
    "\n[Earlier output shortened to save context. Repeat a narrower call if needed.]"
)
PROJECTED_TOOLS = frozenset(
    {
        "list_files",
        "read_file",
        "search_files",
        "workspace_diff",
        "workspace_status",
        "execute_command",
    }
)


def project_coding_tool_results(
    messages: tuple[ModelMessage, ...],
    observation: BehaviorObservation,
    estimated_input_tokens: int,
    max_output_tokens: int,
    estimator: TokenEstimator,
) -> tuple[ModelMessage, ...]:
    batches = [
        tuple(part.tool_call_id for part in message.parts if part.kind == "tool_call")
        for message in messages
        if message.role is ModelRole.ASSISTANT
        and any(part.kind == "tool_call" for part in message.parts)
    ]
    old_calls = {
        call_id for batch in batches[:-KEEP_RECENT_BATCHES] for call_id in batch
    }
    return tuple(
        replace(
            message,
            parts=tuple(
                replace(
                    part,
                    text_content=part.text_content[:PREVIEW_CHARS] + SHORTENED_NOTICE,
                )
                if part.kind == "tool_result"
                and part.tool_call_id in old_calls
                and part.tool_name in PROJECTED_TOOLS
                and part.text_content is not None
                and len(part.text_content) > SHORTEN_ABOVE_CHARS
                else part
                for part in message.parts
            ),
        )
        for message in messages
    )
