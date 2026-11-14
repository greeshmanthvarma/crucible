"""Shorten older coding observations only in outgoing model requests."""

from dataclasses import replace
from uuid import UUID

from crucible.context.invariants import TokenEstimator
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import ModelMessage, ModelRole

KEEP_RECENT_BATCHES = 2
SHORTEN_ABOVE_CHARS = 1_200
PREVIEW_CHARS = 400
SHORTENED_NOTICE = (
    "\n[Older output shortened to fit the token budget. Use your earlier notes; "
    "re-read only the exact lines you need for an edit.]"
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


def request_allowance(observation: BehaviorObservation) -> int:
    return observation.remaining_model_tokens // (observation.remaining_steps + 1)


def project_coding_tool_results(
    messages: tuple[ModelMessage, ...],
    observation: BehaviorObservation,
    estimated_input_tokens: int,
    max_output_tokens: int,
    estimator: TokenEstimator,
) -> tuple[ModelMessage, ...]:
    excess = estimated_input_tokens - request_allowance(observation)
    if excess <= 0:
        return messages
    batches = [
        tuple(part.tool_call_id for part in message.parts if part.kind == "tool_call")
        for message in messages
        if message.role is ModelRole.ASSISTANT
        and any(part.kind == "tool_call" for part in message.parts)
    ]
    results = {
        part.tool_call_id: part
        for message in messages
        for part in message.parts
        if part.kind == "tool_result"
        and part.tool_name in PROJECTED_TOOLS
        and part.text_content is not None
        and len(part.text_content) > SHORTEN_ABOVE_CHARS
    }
    shortened = set()
    projected = messages
    original_estimate = estimator.estimate(messages)
    for batch in batches[:-KEEP_RECENT_BATCHES]:
        for call_id in batch:
            if call_id not in results:
                continue
            shortened.add(call_id)
            projected = _shorten(messages, shortened)
            if original_estimate - estimator.estimate(projected) >= excess:
                return projected
    return projected


def _shorten(
    messages: tuple[ModelMessage, ...], shortened: set[UUID | None]
) -> tuple[ModelMessage, ...]:
    return tuple(
        replace(
            message,
            parts=tuple(
                replace(
                    part,
                    text_content=part.text_content[:PREVIEW_CHARS] + SHORTENED_NOTICE,
                )
                if part.kind == "tool_result"
                and part.tool_call_id in shortened
                and part.tool_name in PROJECTED_TOOLS
                and part.text_content is not None
                and len(part.text_content) > SHORTEN_ABOVE_CHARS
                else part
                for part in message.parts
            ),
        )
        for message in messages
    )
