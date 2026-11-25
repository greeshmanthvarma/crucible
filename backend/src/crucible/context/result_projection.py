"""Shorten older coding observations only in outgoing model requests."""

from dataclasses import replace
from pathlib import PurePosixPath

from crucible.context.invariants import TokenEstimator
from crucible.domain.ids import ToolCallId
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole
from crucible.tools.workspace_authority import patch_paths

KEEP_RECENT_RESULTS = 3
MASK_ABOVE_CHARS = 200
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


def mask_older_results(messages: tuple[ModelMessage, ...]) -> tuple[ModelMessage, ...]:
    protected = _protected_results(messages)
    calls = {
        part.tool_call_id: part
        for message in messages
        for part in message.parts
        if part.kind == "tool_call"
    }
    eligible = [
        part
        for message in messages
        for part in message.parts
        if part.kind == "tool_result"
        and part.tool_name in PROJECTED_TOOLS
        and len(part.text_content or "") > MASK_ABOVE_CHARS
    ]
    stubs = {
        part.tool_call_id: f"[masked {part.tool_name}: "
        f"{_target(calls.get(part.tool_call_id))}; call again to view]"
        for part in eligible[:-KEEP_RECENT_RESULTS]
        if part.tool_call_id is not None and part.tool_call_id not in protected
    }
    return (
        tuple(
            replace(
                message,
                parts=tuple(
                    replace(part, text_content=stubs[part.tool_call_id])
                    if part.kind == "tool_result" and part.tool_call_id in stubs
                    else part
                    for part in message.parts
                ),
            )
            for message in messages
        )
        if stubs
        else messages
    )


def _protected_results(messages: tuple[ModelMessage, ...]) -> set[ToolCallId]:
    protected = set()
    read_paths: dict[ToolCallId, PurePosixPath] = {}
    latest_reads: dict[PurePosixPath, ToolCallId] = {}
    latest_failure = None
    for message in messages:
        for part in message.parts:
            call_id = part.tool_call_id
            if call_id is None:
                continue
            if part.kind == "tool_call":
                args = part.arguments or {}
                path = args.get("path")
                if part.tool_name == "read_file" and isinstance(path, str):
                    read_paths[call_id] = PurePosixPath(path)
                for target in _edit_paths(part):
                    read_id = latest_reads.get(PurePosixPath(target))
                    if read_id is not None:
                        protected.add(read_id)
            elif part.kind == "tool_result":
                if call_id in read_paths:
                    latest_reads[read_paths[call_id]] = call_id
                if part.tool_name == "execute_command" and part.exit_code:
                    latest_failure = call_id
    if latest_failure is not None:
        protected.add(latest_failure)
    return protected


def _edit_paths(call: ModelPart) -> tuple[str, ...]:
    args = call.arguments or {}
    if call.tool_name in {"edit_file", "write_file"}:
        path = args.get("path")
        return (path,) if isinstance(path, str) else ()
    patch = args.get("patch")
    if call.tool_name == "apply_patch" and isinstance(patch, str):
        try:
            return patch_paths(patch)
        except ValueError:
            return ()
    return ()


def _target(call: ModelPart | None) -> str:
    if call is None:
        return "workspace"
    args = call.arguments or {}
    path = args.get("path", ".")
    match call.tool_name:
        case "read_file":
            start = args.get("start_line", 1)
            end = args.get("end_line")
            target = f"{path} lines {start}-{end if end is not None else 'EOF'}"
        case "search_files":
            target = f"{args.get('query', '')!r} in {path}"
        case "list_files":
            target = str(path)
        case "execute_command":
            arguments = args.get("arguments", [])
            argv = arguments if isinstance(arguments, (tuple, list)) else ()
            target = " ".join([str(args.get("executable", "")), *map(str, argv)])[:120]
        case _:
            return "workspace"
    return " ".join(target.splitlines()) or "workspace"


def project_coding_tool_results(
    messages: tuple[ModelMessage, ...],
    observation: BehaviorObservation,
    estimated_input_tokens: int,
    max_output_tokens: int,
    estimator: TokenEstimator,
) -> tuple[ModelMessage, ...]:
    masked = mask_older_results(messages)
    original_estimate = estimator.estimate(masked)
    savings = (
        estimator.estimate(messages) - original_estimate
        if masked is not messages
        else 0
    )
    excess = estimated_input_tokens - savings - request_allowance(observation)
    if excess <= 0:
        return masked
    protected = _protected_results(messages)
    replaced = {
        before.tool_call_id
        for old, new in zip(messages, masked, strict=True)
        for before, after in zip(old.parts, new.parts, strict=True)
        if before != after
    }
    messages = masked
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
        and part.tool_call_id not in protected
        and part.tool_call_id not in replaced
        and part.tool_name in PROJECTED_TOOLS
        and part.text_content is not None
        and len(part.text_content) > SHORTEN_ABOVE_CHARS
    }
    shortened = set()
    projected = messages
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
    messages: tuple[ModelMessage, ...], shortened: set[ToolCallId | None]
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
