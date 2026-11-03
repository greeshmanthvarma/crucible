"""Pure facts-only reconstruction of complete bounded solver wire records."""

import json
from dataclasses import fields
from typing import Literal
from uuid import UUID

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
)
from crucible.domain.tools import ToolCall, ToolResult
from crucible.improvement.trajectory_contracts import (
    SolverTrace,
    TraceCompleteness,
    TraceWire,
    VerifiedTrial,
)
from crucible.repair.identity import canonical_json

ErrorCode = Literal[
    "invalid_trace",
    "identity_mismatch",
    "ambiguous_order",
    "export_limit",
    "artifact_drift",
    "unsupported_trace",
    "unsafe_path",
    "no_failed_development_trial",
]


class TrajectoryError(ValueError):
    def __init__(self, code: ErrorCode, message: str = "") -> None:
        self.code = code
        super().__init__(code + (": " + message[:200] if message else ""))


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TrajectoryError("invalid_trace", "duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value: str) -> object:
    raise TrajectoryError("invalid_trace", "nonfinite JSON number")


def decode_solver_trace(
    raw: bytes, *, trial: VerifiedTrial, maximum: int
) -> SolverTrace:
    if maximum <= 0 or len(raw) > maximum:
        raise TrajectoryError("export_limit", "input trace cap")
    try:
        data = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_invalid_constant,
        )
        if (
            isinstance(data, dict)
            and set(data) == {"trust", "data"}
            and data["trust"] == "untrusted-worker-observation"
        ):
            data = data["data"]
        if not isinstance(data, dict) or data.get("schema_version") != 2:
            raise TrajectoryError("unsupported_trace")
        if type(data["schema_version"]) is not int:
            raise TrajectoryError("unsupported_trace")
        for key, cls in (
            ("messages", Message),
            ("tool_calls", ToolCall),
            ("tool_results", ToolResult),
        ):
            if not isinstance(data.get(key), list):
                raise TrajectoryError("invalid_trace")
            for item in data[key]:
                if not isinstance(item, dict) or set(item) != {
                    f.name for f in fields(cls)
                }:
                    raise TrajectoryError("invalid_trace", "incomplete wire record")
                if key == "messages":
                    if not isinstance(item["parts"], list) or any(
                        not isinstance(p, dict)
                        or set(p) != {f.name for f in fields(MessagePart)}
                        for p in item["parts"]
                    ):
                        raise TrajectoryError("invalid_trace", "incomplete wire part")
                elif (
                    type(item["schema_version"]) is not int
                    or item["schema_version"] != 1
                ):
                    raise TrajectoryError("unsupported_trace", "record schema")
        wire = TraceWire.model_validate_json(canonical_json(data))
    except TrajectoryError:
        raise
    except (ValueError, UnicodeError, TypeError, RecursionError) as error:
        raise TrajectoryError("invalid_trace") from error
    all_records: tuple[Message | ToolCall | ToolResult, ...] = (
        *wire.messages,
        *wire.tool_calls,
        *wire.tool_results,
    )
    ids = [r.id for r in all_records]
    parts = [p for m in wire.messages for p in m.parts]
    ids.extend(p.id for p in parts)
    if len(set(ids)) != len(ids):
        raise TrajectoryError("identity_mismatch", "duplicate record identity")
    if any(r.task_id != trial.task_id or r.run_id != trial.run_id for r in all_records):
        raise TrajectoryError("identity_mismatch", "run/task mismatch")
    if any(m.conversation_sequence < 1 for m in wire.messages) or [
        m.conversation_sequence for m in wire.messages
    ] != sorted({m.conversation_sequence for m in wire.messages}):
        raise TrajectoryError("ambiguous_order", "message order")
    messages = {m.id: m for m in wire.messages}
    calls = {c.id: c for c in wire.tool_calls}
    results = {r.id: r for r in wire.tool_results}
    for records, field in (
        (wire.tool_calls, "call_sequence"),
        (wire.tool_results, "completion_sequence"),
    ):
        by_step: dict[UUID, list[int]] = {}
        for record in records:
            by_step.setdefault(record.step_id, []).append(getattr(record, field))
        if any(values != sorted(set(values)) for values in by_step.values()):
            raise TrajectoryError("ambiguous_order", "structured sequence")
    completed: set[UUID] = set()
    for result in wire.tool_results:
        call = calls.get(result.tool_call_id)
        if call is None or result.step_id != call.step_id or call.id in completed:
            raise TrajectoryError("identity_mismatch", "orphan or conflicting result")
        completed.add(call.id)
    for call in wire.tool_calls:
        if (
            call.assistant_message_id in ids
            and call.assistant_message_id not in messages
        ):
            raise TrajectoryError("identity_mismatch", "conflicting assistant identity")
        anchor = messages.get(call.assistant_message_id)
        if anchor is not None and (
            anchor.role != MessageRole.ASSISTANT or anchor.step_id != call.step_id
        ):
            raise TrajectoryError("identity_mismatch", "assistant link")
    unresolved: list[UUID] = []
    anchored_calls: list[UUID] = []
    anchored_results: list[UUID] = []
    for message in wire.messages:
        sequences = [p.part_sequence for p in message.parts]
        if any(n < 1 for n in sequences) or sequences != sorted(set(sequences)):
            raise TrajectoryError("ambiguous_order", "part order")
        for part in message.parts:
            is_call = part.kind == MessagePartKind.TOOL_CALL
            is_result = part.kind == MessagePartKind.TOOL_RESULT
            if (part.tool_call_id is not None) != is_call or (
                part.tool_result_id is not None
            ) != is_result:
                raise TrajectoryError("identity_mismatch", "part links")
            target = part.tool_call_id if is_call else part.tool_result_id
            if target is None:
                continue
            if message.role != (MessageRole.ASSISTANT if is_call else MessageRole.TOOL):
                raise TrajectoryError("identity_mismatch", "part role")
            item = calls.get(target) if is_call else results.get(target)
            if item is None:
                if target in ids:
                    raise TrajectoryError(
                        "identity_mismatch", "conflicting part identity"
                    )
                unresolved.append(target)
                continue
            if message.step_id != item.step_id or (
                isinstance(item, ToolCall) and item.assistant_message_id != message.id
            ):
                raise TrajectoryError("identity_mismatch", "part anchor")
            anchors = anchored_calls if is_call else anchored_results
            if target in anchors:
                raise TrajectoryError("identity_mismatch", "duplicate part reference")
            anchors.append(target)
    # Admission and completion each have their own ordering, never timestamp merged.
    for records, anchors in (
        (wire.tool_calls, anchored_calls),
        (wire.tool_results, anchored_results),
    ):
        if [r.id for r in records if r.id in anchors] != anchors:
            raise TrajectoryError(
                "ambiguous_order", "anchors disagree with source order"
            )
    steps = [m.step_id for m in wire.messages if m.step_id is not None]
    compressed = [s for i, s in enumerate(steps) if not i or s != steps[i - 1]]
    if len(set(compressed)) != len(compressed):
        raise TrajectoryError("ambiguous_order", "interleaved step groups")
    rank = {step: i for i, step in enumerate(compressed)}
    for structured in (wire.tool_calls, wire.tool_results):
        groups = [r.step_id for r in structured]
        groups = [s for i, s in enumerate(groups) if not i or s != groups[i - 1]]
        known = [rank[s] for s in groups if s in rank]
        if len(groups) != len(set(groups)) or known != sorted(known):
            raise TrajectoryError("ambiguous_order", "structured step groups")
    return SolverTrace(
        2,
        wire.messages,
        wire.tool_calls,
        wire.tool_results,
        TraceCompleteness(
            missing_result_call_ids=tuple(
                c.id for c in wire.tool_calls if c.id not in completed
            ),
            unresolved_part_refs=tuple(unresolved),
        ),
    )
