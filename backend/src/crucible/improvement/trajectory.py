"""Pure facts-only reconstruction of complete bounded solver wire records."""

import hashlib
import json
from dataclasses import fields
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
)
from crucible.domain.tools import ToolCall, ToolResult
from crucible.improvement.roles import EvidenceRef
from crucible.improvement.trajectory_contracts import (
    DetailEntry,
    ExportLimits,
    SolverTrace,
    TraceCompleteness,
    TraceWire,
    TrajectoryBundle,
    TrajectoryRecord,
    TrialOverview,
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
    prefix: Literal["", "/data"] = ""
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
            prefix = "/data"
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
        prefix,
    )


def build_trajectory(
    trace: SolverTrace, trial: VerifiedTrial, limits: ExportLimits
) -> TrajectoryBundle:
    """Build navigation bytes; display sequence carries no global runtime claim."""
    try:
        solution = trial.solution_bytes.decode("utf-8")
    except UnicodeError as error:
        raise TrajectoryError("invalid_trace", "solution encoding") from error
    files: dict[str, bytes] = {}
    entries: list[DetailEntry] = []

    def detail(
        kind: Literal[
            "message", "arguments", "result", "task_instruction", "solution_diff"
        ],
        record_id: UUID | None,
        content: object,
        pointer: str | None,
        artifact: EvidenceRef,
    ) -> str:
        name = f"details/{record_id or trial.trial_id}.{kind}.json"
        raw = canonical_json(content)
        metadata: dict[str, object] = {
            "kind": kind,
            "original_record_id": record_id,
            "content_sha256": hashlib.sha256(raw).hexdigest(),
            "original_bytes": len(raw),
            "truncated": False,
            "content": content,
        }
        envelope = canonical_json(metadata)
        names: tuple[str, ...] = (name,)
        truncated = len(envelope) > limits.detail_bytes
        if truncated:
            text_name = name.removesuffix(".json") + ".excerpt.txt"
            metadata.update(
                truncated=True,
                content=None,
                excerpt_ref=text_name,
                excerpt_encoding="canonical-json-utf8-prefix",
                excerpt_notice="Incomplete excerpt; not a parseable record",
            )
            envelope = canonical_json(metadata)
            remaining = limits.detail_bytes - len(envelope)
            if remaining < 1:
                raise TrajectoryError("export_limit", "detail metadata")
            files[text_name] = (
                raw[:remaining].decode("utf-8", errors="ignore").encode("utf-8")
            )
            names += (text_name,)
        files[name] = envelope
        if len(envelope) > limits.detail_bytes:
            raise TrajectoryError("export_limit", "detail envelope")
        entries.append(
            DetailEntry(
                ref=name,
                kind=kind,
                original_record_id=record_id,
                original_artifact=artifact,
                original_json_pointer=trace.original_pointer_prefix + pointer
                if pointer is not None
                else None,
                content_sha256=hashlib.sha256(raw).hexdigest(),
                original_bytes=len(raw),
                exported_bytes=sum(len(files[n]) for n in names),
                truncated=truncated,
                files=names,
            )
        )
        return name

    details: dict[UUID, str] = {}
    for i, message in enumerate(trace.messages):
        content = json.loads(canonical_json(message))
        # Structured payloads live once. Retain the surrounding independent text parts.
        for part in content["parts"]:
            if part["kind"] in ("tool_call", "tool_result"):
                part["text_content"] = None
                part["reasoning_content"] = None
        details[message.id] = detail(
            "message", message.id, content, f"/messages/{i}", trial.trace_ref
        )
    for i, call in enumerate(trace.tool_calls):
        details[call.id] = detail(
            "arguments",
            call.id,
            dict(call.arguments),
            f"/tool_calls/{i}/arguments",
            trial.trace_ref,
        )
    for i, result in enumerate(trace.tool_results):
        details[result.id] = detail(
            "result",
            result.id,
            {"result": dict(result.result), "display_text": result.display_text},
            f"/tool_results/{i}",
            trial.trace_ref,
        )
    initial = next(
        (m for m in trace.messages if m.role == MessageRole.USER and m.step_id is None),
        None,
    )
    instruction = None
    if initial is not None:
        instruction = detail(
            "task_instruction",
            initial.id,
            [p.text_content for p in initial.parts if p.kind == MessagePartKind.TEXT],
            f"/messages/{trace.messages.index(initial)}/parts",
            trial.trace_ref,
        )
    detail(
        "solution_diff",
        None,
        solution,
        None,
        trial.solution_ref,
    )
    calls = {c.id: c for c in trace.tool_calls}
    results = {r.id: r for r in trace.tool_results}
    ordered: list[Message | ToolCall | ToolResult] = []
    seen: set[UUID] = set()
    for message in trace.messages:
        ordered.append(message)
        for part in message.parts:
            target = (
                calls.get(part.tool_call_id)
                if part.tool_call_id
                else results.get(part.tool_result_id)
                if part.tool_result_id
                else None
            )
            if target is not None:
                ordered.append(target)
                seen.add(target.id)
    structured: tuple[ToolCall | ToolResult, ...] = (
        *trace.tool_calls,
        *trace.tool_results,
    )
    unanchored = [r for r in structured if r.id not in seen]
    ordered.extend(unanchored)
    lines: list[bytes] = []
    for sequence, record in enumerate(ordered, 1):
        record_call: ToolCall | None = None
        linked: tuple[UUID, ...]
        if isinstance(record, Message):
            linked = tuple(
                linked_target
                for p in record.parts
                if (linked_target := p.tool_call_id or p.tool_result_id) is not None
            )
        elif isinstance(record, ToolCall):
            linked = (record.assistant_message_id,)
            record_call = record
        else:
            linked = (record.tool_call_id,)
            record_call = calls[record.tool_call_id]
        entry = TrajectoryRecord(
            sequence=sequence,
            kind="message"
            if isinstance(record, Message)
            else "tool_call"
            if isinstance(record, ToolCall)
            else "tool_result",
            record_id=record.id,
            step_id=record.step_id,
            conversation_sequence=record.conversation_sequence
            if isinstance(record, Message)
            else None,
            call_sequence=record.call_sequence
            if isinstance(record, ToolCall)
            else None,
            completion_sequence=record.completion_sequence
            if isinstance(record, ToolResult)
            else None,
            call_id=record_call.id if record_call else None,
            result_id=record.id if isinstance(record, ToolResult) else None,
            tool=record_call.name if record_call else None,
            status=str(record.status),
            error_code=record.error_code if isinstance(record, ToolResult) else None,
            execution_mode=record_call.execution_mode.value if record_call else None,
            detail_ref=details[record.id],
            linked_ids=linked,
        )
        line = canonical_json(entry) + b"\n"
        if len(line) > limits.index_record_bytes:
            raise TrajectoryError("export_limit", "index record")
        lines.append(line)
    files["trajectory.jsonl"] = b"".join(lines)
    if len(files["trajectory.jsonl"]) > limits.index_bytes:
        raise TrajectoryError("export_limit", "index total")
    files["details-manifest.json"] = canonical_json(entries)
    if len(files["details-manifest.json"]) > limits.detail_bytes:
        raise TrajectoryError("export_limit", "details manifest")
    signatures = [canonical_json([c.name, dict(c.arguments)]) for c in trace.tool_calls]
    completeness = trace.completeness.model_copy(
        update={"truncated_detail_refs": tuple(e.ref for e in entries if e.truncated)}
    )
    overview = TrialOverview(
        trial_id=trial.trial_id,
        case_id=trial.case_id,
        repeat_index=trial.repeat_index,
        run_id=trial.run_id,
        task_id=trial.task_id,
        source=trial.source,
        verdict=trial.verdict,
        termination=trial.termination,
        task_instruction_ref=instruction,
        usage=trial.usage,
        usage_available=trial.usage is not None,
        task_instruction_available=instruction is not None,
        ordering_basis="conversation-anchors-with-unanchored-source-order"
        if unanchored
        else "conversation-anchors",
        message_count=len(trace.messages),
        call_count=len(trace.tool_calls),
        result_count=len(trace.tool_results),
        error_result_count=sum(r.status != "succeeded" for r in trace.tool_results),
        exact_repeated_call_count=len(signatures) - len(set(signatures)),
        completeness=completeness,
        index_ref="trajectory.jsonl",
        details_manifest_ref="details-manifest.json",
    )
    files["overview.json"] = canonical_json(overview)
    if (
        len(files["overview.json"]) > limits.overview_bytes
        or sum(map(len, files.values())) > limits.evidence_bytes
    ):
        raise TrajectoryError("export_limit", "overview or evidence total")
    return TrajectoryBundle(
        trial.trial_id,
        MappingProxyType(files),
        overview,
        (trial.trace_ref, trial.solution_ref),
    )
