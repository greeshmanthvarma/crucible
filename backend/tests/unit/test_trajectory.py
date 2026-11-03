"""Pure solver trajectory decoding at the approved serialized boundary."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.tools import (
    ToolCall,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.improvement.contracts import SourceIdentity
from crucible.improvement.roles import EvidenceRef
from crucible.improvement.trajectory import TrajectoryError, decode_solver_trace
from crucible.improvement.trajectory_contracts import VerifiedTrial
from crucible.repair.identity import canonical_json


def uid(n):
    return UUID(int=n)


@pytest.fixture
def parallel_records():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    calls = tuple(
        ToolCall(
            uid(i),
            uid(1),
            uid(2),
            uid(3),
            uid(4),
            i - 9,
            "read_file",
            {"path": "a.py"},
            1,
            None,
            ToolExecutionMode.PARALLEL,
            now,
        )
        for i in (10, 11)
    )
    results = tuple(
        ToolResult(
            uid(i),
            uid(1),
            uid(2),
            uid(3),
            calls[1 - j].id,
            ToolResultStatus.SUCCEEDED,
            {"text": "same"},
            1,
            "same",
            None,
            j + 1,
            now,
            now,
        )
        for j, i in enumerate((20, 21))
    )
    messages = (
        Message(
            uid(5),
            uid(1),
            uid(2),
            None,
            1,
            MessageRole.USER,
            MessageStatus.COMPLETED,
            (MessagePart(uid(30), 1, MessagePartKind.TEXT, "Fix the task"),),
            now,
            now,
        ),
        Message(
            uid(4),
            uid(1),
            uid(2),
            uid(3),
            2,
            MessageRole.ASSISTANT,
            MessageStatus.COMPLETED,
            tuple(
                MessagePart(
                    uid(31 + j),
                    j + 1,
                    MessagePartKind.TOOL_CALL,
                    None,
                    tool_call_id=c.id,
                )
                for j, c in enumerate(calls)
            ),
            now,
            now,
        ),
        Message(
            uid(6),
            uid(1),
            uid(2),
            uid(3),
            3,
            MessageRole.TOOL,
            MessageStatus.COMPLETED,
            tuple(
                MessagePart(
                    uid(33 + j),
                    j + 1,
                    MessagePartKind.TOOL_RESULT,
                    None,
                    tool_result_id=r.id,
                )
                for j, r in enumerate(results)
            ),
            now,
            now,
        ),
    )
    return {
        "schema_version": 2,
        "messages": messages,
        "tool_calls": calls,
        "tool_results": results,
    }


@pytest.fixture
def parallel_trace(parallel_records):
    return canonical_json(parallel_records)


@pytest.fixture
def verified_trial(parallel_trace):
    import hashlib

    return VerifiedTrial(
        uid(40),
        "development",
        0,
        uid(2),
        uid(1),
        "fail",
        "completed",
        SourceIdentity(commit="a" * 40, source_digest="b" * 64, export_digest="c" * 64),
        EvidenceRef(
            trial_id=uid(40),
            artifact="trace",
            sha256=hashlib.sha256(parallel_trace).hexdigest(),
        ),
        parallel_trace,
        EvidenceRef(
            trial_id=uid(40),
            artifact="solution_diff",
            sha256=hashlib.sha256(b"diff").hexdigest(),
        ),
        b"diff",
        None,
    )


def decode(raw, trial):
    return decode_solver_trace(raw, trial=trial, maximum=8_388_608)


def test_parallel_admission_and_completion_are_distinct(parallel_trace, verified_trial):
    trace = decode(parallel_trace, verified_trial)
    assert [c.call_sequence for c in trace.tool_calls] == [1, 2]
    assert [r.completion_sequence for r in trace.tool_results] == [1, 2]
    assert trace.tool_results[0].tool_call_id == trace.tool_calls[1].id


def test_message_parts_reference_single_tool_payload(parallel_trace, verified_trial):
    trace = decode(parallel_trace, verified_trial)
    assert trace.messages[1].parts[0].tool_call_id == trace.tool_calls[0].id
    assert trace.messages[2].parts[0].tool_result_id == trace.tool_results[0].id
    assert trace.messages[1].parts[0].text_content is None


def test_missing_result_is_explicit(parallel_records, verified_trial):
    parallel_records["tool_results"] = parallel_records["tool_results"][:1]
    trace = decode(canonical_json(parallel_records), verified_trial)
    assert trace.completeness.missing_result_call_ids == (uid(10),)
    assert trace.completeness.unresolved_part_refs == (uid(21),)


def test_duplicate_structured_ids_rejected(parallel_records, verified_trial):
    parallel_records["tool_calls"] = parallel_records["tool_calls"] * 2
    with pytest.raises(TrajectoryError, match="identity_mismatch"):
        decode(canonical_json(parallel_records), verified_trial)


def test_cross_run_result_rejected(parallel_records, verified_trial):
    parallel_records["tool_results"] = (
        replace(parallel_records["tool_results"][0], run_id=uid(999)),
    )
    with pytest.raises(TrajectoryError, match="identity_mismatch"):
        decode(canonical_json(parallel_records), verified_trial)


def test_equal_text_different_ids_remain_distinct(parallel_trace, verified_trial):
    trace = decode(parallel_trace, verified_trial)
    assert len(trace.tool_calls) == 2
    assert trace.tool_calls[0].arguments == trace.tool_calls[1].arguments
    assert trace.tool_calls[0].id != trace.tool_calls[1].id


def test_untrusted_wrapper_does_not_expand_authority(parallel_records, verified_trial):
    trace = decode(
        canonical_json(
            {"trust": "untrusted-worker-observation", "data": parallel_records}
        ),
        verified_trial,
    )
    assert len(trace.tool_calls) == 2
    with pytest.raises(TrajectoryError):
        decode(
            canonical_json({"trust": "trusted", "data": parallel_records}),
            verified_trial,
        )


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b'{"steps":[]}',
        b'{"schema_version":1}',
        b"\xff",
        b'{"schema_version":2,"schema_version":2}',
        b'{"schema_version":2,"messages":[',
    ],
)
def test_legacy_and_role_trace_shapes_rejected(raw, verified_trial):
    with pytest.raises(TrajectoryError):
        decode(raw, verified_trial)


def test_conflicting_step_order_is_ambiguous(parallel_records, verified_trial):
    parallel_records["tool_calls"] = tuple(
        replace(c, step_id=uid(90)) for c in parallel_records["tool_calls"]
    )
    with pytest.raises(TrajectoryError):
        decode(canonical_json(parallel_records), verified_trial)


def test_structured_step_groups_cannot_reverse_message_anchors(
    parallel_records, verified_trial
):
    # Unanchored calls still cannot contradict the known order of message steps.
    calls = parallel_records["tool_calls"]
    parallel_records["tool_calls"] = (
        replace(calls[0], step_id=uid(80), assistant_message_id=uid(81)),
        calls[1],
    )
    parallel_records["tool_results"] = ()
    messages = parallel_records["messages"]
    parallel_records["messages"] = (
        messages[0],
        replace(messages[1], parts=()),
        replace(messages[2], step_id=uid(80), parts=()),
    )
    with pytest.raises(TrajectoryError, match="ambiguous_order"):
        decode(canonical_json(parallel_records), verified_trial)


def test_oversized_json_integer_has_bounded_error(verified_trial):
    with pytest.raises(TrajectoryError, match="invalid_trace"):
        decode(b'{"schema_version":' + b"9" * 5000 + b"}", verified_trial)


@pytest.mark.parametrize(
    "mutation",
    [
        "float_schema",
        "default_status",
        "result_role",
        "assistant_identity",
        "record_schema",
    ],
)
def test_strict_wire_and_link_semantics(mutation, parallel_trace, verified_trial):
    import json

    data = json.loads(parallel_trace)
    if mutation == "float_schema":
        data["schema_version"] = 2.0
    elif mutation == "default_status":
        del data["tool_calls"][0]["status"]
    elif mutation == "result_role":
        data["messages"][2]["role"] = "user"
    elif mutation == "record_schema":
        data["tool_calls"][0]["schema_version"] = 2
    else:
        data["messages"][1]["parts"] = []
        data["tool_calls"][0]["assistant_message_id"] = str(uid(20))
    with pytest.raises(TrajectoryError):
        decode(canonical_json(data), verified_trial)
