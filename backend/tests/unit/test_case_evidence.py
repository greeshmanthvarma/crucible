"""Case packets assembled from sealed development exports."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID

import pytest

from crucible.domain.tools import ToolExecutionMode, ToolResultStatus
from crucible.improvement import case_evidence
from crucible.improvement.case_evidence import CasePacketError, assemble_case_packet
from crucible.improvement.source import _digest
from crucible.improvement.trajectory_contracts import (
    ExportLimits,
    TrialSelection,
    VerifiedDevelopmentInputs,
)
from crucible.improvement.trajectory_export import export_trajectory_package
from crucible.models.token_counting import estimate_input_tokens
from crucible.repair.identity import canonical_json
from tests.unit import test_trajectory as fixtures

parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


def seal(root, trials, limits=None):
    source = trials[0].source.model_copy(update={"export_digest": _digest({})})
    ids = tuple(t.trial_id for t in trials)
    return export_trajectory_package(
        VerifiedDevelopmentInputs(
            tuple(replace(t, source=source) for t in trials),
            source,
            {},
            ids,
            UUID(int=800),
            UUID(int=801),
            TrialSelection(mode="failed-family-v1", trial_ids=ids),
        ),
        root,
        limits or ExportLimits(),
    )


def with_evidence(trial, trace, diff):
    raw = canonical_json(trace)
    return replace(
        trial,
        trace_bytes=raw,
        solution_bytes=diff,
        trace_ref=trial.trace_ref.model_copy(
            update={"sha256": hashlib.sha256(raw).hexdigest()}
        ),
        solution_ref=trial.solution_ref.model_copy(
            update={"sha256": hashlib.sha256(diff).hexdigest()}
        ),
    )


def passing(trial, identifier, case, repeat):
    trial_id = UUID(int=identifier)
    return replace(
        trial,
        trial_id=trial_id,
        case_id=case,
        repeat_index=repeat,
        verdict="pass",
        termination="budget_exhausted",
        trace_ref=trial.trace_ref.model_copy(update={"trial_id": trial_id}),
        solution_ref=trial.solution_ref.model_copy(update={"trial_id": trial_id}),
    )


@pytest.fixture
def patch_package(tmp_path, verified_trial, parallel_records):
    trace = parallel_records
    trace["tool_calls"] = tuple(
        replace(call, name="apply_patch") for call in trace["tool_calls"]
    )
    first, second = trace["tool_results"]
    trace["tool_results"] = (
        replace(first, status=ToolResultStatus.FAILED, error_code="patch_rejected"),
        second,
    )
    diff = b"--- a/example.py\n+++ b/example.py\n@@ -1 +1 @@\n-old\n+new\n"
    target = with_evidence(verified_trial, trace, diff)
    root = tmp_path / "package"
    package = seal(
        root,
        (
            target,
            passing(target, 41, target.case_id, 2),
            passing(target, 42, "foreign", 1),
        ),
    )
    return root, package, target.trial_id


def prepare(export, **kwargs):
    root, package, trial_id = export
    return assemble_case_packet(
        root, trial_id, expected_bundle_digest=package.bundle_digest, **kwargs
    )


def test_packet_rejects_wrong_digest_or_nonfailed_target(patch_package):
    root, package, trial_id = patch_package
    for target, digest, code in (
        (trial_id, "0" * 64, "package_mismatch"),
        (UUID(int=99), package.bundle_digest, "invalid_target"),
        (UUID(int=41), package.bundle_digest, "invalid_target"),
    ):
        with pytest.raises(CasePacketError) as rejected:
            assemble_case_packet(root, target, expected_bundle_digest=digest)
        assert rejected.value.code == code


def test_packet_retains_failed_edit_and_later_success(patch_package):
    prepared = prepare(patch_package)
    packet = prepared.packet
    assert packet.submission.changed_paths == ("example.py",)
    assert packet.comparison.trial_id == UUID(int=41)
    assert packet.comparison.verdict == "pass"
    assert packet.comparison.termination == "budget_exhausted"
    assert b"foreign" not in canonical_json(packet)
    assert packet.gaps.truncated_reference_ids == ("E3", "E4")
    assert [
        (row.id, row.call_id, row.status)
        for row in packet.events
        if row.kind == "tool_result"
    ] == [
        ("E6", "E4", "failed"),
        ("E7", "E3", "succeeded"),
    ]
    assert [(row.status, row.error_code, row.count) for row in packet.outcomes] == [
        ("failed", "patch_rejected", 1),
        ("succeeded", None, 1),
    ]


@pytest.fixture
def long_package(tmp_path, verified_trial, parallel_records):
    trace = parallel_records
    trace["messages"] = trace["messages"][:1]
    call = trace["tool_calls"][0]
    result = trace["tool_results"][0]
    trace["tool_calls"] = tuple(
        replace(call, id=UUID(int=100 + i), call_sequence=i + 1, name="apply_patch")
        for i in range(48)
    )
    trace["tool_results"] = tuple(
        replace(
            result,
            id=UUID(int=200 + i),
            tool_call_id=call.id,
            completion_sequence=i + 1,
            status=ToolResultStatus.FAILED if i < 24 else ToolResultStatus.SUCCEEDED,
            error_code="patch_rejected" if i < 24 else None,
            display_text="Recorded edit result " * 100,
        )
        for i, call in enumerate(trace["tool_calls"])
    )
    target = with_evidence(verified_trial, trace, b"")
    root = tmp_path / "package"
    return root, seal(root, (target,)), target.trial_id


def test_packet_caps_full_request_and_reports_omissions(long_package):
    prepared = prepare(long_package, initial_input_tokens=2200)
    assert prepared.initial.input_tokens == estimate_input_tokens(
        prepared.initial.messages, ()
    )
    assert prepared.initial.input_tokens <= 2200
    assert prepared.initial.output_reserve_tokens == 2000
    assert [row.count for row in prepared.packet.outcomes] == [24, 24]
    assert {"E50", "E73", "E74", "E97"} <= {row.id for row in prepared.packet.events}
    assert prepared.packet.gaps.omitted_event_ranges == (
        (14, 24),
        (27, 48),
        (51, 72),
        (75, 96),
    )
    with pytest.raises(CasePacketError, match="budget_exceeded"):
        prepare(long_package, initial_input_tokens=1)
    with pytest.raises(CasePacketError, match="invalid_limit"):
        prepare(long_package, initial_input_tokens=8001)


def test_packet_is_repeatable_and_rejects_changed_detail(patch_package, monkeypatch):
    first = prepare(patch_package)
    second = prepare(patch_package)
    assert canonical_json(first.packet) == canonical_json(second.packet)
    assert canonical_json(first.initial) == canonical_json(second.initial)
    verify = case_evidence.verify_trajectory_package

    def verify_then_change(root):
        verified = verify(root)
        detail = next(
            root.glob(
                "trials/00000000-0000-0000-0000-000000000028/details/*.result.json"
            )
        )
        detail.write_bytes(b"{}")
        return verified

    monkeypatch.setattr(case_evidence, "verify_trajectory_package", verify_then_change)
    with pytest.raises(CasePacketError, match="artifact_drift"):
        prepare(patch_package)


def test_packet_keeps_missing_and_empty_distinct(
    tmp_path, verified_trial, parallel_records
):
    trace = parallel_records
    trace["messages"] = ()
    trace["tool_results"] = ()
    for name, diff in (("empty", b""), ("truncated", b"diff\n" * 2000)):
        target = with_evidence(verified_trial, trace, diff)
        root = tmp_path / name
        package = seal(root, (target,), ExportLimits(detail_bytes=4096))
        packet = prepare((root, package, target.trial_id)).packet
        assert packet.task_instruction is None
        assert packet.submission.state == ("empty" if name == "empty" else "nonempty")
        assert packet.submission.changed_paths == ()
        assert packet.submission.paths_complete == (name == "empty")
        assert packet.gaps.missing_result_call_ids == ("E1", "E2")


def test_expansion_rejects_foreign_unknown_and_duplicate_refs(long_package):
    prepared = prepare(long_package, initial_input_tokens=2200)
    for refs in (
        (),
        ("E50", "E50"),
        ("E50", "E73", "E74", "E97"),
        ("E14",),
        ("E999",),
        ("E1",),
        ("../details/raw.json",),
    ):
        with pytest.raises(CasePacketError, match="invalid_refs"):
            case_evidence.expand_case_packet(prepared, refs)


def test_expansion_counts_resupplied_context_and_caps_details(long_package):
    prepared = prepare(long_package, initial_input_tokens=2200)
    refs = ("E97", "E50")
    expanded = case_evidence.expand_case_packet(prepared, refs)
    assert expanded.messages[:2] == prepared.initial.messages
    assert expanded.messages[2].role == "assistant"
    assert expanded.input_tokens == estimate_input_tokens(expanded.messages, ())
    assert expanded.input_tokens > prepared.initial.input_tokens
    assert expanded.output_reserve_tokens == 2000
    details = json.loads(expanded.messages[3].parts[0].text_content)["details"]
    assert [detail["id"] for detail in details] == list(refs)
    assert all("Recorded edit result" in detail["excerpt"] for detail in details)
    assert all(
        len(detail["excerpt"]) == 1000 and detail["truncated"] for detail in details
    )
    tight = case_evidence.expand_case_packet(
        prepared, refs, followup_input_tokens=expanded.input_tokens - 1
    )
    assert tight.input_tokens < expanded.input_tokens
    assert tight.input_tokens == estimate_input_tokens(tight.messages, ())
    clipped = json.loads(tight.messages[3].parts[0].text_content)["details"]
    assert all(detail["truncated"] for detail in clipped)
    with pytest.raises(CasePacketError, match="budget_exceeded"):
        case_evidence.expand_case_packet(
            prepared, refs, followup_input_tokens=prepared.initial.input_tokens
        )
    with pytest.raises(CasePacketError, match="invalid_limit"):
        case_evidence.expand_case_packet(prepared, refs, followup_input_tokens=12001)


@pytest.fixture
def nonpatch_package(tmp_path, verified_trial, parallel_records):
    trace = parallel_records
    first, second = trace["tool_calls"]
    trace["tool_calls"] = (
        replace(first, name="list_files", execution_mode=ToolExecutionMode.SEQUENTIAL),
        replace(
            second,
            name="execute_command",
            arguments={"executable": "check", "reason": "Check the synthetic task"},
            execution_mode=ToolExecutionMode.SEQUENTIAL,
        ),
    )
    first, second = trace["tool_results"]
    trace["tool_results"] = (
        replace(
            first,
            status=ToolResultStatus.FAILED,
            error_code="command_exit_nonzero",
            result={"exit_code": 1, "termination": "completed"},
            display_text="RAW_COMMAND_OUTPUT_SENTINEL",
        ),
        second,
    )
    target = with_evidence(verified_trial, trace, b"")
    root = tmp_path / "nonpatch"
    return root, seal(root, (target,)), target.trial_id


def test_same_assembler_handles_nonpatch_development_failure(nonpatch_package):
    prepared = prepare(nonpatch_package)
    packet = prepared.packet
    assert packet.verdict == "fail"
    assert packet.submission.state == "empty"
    assert [(item.tool, item.status, item.error_code) for item in packet.outcomes] == [
        ("execute_command", "failed", "command_exit_nonzero"),
        ("list_files", "succeeded", None),
    ]
    assert "E4" not in packet.available_detail_refs
    assert "E6" in packet.available_detail_refs
    expanded = case_evidence.expand_case_packet(prepared, ("E6",))
    wire = canonical_json((packet, expanded))
    assert b"exit_code" in wire
    assert b"RAW_COMMAND_OUTPUT_SENTINEL" not in wire
    assert b"apply_patch" not in wire
    assert (
        "A later success alone does not establish recovery"
        in prepared.initial.messages[0].parts[0].text_content
    )
