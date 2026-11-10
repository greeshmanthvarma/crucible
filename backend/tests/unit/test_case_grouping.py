"""Grouping uses the cited evidence of an already validated diagnosis."""

from dataclasses import replace
from uuid import UUID

import pytest

from crucible.improvement.case_diagnosis import Diagnosis
from crucible.improvement.case_evidence import CaseEvent
from crucible.improvement.case_grouping import group_tool_failures
from tests.unit import test_case_evidence as fixtures

patch_package = fixtures.patch_package
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


@pytest.fixture
def packet(patch_package):
    return fixtures.prepare(patch_package).packet


def observed(*refs, relationship="linked"):
    return Diagnosis(
        observed_behavior="A required tool call failed.",
        relationship=relationship,
        evidence_refs=refs,
        uncertainty="The underlying cause is unresolved.",
    )


def result(tool, *, status="failed", code="tool_exception", ref="E1"):
    return CaseEvent(ref, "tool_result", tool, status, code, None, None, None)


@pytest.mark.parametrize(
    "tool",
    [
        "list_files",
        "search_files",
        "read_file",
        "write_file",
        "apply_patch",
        "workspace_status",
        "workspace_diff",
        "execute_command",
    ],
)
def test_maps_cited_failure_for_each_tool(packet, tool):
    packet = replace(packet, events=(result(tool), result("apply_patch", ref="E2")))
    report = group_tool_failures(((packet, observed("E1")),))
    (group,) = report.groups
    assert group.label == f"{tool}_failure"
    (attempt,) = group.attempts
    assert attempt.trial_id == packet.failed_trial_id
    assert [event.id for event in attempt.cited_results] == ["E1"]
    assert not report.unresolved


def test_groups_repeats_without_losing_recovery_or_counting_calls(packet):
    later = replace(packet, failed_trial_id=UUID(int=900))
    first = (packet, observed("E4", "E6"))
    second = (later, observed("E6", relationship="recovered"))
    report = group_tool_failures((second, first))
    assert report == group_tool_failures((first, second))
    (group,) = report.groups
    assert group.label == "apply_patch_failure"
    assert [attempt.trial_id for attempt in group.attempts] == [
        packet.failed_trial_id,
        later.failed_trial_id,
    ]
    assert [attempt.diagnosis.relationship for attempt in group.attempts] == [
        "linked",
        "recovered",
    ]
    assert [event.id for event in group.attempts[0].cited_results] == ["E6"]


def test_multiple_cited_failure_tools_are_ambiguous(packet):
    packet = replace(
        packet,
        events=(result("apply_patch"), result("read_file", ref="E2")),
    )
    report = group_tool_failures(((packet, observed("E2", "E1")),))
    assert not report.groups
    (unresolved,) = report.unresolved
    assert unresolved.reason == "multiple_tools"
    assert unresolved.candidate_labels == ("apply_patch_failure", "read_file_failure")


@pytest.mark.parametrize(
    "event",
    [
        result("execute_command", code="command_exit_nonzero"),
        result("execute_command", status="denied", code="approval_denied"),
        result("execute_command", status="cancelled", code="cancelled"),
        result("read_file", status="interrupted", code="process_restarted"),
        result("read_file", status="rejected", code="budget_exhausted"),
        result("search_files", status="succeeded", code=None),
        result("unregistered_tool"),
        CaseEvent(
            "E1", "tool_call", "apply_patch", "completed", None, None, "E2", None
        ),
    ],
)
def test_does_not_label_control_outcomes_or_uncited_failures(packet, event):
    packet = replace(packet, events=(event, result("apply_patch", ref="E2")))
    report = group_tool_failures(((packet, observed("E1", relationship="unknown")),))
    assert not report.groups
    (unresolved,) = report.unresolved
    assert unresolved.reason == "no_eligible_failure"
    assert unresolved.candidate_labels == ()
    assert unresolved.attempt.diagnosis.evidence_refs == ("E1",)


def test_command_timeout_is_a_tool_failure(packet):
    packet = replace(
        packet,
        events=(
            result("execute_command", status="timed_out", code="command_timed_out"),
        ),
    )
    (group,) = group_tool_failures(((packet, observed("E1")),)).groups
    assert group.label == "execute_command_failure"


def test_rejects_invalid_provenance_or_duplicate_trial(packet):
    with pytest.raises(ValueError, match="invisible evidence"):
        group_tool_failures(((packet, observed("E999")),))
    item = (packet, observed("E6"))
    with pytest.raises(ValueError, match="Duplicate diagnosed trial"):
        group_tool_failures((item, item))
