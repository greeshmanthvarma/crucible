"""Protected verdicts inspect observations and files, never worker pass claims."""

from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from crucible.repair.cases import IncidentCase
from crucible.repair.verification import verify_observation

SUMMARY = {
    name: ""
    for name in (
        "objective_and_constraints",
        "decisions",
        "repository_facts",
        "changes",
        "commands_and_validation",
        "unresolved_problems",
        "execution_state",
        "important_paths_and_symbols",
    )
}


def make_case(assertions, *, settings=None):
    return TypeAdapter(IncidentCase).validate_python(
        {
            "schema_version": 1,
            "case_id": "synthetic-check",
            "incident_ids": {},
            "kind": "tool",
            "replay_mode": "direct",
            "fixture_manifest_digest": "fixture",
            "settings": settings or {},
            "expected_baseline_failure": {
                "status": "error",
                "error_code": "write_failed",
                "detail_contains": ["cannot write", "{workspace}/note.txt"],
            },
            "candidate_assertions": assertions,
            "expected_behavior": "Deterministic assertion fixture.",
            "evidence_ids": [],
            "tool_name": "write_file",
            "arguments": {"path": "note.txt"},
        }
    )


def observation(**changes):
    return {
        "schema_version": 1,
        "job_id": str(uuid4()),
        "status": "completed",
        "tool_result": {
            "status": "success",
            "output_text": "café\n",
            "data": {"ok": True},
        },
        "context_messages": None,
        "summary": None,
        "error_code": None,
        "detail": None,
        "artifact_refs": [],
        "command_records": [],
    } | changes


@pytest.fixture
def workspaces(tmp_path):
    before, after = tmp_path / "before", tmp_path / "after"
    before.mkdir()
    after.mkdir()
    for root in (before, after):
        (root / "note.txt").write_bytes("café\r\n".encode())
        (root / "unrelated.txt").write_text("original")
    return before, after


def check(workspaces, assertion, observed=None, *, settings=None, phase="candidate"):
    results = verify_observation(
        make_case([assertion], settings=settings),
        observation() if observed is None else observed,
        *workspaces,
        phase=phase,
    )
    assert results
    return all(result.passed for result in results)


@pytest.mark.parametrize(
    ("assertion", "passed"),
    [
        ({"op": "error_code_equals", "value": None}, True),
        ({"op": "error_code_equals", "value": "write_failed"}, False),
        ({"op": "status_equals", "value": "completed"}, True),
        ({"op": "status_equals", "value": "error"}, False),
        ({"op": "output_text_equals", "value": "café\n"}, True),
        ({"op": "output_text_equals", "value": "cafe\n"}, False),
        ({"op": "output_text_contains", "value": "fé"}, True),
        ({"op": "output_text_contains", "value": "absent"}, False),
        ({"op": "result_field_equals", "path": "/data/ok", "value": True}, True),
        ({"op": "result_field_equals", "path": "/missing", "value": None}, False),
        ({"op": "file_exists", "path": "note.txt"}, True),
        ({"op": "file_exists", "path": "missing.txt"}, False),
        ({"op": "file_absent", "path": "missing.txt"}, True),
        ({"op": "file_absent", "path": "note.txt"}, False),
        ({"op": "file_text_equals", "path": "note.txt", "value": "café\r\n"}, True),
        ({"op": "file_text_equals", "path": "note.txt", "value": "café\n"}, False),
        ({"op": "file_text_contains", "path": "note.txt", "value": "fé"}, True),
        ({"op": "file_text_contains", "path": "missing.txt", "value": ""}, False),
    ],
)
def test_scalar_and_file_operators(workspaces, assertion, passed):
    assert check(workspaces, assertion) is passed


@pytest.mark.parametrize("change", ["modify", "delete", "create"])
def test_unrelated_files_unchanged_checks_entire_fixture_and_new_files(
    workspaces, change
):
    assertion = {"op": "unrelated_files_unchanged"}
    settings = {"admitted_mutation_paths": ["note.txt"]}
    assert check(workspaces, assertion, settings=settings)
    after = workspaces[1]
    (after / "note.txt").write_text("allowed change")
    assert check(workspaces, assertion, settings=settings)
    if change == "modify":
        (after / "unrelated.txt").write_text("forged")
    elif change == "delete":
        (after / "unrelated.txt").unlink()
    else:
        (after / "new.txt").write_text("unexpected")
    assert not check(workspaces, assertion, settings=settings)


@pytest.mark.parametrize("content", [b"\xff", b"cafe\n"])
def test_file_text_rejects_invalid_utf8_and_different_unicode(workspaces, content):
    (workspaces[1] / "note.txt").write_bytes(content)
    assert not check(
        workspaces, {"op": "file_text_equals", "path": "note.txt", "value": "café\n"}
    )


def test_file_assertions_do_not_follow_symlinks(workspaces, tmp_path):
    outside = tmp_path / "private.txt"
    outside.write_text("secret")
    (workspaces[1] / "link").symlink_to(outside)
    assert not check(
        workspaces, {"op": "file_text_equals", "path": "link", "value": "secret"}
    )


@pytest.mark.parametrize(
    "summary", [None, {}, {**SUMMARY, "decisions": []}, {**SUMMARY, "extra": "x"}]
)
def test_summary_schema_rejects_missing_invalid_and_extra_fields(workspaces, summary):
    assert not check(
        workspaces, {"op": "summary_schema_valid"}, observation(summary=summary)
    )


def test_summary_schema_and_literal_facts_use_only_allowed_fields(workspaces):
    summary = SUMMARY | {"decisions": "Keep repair.py private."}
    observed = observation(summary=summary)
    assert check(workspaces, {"op": "summary_schema_valid"}, observed)
    assertion = {
        "op": "required_literal_fact_present",
        "value": "repair.py",
        "fields": ["decisions"],
    }
    assert check(workspaces, assertion, observed)
    summary["decisions"] = "Keep Repair.py private."
    summary["repository_facts"] = "repair.py"
    assert not check(workspaces, assertion, observed)


def test_baseline_matches_failure_and_only_controller_known_prefixes(workspaces):
    before, after = workspaces
    failed = observation(
        status="error",
        error_code="write_failed",
        detail=f"cannot write {after}/note.txt",
    )
    assert check(
        workspaces, {"op": "error_code_equals", "value": None}, failed, phase="baseline"
    )
    for changes in (
        {"status": "completed", "error_code": None},
        {"error_code": "different"},
        {"detail": "cannot write /arbitrary/note.txt"},
        {"detail": f"unrelated {before}/note.txt"},
    ):
        assert not check(
            workspaces,
            {"op": "error_code_equals", "value": None},
            failed | changes,
            phase="baseline",
        )


@pytest.mark.parametrize("status", ["error", "unsupported"])
def test_worker_errors_cannot_pass_candidate_even_if_assertion_matches(
    workspaces, status
):
    assert not check(
        workspaces, {"op": "status_equals", "value": status}, observation(status=status)
    )


@pytest.mark.parametrize("bad", ["not JSON", [], {"status": "completed"}])
def test_malformed_observation_cannot_pass(workspaces, bad):
    try:
        passed = check(workspaces, {"op": "error_code_equals", "value": None}, bad)
    except (ValueError, TypeError):
        return
    assert not passed


def messages():
    return [
        {
            "conversation_sequence": 1,
            "role": "assistant",
            "parts": [
                {
                    "kind": "tool_call",
                    "tool_call_id": "call-1",
                    "tool_name": "read_file",
                    "arguments": {},
                }
            ],
        },
        {
            "conversation_sequence": 2,
            "role": "tool",
            "parts": [
                {
                    "kind": "tool_result",
                    "tool_call_id": "call-1",
                    "text_content": "result",
                }
            ],
        },
    ]


def test_complete_pairs_and_retained_order(workspaces):
    source = messages()
    settings = {"source_messages": source}
    for op in ("tool_pairs_complete", "retained_messages_ordered"):
        assert check(
            workspaces,
            {"op": op},
            observation(context_messages=source),
            settings=settings,
        )
    assert not check(
        workspaces,
        {"op": "tool_pairs_complete"},
        observation(context_messages=source[:1]),
        settings=settings,
    )
    assert not check(
        workspaces,
        {"op": "tool_pairs_complete"},
        observation(context_messages=source[1:]),
        settings=settings,
    )
    assert not check(
        workspaces,
        {"op": "retained_messages_ordered"},
        observation(context_messages=source[::-1]),
        settings=settings,
    )
    forged = deepcopy(source)
    forged[1]["parts"][0]["text_content"] = "invented"
    assert not check(
        workspaces,
        {"op": "retained_messages_ordered"},
        observation(context_messages=forged),
        settings=settings,
    )


def test_context_limit_uses_protected_estimate_not_worker_claim(workspaces):
    selected = [
        {
            "conversation_sequence": 1,
            "role": "user",
            "parts": [{"kind": "text", "text_content": "x" * 80}],
        }
    ]
    observed = observation(
        context_messages=selected, tool_result={"estimated_tokens": 0}
    )
    assertion = {"op": "context_within_estimated_limit"}
    settings = {"input_limit": 20, "output_reserve": 0, "threshold": 1.0}
    assert check(workspaces, assertion, observed, settings=settings)
    assert not check(
        workspaces, assertion, observed, settings=settings | {"output_reserve": 1}
    )


@pytest.mark.parametrize("value,passed", [({"ok": True}, True), ({"ok": 1}, False)])
def test_sealed_nested_json_comparison_preserves_boolean_types(
    workspaces, value, passed
):
    from crucible.repair.cases import seal_case

    case = seal_case(
        make_case(
            [
                {
                    "op": "result_field_equals",
                    "path": "/data",
                    "value": value,
                }
            ]
        ),
        uuid4(),
    )
    results = verify_observation(
        case.case, observation(), *workspaces, phase="candidate"
    )
    assert all(result.passed for result in results) is passed


@pytest.mark.parametrize("reversed_order", [False, True])
def test_retained_conversation_order_separate_from_full_request(
    workspaces, reversed_order
):
    source = [
        {
            "conversation_sequence": i,
            "role": "user",
            "parts": [{"kind": "text", "text_content": str(i)}],
        }
        for i in range(2)
    ]
    case = make_case(
        [{"op": "retained_messages_ordered"}], settings={"source_messages": source}
    )
    retained = list(reversed(source)) if reversed_order else source
    full_request = [
        {
            "role": "system",
            "parts": [{"kind": "text", "text_content": "Protected instructions"}],
        }
    ] + source
    results = verify_observation(
        case,
        observation(
            context_messages=full_request, tool_result={"retained_messages": retained}
        ),
        *workspaces,
        "candidate",
    )
    assert all(result.passed for result in results) is (not reversed_order)
