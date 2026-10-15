"""Public case contracts; fixtures are synthetic deterministic test evidence."""

from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from crucible.repair.cases import ApprovedCase, Assertion, IncidentCase, seal_case


def case_payload():
    return {
        "schema_version": 1,
        "case_id": "write-utf8",
        "incident_ids": {"run": str(uuid4())},
        "kind": "tool",
        "replay_mode": "direct",
        "fixture_manifest_digest": "fixture-digest",
        "settings": {},
        "expected_baseline_failure": {
            "status": "error",
            "error_code": "write_failed",
            "detail_contains": [],
        },
        "candidate_assertions": [{"op": "error_code_equals", "value": None}],
        "expected_behavior": "Write the admitted UTF-8 file.",
        "evidence_ids": [str(uuid4())],
        "tool_name": "write_file",
        "arguments": {"path": "note.txt", "content": "café\n"},
    }


def parse(payload):
    return TypeAdapter(IncidentCase).validate_python(payload)


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "python"},
        {"schema_version": 2},
        {"schema_version": True},
        {"import_path": "os.system"},
        {"expression": "__import__('os')"},
        {"settings": {"bad": object()}},
        {"settings": {"bad": float("nan")}},
        {"replay_mode": "shell"},
    ],
)
def test_case_rejects_unknown_kind_path_and_expression(change):
    with pytest.raises(ValidationError):
        parse(case_payload() | change)


@pytest.mark.parametrize("path", ["../key", "/tmp/key", "a/../../key", "a\\..\\key"])
def test_file_assertions_reject_host_paths_and_traversal(path):
    with pytest.raises(ValidationError):
        Assertion(op="file_exists", path=path)


@pytest.mark.parametrize("path", ["/a/*", "/a/**", "a.b", "/a/../b", "/a/~2"])
def test_result_pointer_is_restricted(path):
    with pytest.raises(ValidationError):
        Assertion(op="result_field_equals", path=path, value=1)


@pytest.mark.parametrize(
    "payload",
    [
        {"op": "regex", "value": ".*"},
        {"op": "python", "value": "True"},
        {"op": "file_exists"},
        {"op": "required_literal_fact_present", "value": "x", "fields": ["secret"]},
        {"op": "required_literal_fact_present", "value": "x", "fields": []},
        {"op": "status_equals", "value": "completed", "expression": "True"},
    ],
)
def test_assertion_rejects_unsupported_or_incomplete_checks(payload):
    with pytest.raises(ValidationError):
        Assertion.model_validate(payload)


def test_null_error_check_and_escaped_literal_pointer_are_supported():
    assert Assertion(op="error_code_equals", value=None).value is None
    assert Assertion(op="result_field_equals", path="/a~1b/~0", value=None).path


def test_sealed_case_is_canonical_detached_and_detects_tampering():
    payload = case_payload()
    case = parse(payload)
    review_id = uuid4()
    approved = seal_case(case, review_id)
    reordered = dict(reversed(list(deepcopy(payload).items())))
    assert approved.case_digest == seal_case(parse(reordered), uuid4()).case_digest
    assert approved.review_id == review_id
    payload["arguments"]["content"] = "mutated caller input"
    assert approved.case.arguments["content"] == "café\n"
    with pytest.raises((ValidationError, TypeError, AttributeError)):
        approved.case.case_id = "changed"
    # Nested mutation must either be prevented or detected when revalidating.
    serialized = approved.model_dump(mode="json")
    serialized["case"]["arguments"]["content"] = "forged"
    with pytest.raises((ValidationError, ValueError)):
        ApprovedCase.model_validate(serialized)


def test_changed_expectations_require_a_new_seal():
    payload = case_payload()
    original = seal_case(parse(payload), uuid4())
    payload["candidate_assertions"] = [{"op": "status_equals", "value": "completed"}]
    assert seal_case(parse(payload), uuid4()).case_digest != original.case_digest


def test_case_rejects_required_known_secret(monkeypatch):
    secret = "synthetic-credential-for-schema-test"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    payload = case_payload()
    payload["candidate_assertions"] = [
        {
            "op": "required_literal_fact_present",
            "value": secret,
            "fields": ["decisions"],
        }
    ]
    with pytest.raises((ValidationError, ValueError)):
        seal_case(parse(payload), uuid4())


@pytest.mark.parametrize("kind", ["context_selection", "compaction"])
def test_tagged_context_cases_use_artifact_references(kind):
    payload = case_payload()
    del payload["tool_name"]
    del payload["arguments"]
    payload.update(
        kind=kind,
        replay_mode="direct" if kind == "context_selection" else "recorded",
        source_artifact_ids=[str(uuid4())],
        correlation_links={},
        prior_compaction_artifact_id=None,
    )
    if kind == "compaction":
        payload.update(
            request_artifact_id=str(uuid4()),
            raw_response_artifact_id=str(uuid4()),
        )
    case = parse(payload)
    assert case.kind == kind
    payload["host_path"] = "/private/incident.json"
    with pytest.raises(ValidationError):
        parse(payload)


def test_seal_prevents_nested_dictionary_and_list_mutation():
    payload = case_payload()
    payload["settings"] = {"items": ["original"]}
    approved = seal_case(parse(payload), uuid4())
    with pytest.raises(TypeError):
        approved.case.arguments["content"] = "changed"
    with pytest.raises(TypeError):
        approved.case.settings["items"].append("changed")
    assert (
        ApprovedCase.model_validate_json(approved.model_dump_json()).case_digest
        == approved.case_digest
    )
