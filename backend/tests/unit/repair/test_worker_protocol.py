"""Versioned wire contracts; all fixtures are deterministic test evidence."""

from uuid import uuid4

import pytest
from crucible.repair.worker_protocol import (
    WorkerJob,
    WorkerObservation,
    decode_event,
    decode_request,
    encode_event,
    encode_request,
    read_observation,
)
from pydantic import ValidationError

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelToolDefinition,
    ModelUsage,
    PreparedModelRequest,
    ReasoningDelta,
    TextDelta,
)


def job_payload():
    return {
        "schema_version": 1,
        "job_id": str(uuid4()),
        "case_id": "synthetic-tool",
        "repeat_index": 0,
        "purpose": "candidate",
        "source_digest": "a" * 64,
        "input_manifest": {
            "kind": "tool",
            "tool_name": "read_file",
            "arguments": {"path": "note.txt"},
        },
        "settings": {},
        "deadline_seconds": 600,
        "observation_bytes": 1_048_576,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"repeat_index": -1},
        {"purpose": "shell"},
        {"deadline_seconds": 0},
        {"deadline_seconds": float("inf")},
        {"observation_bytes": 1_048_577},
        {"observation_bytes": 0},
        {"host_docker_socket": "/var/run/docker.sock"},
        {"settings": {"executable": object()}},
    ],
)
def test_job_rejects_unknown_version_fields_and_unbounded_resources(change):
    with pytest.raises(ValidationError):
        WorkerJob.model_validate(job_payload() | change)


def observed(job_id):
    return WorkerObservation(
        job_id=job_id,
        status="completed",
        tool_result={"output_text": "café"},
        context_messages=None,
        summary=None,
        error_code=None,
        detail=None,
        artifact_refs=(),
        command_records=[],
    )


def test_observation_validates_identity_size_and_self_reported_verdict():
    job = WorkerJob.model_validate(job_payload())
    result = observed(job.job_id)
    raw = result.model_dump_json().encode()
    assert read_observation(raw, job) == result
    for invalid in (
        b"not-json",
        raw + b"garbage",
        observed(uuid4()).model_dump_json().encode(),
    ):
        with pytest.raises(ValueError):
            read_observation(invalid, job)
    with pytest.raises(ValueError):
        read_observation(raw, job.model_copy(update={"observation_bytes": 5}))
    with pytest.raises(ValidationError):
        WorkerObservation.model_validate(result.model_dump() | {"verdict": "pass"})


@pytest.mark.parametrize(
    "reference", ["../private", "/etc/passwd", "a/../../key", "a\\..\\key"]
)
def test_observation_references_cannot_escape_private_state(reference):
    payload = observed(uuid4()).model_dump() | {"artifact_refs": [reference]}
    with pytest.raises(ValidationError):
        WorkerObservation.model_validate(payload)


def prepared_request():
    call_id = new_id()
    return PreparedModelRequest(
        new_id(),
        new_id(),
        "test-model",
        (
            ModelMessage(
                ModelRole.ASSISTANT,
                (
                    ModelPart(
                        "tool_call",
                        tool_call_id=call_id,
                        tool_name="read_file",
                        arguments={"path": "note.txt"},
                        provider_correlation_id="provider-call",
                    ),
                ),
            ),
        ),
        (ModelToolDefinition("read_file", "Read UTF-8", {"type": "object"}),),
        32,
    )


def test_model_request_round_trip_preserves_internal_contract():
    request = prepared_request()
    assert decode_request(encode_request(request)) == request
    payload = encode_request(request)
    with pytest.raises(ValueError):
        decode_request(payload | {"provider_api_key": "synthetic-secret"})
    with pytest.raises(ValueError):
        decode_request(payload | {"schema_version": 2})


@pytest.mark.parametrize(
    "event",
    [
        TextDelta("café\n"),
        ReasoningDelta("internal reasoning"),
        CompleteToolCall(new_id(), "read_file", {"path": "note.txt"}, "provider-call"),
        ModelUsage(10, 3),
        ModelUsage(None, None),
        ModelStop(ModelStopReason.COMPLETE),
        ModelError("upstream_error", "unavailable"),
    ],
)
def test_ndjson_event_round_trip(event):
    assert decode_event(encode_event(event)) == event


def test_event_decoder_rejects_unknown_kinds_and_unversioned_payloads():
    for payload in (
        {"kind": "execute_python", "code": "pass"},
        {"schema_version": 2, "kind": "text_delta", "text": "x"},
    ):
        with pytest.raises(ValueError):
            decode_event(payload)
