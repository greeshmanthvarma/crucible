"""Public response and gateway contracts for the one-case diagnosis runner."""

import asyncio
import json
from uuid import UUID

import pytest

from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelStop,
    ModelStopReason,
    TextDelta,
)
from crucible.improvement.case_diagnosis import (
    CaseDiagnosisError,
    DetailRequest,
    Diagnosis,
    parse_case_response,
    run_case_diagnosis,
)
from crucible.improvement.case_evidence import expand_case_packet
from tests.unit import test_case_evidence as fixtures

patch_package = fixtures.patch_package
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


@pytest.fixture
def prepared(patch_package):
    return fixtures.prepare(patch_package)


class RecordingGateway:
    def __init__(self, exchanges):
        self.exchanges = iter(exchanges)
        self.requests = []
        self.closed = 0

    async def stream(self, request):
        self.requests.append(request)
        try:
            for event in next(self.exchanges):
                yield event
        finally:
            self.closed += 1


def completed(text):
    return TextDelta(text[:9]), TextDelta(text[9:]), ModelStop(ModelStopReason.COMPLETE)


def disclosure(ref):
    return json.dumps({"request_details": {"refs": [ref]}})


def diagnosis(relationship="linked", refs=("E7",), **fields):
    return json.dumps(
        {
            "diagnosis": {
                "observed_behavior": "An edit failed",
                "relationship": relationship,
                "evidence_refs": refs,
                "uncertainty": "Cause unclear",
                **fields,
            }
        }
    )


def parse(text):
    return parse_case_response(
        text, visible_refs=frozenset({"E7", "E9"}), detail_refs=frozenset({"E7"})
    )


@pytest.mark.parametrize(
    "text",
    [
        "{",
        ' ```json\n{"request_details":{"refs":["E7"]}}\n``` ',
        '{"request_details":{"refs":["E7"],"refs":["E7"]}}',
        '{"diagnosis":{},"request_details":{"refs":["E7"]}}',
        '{"request_details":{"refs":"E7"}}',
        '{"request_details":{"refs":[1]}}',
        diagnosis(observed_behavior=1),
        diagnosis(observed_behavior=float("nan")),
        diagnosis(observed_behavior=" "),
        diagnosis(uncertainty="\t"),
        diagnosis(relationship="likely"),
        diagnosis(extra=True),
        diagnosis(observed_behavior="x" * 16_384),
    ],
)
def test_rejects_invalid_diagnosis_envelopes(text):
    with pytest.raises(CaseDiagnosisError, match="^invalid_response$"):
        parse(text)


def test_unknown_and_citation_membership():
    parsed = parse(diagnosis(relationship="unknown", refs=()))
    assert isinstance(parsed, Diagnosis)
    assert parsed.relationship == "unknown"
    assert parsed.evidence_refs == ()
    assert parse(diagnosis(refs=("E9", "E7"))).evidence_refs == ("E9", "E7")
    for refs in ((), ("E8",), ("E7", "E7")):
        with pytest.raises(CaseDiagnosisError, match="^invalid_citations$"):
            parse(diagnosis(refs=refs))
    with pytest.raises(CaseDiagnosisError, match="^invalid_citations$"):
        parse(diagnosis(relationship="unknown", refs=("E8",)))


def test_details_require_distinct_advertised_ids():
    assert parse('{"request_details":{"refs":["E7"]}}') == DetailRequest(refs=("E7",))
    for refs in ([], ["E7", "E7"], ["E9"], ["../../E7"], ["E7", "E8", "E9", "E10"]):
        with pytest.raises(CaseDiagnosisError, match="^invalid_disclosure$"):
            parse(json.dumps({"request_details": {"refs": refs}}))


@pytest.mark.parametrize("relationship, refs", [("linked", ("E6",)), ("unknown", ())])
async def test_direct_diagnosis_uses_one_request(prepared, relationship, refs):
    gateway = RecordingGateway([completed(diagnosis(relationship, refs))])
    result = await run_case_diagnosis(prepared, gateway=gateway, model="scripted")
    assert result.error_code is None
    assert result.diagnosis.relationship == relationship
    assert result.diagnosis.evidence_refs == refs
    assert result.requests_sent == len(gateway.requests) == 1
    assert result.disclosed_refs == ()


async def test_discloses_one_advertised_batch(prepared):
    ref = prepared.packet.available_detail_refs[0]
    expanded = expand_case_packet(prepared, (ref,))
    gateway = RecordingGateway(
        [completed(disclosure(ref)), completed(diagnosis(refs=(ref,)))]
    )
    result = await run_case_diagnosis(prepared, gateway=gateway, model="scripted")
    assert result.error_code is None
    assert result.diagnosis.evidence_refs == (ref,)
    assert result.requests_sent == 2
    assert result.disclosed_refs == (ref,)
    first, second = gateway.requests
    assert first.messages == prepared.initial.messages
    assert second.messages == expanded.messages
    assert first.tools == second.tools == ()
    assert first.max_output_tokens == second.max_output_tokens == 2_000
    assert first.model == second.model == "scripted"
    assert first.run_id == second.run_id
    assert first.step_id != second.step_id
    assert result.total_reserved_tokens == (
        prepared.initial.input_tokens + expanded.input_tokens + 4_000
    )


async def test_rejects_second_disclosure(prepared):
    ref = prepared.packet.available_detail_refs[0]
    gateway = RecordingGateway([completed(disclosure(ref))] * 2)
    result = await run_case_diagnosis(prepared, gateway=gateway, model="scripted")
    assert result.error_code == "disclosure_exhausted"
    assert result.diagnosis is None
    assert result.requests_sent == len(gateway.requests) == 2


async def test_budget_denial_never_dispatches_request(prepared):
    ref = prepared.packet.available_detail_refs[0]
    expanded = expand_case_packet(prepared, (ref,))
    initial = prepared.initial.input_tokens + 2_000
    cumulative = initial + expanded.input_tokens + 2_000
    for cap, requests, reserved, error in (
        (0, 0, 0, "invalid_limit"),
        (24_001, 0, 0, "invalid_limit"),
        (initial - 1, 0, 0, "budget_exceeded"),
        (initial, 1, initial, "budget_exceeded"),
        (cumulative - 1, 1, initial, "budget_exceeded"),
        (cumulative, 2, cumulative, None),
    ):
        gateway = RecordingGateway(
            [completed(disclosure(ref)), completed(diagnosis(refs=(ref,)))]
        )
        result = await run_case_diagnosis(
            prepared, gateway=gateway, model="scripted", total_token_cap=cap
        )
        assert result.error_code == error
        assert result.requests_sent == len(gateway.requests) == requests
        assert result.total_reserved_tokens == reserved
        assert result.disclosed_refs == ((ref,) if requests == 2 else ())


@pytest.mark.parametrize(
    "events, error",
    [
        ((ModelError("failed", "private provider detail"),), "gateway_error"),
        ((TextDelta("{}"),), "incomplete_response"),
        ((ModelStop(ModelStopReason.LENGTH),), "output_limit"),
        ((ModelStop(ModelStopReason.TOOL_CALLS),), "unexpected_tool_call"),
        ((CompleteToolCall(UUID(int=1), "read_file", {}),), "unexpected_tool_call"),
        ((TextDelta("é" * 4096), TextDelta("é" * 4097)), "output_limit"),
        (completed(disclosure("E999")), "invalid_disclosure"),
        (completed(diagnosis(refs=("E999",))), "invalid_citations"),
        (completed("{"), "invalid_response"),
    ],
)
async def test_rejects_failed_or_incomplete_stream(prepared, events, error):
    gateway = RecordingGateway([events])
    result = await run_case_diagnosis(prepared, gateway=gateway, model="scripted")
    assert result.error_code == error
    assert result.diagnosis is None
    assert "private provider detail" not in repr(result)
    assert result.requests_sent == gateway.closed == 1


async def test_propagates_cancellation_and_closes_stream(prepared):
    waiting = asyncio.Event()
    closed = asyncio.Event()

    class BlockingGateway:
        async def stream(self, request):
            try:
                yield TextDelta("{")
                waiting.set()
                await asyncio.Future()
            finally:
                closed.set()

    task = asyncio.create_task(
        run_case_diagnosis(prepared, gateway=BlockingGateway(), model="scripted")
    )
    async with asyncio.timeout(2):
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert closed.is_set()
