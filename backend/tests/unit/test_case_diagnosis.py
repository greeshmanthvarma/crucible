"""Public response and gateway contracts for the one-case diagnosis runner."""

import json

import pytest

from crucible.improvement.case_diagnosis import (
    CaseDiagnosisError,
    DetailRequest,
    Diagnosis,
    parse_case_response,
)


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
