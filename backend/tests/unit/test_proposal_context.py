import json

import pytest

from crucible.engine.run_engine import ModelResponseInvalid
from crucible.improvement.proposal_context import ProposalStepPolicy

DECISION = json.dumps(
    dict(
        decision="decline",
        selected_weakness_id="budget_exhaustion",
        reason="Insufficient evidence.",
        paths=[],
    )
)


@pytest.mark.parametrize(
    "text",
    [
        DECISION,
        "Notes: no change justified.\n" + DECISION,
        "```json\n" + DECISION + "\n```",
        "Notes: no change.\n```json\n" + DECISION + "\n```",
    ],
)
def test_wrapped_decision(text):
    result, followup = ProposalStepPolicy(150000, 100000).finish_response(text, ())
    assert not followup
    assert json.loads(result)["claimed_paths"] == []
    assert json.loads(result)["summary"] == "Insufficient evidence."


@pytest.mark.parametrize(
    "text",
    [
        "Notes only",
        DECISION + DECISION,
        "{broken}",
        DECISION[:-1] + ',"unexpected":true}',
        DECISION.replace('"decline"', '"edit"').replace("[]", '["/tmp/escape"]'),
        '{"reason":"missing fields"}',
        "x" * (1048576 + 1),
    ],
)
def test_invalid_decision(text):
    policy = ProposalStepPolicy(150000, 100000)
    policy._has_followup_step = False
    with pytest.raises(ModelResponseInvalid):
        policy.finish_response(text, ())


def test_wrapped_edit_keeps_followup_and_allowed_paths():
    from crucible.improvement.contracts import EDITABLE_PATHS

    decision = json.dumps(
        dict(
            decision="edit",
            selected_weakness_id="budget_exhaustion",
            reason="Concrete change.",
            paths=[EDITABLE_PATHS[0]],
        )
    )
    result, followup = ProposalStepPolicy(150000, 100000).finish_response(
        "Notes:\n" + decision, ()
    )
    assert followup
    assert json.loads(result)["paths"] == [EDITABLE_PATHS[0]]


def test_tool_response_does_not_parse_partial_text():
    from uuid import uuid4

    from crucible.engine.gateway import CompleteToolCall

    calls = (CompleteToolCall(uuid4(), "read_file", {"path": "README.md"}),)
    assert ProposalStepPolicy(150000, 100000).finish_response(
        "Investigating.", calls
    ) == ("Investigating.", False)


def test_final_report_can_be_corrected_without_changing_scope():
    from crucible.improvement.proposal_context import ProposalDecision

    policy = ProposalStepPolicy(150000, 100000)
    policy._phase = "report"
    policy._decision = ProposalDecision.model_validate_json(DECISION)
    assert policy.finish_response("unfinished report", ()) == (
        "unfinished report",
        True,
    )
    assert "missing decision object" in policy.response_correction
    report = json.dumps(
        dict(
            schema_version=1,
            selected_weakness_id="budget_exhaustion",
            summary="Declined.",
            claimed_paths=[],
            risks=[],
        )
    )
    assert not policy.finish_response(report, ())[1]
    assert policy.response_correction is None


def test_schema_errors_are_available_for_correction_without_echoing_input():
    policy = ProposalStepPolicy(150000, 100000)
    text = json.dumps(
        dict(
            decision="edit",
            selected_weakness_id="budget_exhaustion",
            reason="Concrete change.",
            paths=["/tmp/outside-scope"],
        )
    )
    assert policy.finish_response(text, ()) == (text, True)
    assert "allowed paths" in policy.response_correction
    assert "/tmp/outside-scope" not in policy.response_correction
    assert policy._decision is None
