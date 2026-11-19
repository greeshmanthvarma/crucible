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
    with pytest.raises(ModelResponseInvalid):
        ProposalStepPolicy(150000, 100000).finish_response(text, ())


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
