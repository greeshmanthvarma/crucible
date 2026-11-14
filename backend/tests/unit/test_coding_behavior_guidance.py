from crucible.context.manager import validated_advice
from crucible.engine.coding_behavior import (
    BehaviorObservation,
    ToolObservation,
    coding_guidance,
    step_guidance,
)


def observe(step, remaining, tools=()):
    return BehaviorObservation(
        step, remaining, 100, 150_000, 300.0, tuple(tools), False
    )


def test_coding_guidance_is_valid_advice_and_mentions_limits():
    text = coding_guidance()
    assert validated_advice(text) == text
    assert "edit_file" in text and "note" in text.lower()


def test_step_guidance_reports_request_and_tokens():
    text = step_guidance(observe(3, 17))
    assert "request 3 of 20" in text and "150000" in text
    assert "Stop investigating" not in text


def test_step_guidance_pushes_edit_at_halfway_without_edit():
    text = step_guidance(
        observe(10, 10, [ToolObservation("read_file", "succeeded", None)])
    )
    assert "Stop investigating" in text


def test_step_guidance_quiet_after_recent_edit():
    text = step_guidance(
        observe(12, 8, [ToolObservation("edit_file", "succeeded", None)])
    )
    assert "Stop investigating" not in text


def test_step_guidance_final_requests_warning():
    assert "Only 2 requests remain" in step_guidance(observe(18, 2))
