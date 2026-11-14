from crucible.context.invariants import SimpleTokenEstimator
from crucible.context.result_projection import (
    PREVIEW_CHARS,
    SHORTEN_ABOVE_CHARS,
    SHORTENED_NOTICE,
    project_coding_tool_results,
    request_allowance,
)
from crucible.domain.ids import new_id
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole


def batch(name: str, text: str) -> tuple[ModelMessage, ...]:
    call_id = new_id()
    return (
        ModelMessage(
            ModelRole.ASSISTANT,
            (
                ModelPart(
                    "tool_call", tool_call_id=call_id, tool_name=name, arguments={}
                ),
            ),
        ),
        ModelMessage(
            ModelRole.TOOL,
            (ModelPart("tool_result", text, tool_call_id=call_id, tool_name=name),),
        ),
    )


ESTIMATOR = SimpleTokenEstimator()


def project(
    messages: tuple[ModelMessage, ...],
    *,
    remaining_tokens: int = 0,
    remaining_steps: int = 0,
) -> tuple[ModelMessage, ...]:
    return project_coding_tool_results(
        messages,
        BehaviorObservation(1, remaining_steps, 100, remaining_tokens, 60, (), False),
        ESTIMATOR.estimate(messages),
        0,
        ESTIMATOR,
    )


def test_projection_shortens_old_read_results() -> None:
    messages = batch("list_files", "x" * 5000) + sum(
        (batch("read_file", "y" * 2000) for _ in range(3)), ()
    )
    projected = project(messages)
    assert projected[1].parts[0].text_content == "x" * PREVIEW_CHARS + SHORTENED_NOTICE
    assert messages[1].parts[0].text_content == "x" * 5000


def test_projection_keeps_recent_batches_and_mutation_results() -> None:
    messages = sum(
        (
            batch(name, "x" * 2000)
            for name in (
                "apply_patch",
                "write_file",
                "edit_file",
                "read_file",
                "read_file",
            )
        ),
        (),
    )
    assert project(messages) == messages


def test_projection_leaves_short_results_and_preserves_structure() -> None:
    messages = (
        batch("read_file", "x" * SHORTEN_ABOVE_CHARS)
        + batch("list_files", "y" * 5000)
        + batch("read_file", "z")
        + batch("read_file", "z")
    )
    projected = project(messages)
    assert projected[1] == messages[1]
    assert len(projected) == len(messages)
    for before, after in zip(messages, projected, strict=True):
        assert before.role == after.role
        assert [
            (p.kind, p.tool_call_id, p.tool_name, p.arguments) for p in before.parts
        ] == [(p.kind, p.tool_call_id, p.tool_name, p.arguments) for p in after.parts]


def reads(count: int, size: int = 4000) -> tuple[ModelMessage, ...]:
    return sum((batch("read_file", "y" * size) for _ in range(count)), ())


def test_request_allowance_divides_remaining_tokens_by_remaining_requests() -> None:
    assert (
        request_allowance(BehaviorObservation(1, 19, 100, 200_000, 60, (), False))
        == 10_000
    )
    assert (
        request_allowance(BehaviorObservation(20, 0, 100, 7_000, 60, (), False))
        == 7_000
    )


def test_projection_unchanged_within_allowance() -> None:
    messages = reads(6)
    assert project(messages, remaining_tokens=ESTIMATOR.estimate(messages)) == messages


def test_projection_shortens_oldest_first_until_within_allowance() -> None:
    messages = reads(6)
    one_result = ESTIMATOR.estimate(messages) - ESTIMATOR.estimate(reads(5))
    projected = project(
        messages, remaining_tokens=ESTIMATOR.estimate(messages) - one_result // 2
    )
    results = [m.parts[0].text_content for m in projected if m.role is ModelRole.TOOL]
    assert results[0] is not None and results[0].endswith(SHORTENED_NOTICE)
    assert all(
        text is not None and not text.endswith(SHORTENED_NOTICE) for text in results[1:]
    )
