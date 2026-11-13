from crucible.context.invariants import SimpleTokenEstimator
from crucible.context.result_projection import (
    PREVIEW_CHARS,
    SHORTEN_ABOVE_CHARS,
    SHORTENED_NOTICE,
    project_coding_tool_results,
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


def project(messages: tuple[ModelMessage, ...]) -> tuple[ModelMessage, ...]:
    return project_coding_tool_results(
        messages,
        BehaviorObservation(1, 20, 100, 200_000, 60, (), False),
        0,
        0,
        SimpleTokenEstimator(),
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
