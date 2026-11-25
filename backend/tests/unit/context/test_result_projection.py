from collections.abc import Mapping
from dataclasses import replace

from crucible.context.invariants import SimpleTokenEstimator
from crucible.context.result_projection import (
    PREVIEW_CHARS,
    SHORTENED_NOTICE,
    project_coding_tool_results,
    request_allowance,
)
from crucible.domain.ids import new_id
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import (
    ModelMessage,
    ModelPart,
    ModelRole,
    PreparedModelRequest,
)
from crucible.repair.worker_protocol import decode_request, encode_request


def batch(
    name: str, text: str, arguments: Mapping[str, object] | None = None
) -> tuple[ModelMessage, ...]:
    call_id = new_id()
    return (
        ModelMessage(
            ModelRole.ASSISTANT,
            (
                ModelPart(
                    "tool_call",
                    tool_call_id=call_id,
                    tool_name=name,
                    arguments=arguments or {},
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


def test_masking_preserves_call_result_pairing() -> None:
    messages = (
        batch("read_file", "x" * 500)
        + batch("list_files", "y" * 5000)
        + batch("read_file", "z" * 500)
        + batch("read_file", "z" * 500)
    )
    projected = project(messages)
    assert projected[1] != messages[1]
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


def test_masks_all_but_last_three_results() -> None:
    messages = reads(6)
    projected = project(messages, remaining_tokens=100_000)
    assert projected[-6:] == messages[-6:]
    assert all(projected[index] != messages[index] for index in (1, 3, 5))
    assert messages[1].parts[0].text_content == "y" * 4000


def test_projection_fallback_shortens_oldest_eligible_result() -> None:
    messages = reads(6)
    masked = project(messages, remaining_tokens=100_000)
    allowance = ESTIMATOR.estimate(masked) - 1
    projected = project(messages, remaining_tokens=allowance)
    assert projected[7].parts[0].text_content == "y" * PREVIEW_CHARS + SHORTENED_NOTICE
    assert projected[:7] == masked[:7]
    assert projected[8:] == masked[8:]


def test_masking_keeps_edit_target_and_latest_failure() -> None:
    content = "x" * 2000
    earlier_failure = batch("execute_command", content)
    latest_failure = batch("execute_command", content)
    messages = (
        earlier_failure[:1]
        + (
            replace(
                earlier_failure[1],
                parts=(replace(earlier_failure[1].parts[0], exit_code=1),),
            ),
        )
        + latest_failure[:1]
        + (
            replace(
                latest_failure[1],
                parts=(replace(latest_failure[1].parts[0], exit_code=2),),
            ),
        )
        + batch("execute_command", content)
    )
    for name in ("edit_file", "write_file", "apply_patch"):
        path = f"{name}.py"
        args = {"path": path}
        edit_args = (
            {"patch": f"--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n-x\n+y\n"}
            if name == "apply_patch"
            else args
        )
        messages += (
            batch("read_file", content, args)
            + batch("read_file", content, args)
            + batch(name, "edited", edit_args)
        )
    messages += reads(3)
    projected = project(messages)
    for index in (3, 9, 15, 21):
        assert projected[index] == messages[index]
    for index in (1, 5, 7, 13, 19):
        assert projected[index] != messages[index]


def test_mask_stub_names_tool_and_target() -> None:
    cases: tuple[tuple[str, Mapping[str, object], str], ...] = (
        (
            "read_file",
            {"path": "src/a.py", "start_line": 10, "end_line": 20},
            "src/a.py lines 10-20",
        ),
        (
            "search_files",
            {"query": "class Widget", "path": "src"},
            "'class Widget' in src",
        ),
        ("list_files", {"path": "src"}, "src"),
        (
            "execute_command",
            {"executable": "python", "arguments": ["-m", "pytest"]},
            "python -m pytest",
        ),
        ("workspace_diff", {}, "workspace"),
    )
    for tool, args, target in cases:
        messages = batch(tool, "x" * 500, args) + reads(3)
        assert project(messages, remaining_tokens=100_000)[1].parts[0].text_content == (
            f"[masked {tool}: {target}; call again to view]"
        )


def test_small_results_are_not_masked() -> None:
    messages = reads(1, 201) + reads(4, 200) + reads(3, 201)
    projected = project(messages, remaining_tokens=100_000)
    assert projected[1] != messages[1]
    assert projected[2:] == messages[2:]


def test_projection_metadata_stays_outside_worker_wire() -> None:
    call, result = batch("execute_command", "test failed")
    result = replace(result, parts=(replace(result.parts[0], exit_code=1),))
    messages = project((call, result))
    request = PreparedModelRequest(new_id(), new_id(), "fake", messages, (), 1024)
    decoded = decode_request(encode_request(request))
    assert decoded.messages == (
        call,
        replace(result, parts=(replace(result.parts[0], exit_code=None),)),
    )
