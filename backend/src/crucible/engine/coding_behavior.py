"""Editable, advisory coding strategy. Enforcement lives in trusted callers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ToolObservation:
    name: str
    status: str
    error_code: str | None


@dataclass(frozen=True)
class BehaviorObservation:
    step_number: int
    remaining_steps: int
    remaining_tool_calls: int
    remaining_model_tokens: int
    remaining_active_seconds: float
    recent_tools: tuple[ToolObservation, ...]
    completion_proposed: bool


MUTATION_TOOLS = frozenset({"edit_file", "apply_patch", "write_file"})


def coding_guidance() -> str:
    return (
        "You have a strict limit on model requests, and older tool output "
        "may be shortened. Locate the relevant code quickly with "
        "search_files, read only the line ranges you need, and request "
        "several independent read-only tools in the same turn when "
        "possible. After each investigation step, write one or two "
        "sentences of notes (file, function, line numbers, what must "
        "change), because your notes are never shortened. Do not re-read a "
        "range you already read unless you need its exact text for "
        "edit_file. Make the smallest correct fix with edit_file, then run "
        "a focused test if requests remain. The final workspace diff is "
        "your answer."
    )


def step_guidance(o: BehaviorObservation) -> str:
    total = o.step_number + o.remaining_steps
    lines = [
        f"This is model request {o.step_number} of {total}; about "
        f"{o.remaining_model_tokens} model tokens remain."
    ]
    edited = any(
        t.name in MUTATION_TOOLS and t.status == "succeeded" for t in o.recent_tools
    )
    if o.remaining_steps <= 3:
        lines.append(
            f"Only {o.remaining_steps} requests remain after this one. Make or "
            "correct your edit now; the workspace diff at the end is graded."
        )
    elif not edited and o.step_number * 2 >= total:
        lines.append(
            "At least half of your requests are used and no edit has succeeded "
            "recently. Stop investigating and make your best fix with edit_file "
            "now."
        )
    return "\n".join(lines)
