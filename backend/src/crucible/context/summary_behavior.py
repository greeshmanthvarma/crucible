"""Editable compaction prompt and rendering, with protected summary DTOs."""

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crucible.context.compaction import CompactionRequest, CompactionSummary


def build_compaction_prompt(request: "CompactionRequest") -> str:
    source = [
        {
            "role": message.role.value,
            "parts": [
                part.text_content or part.reasoning_content or ""
                for part in message.parts
            ],
        }
        for unit in request.source_units
        for message in unit.messages
    ]
    schema = ", ".join(
        (
            "objective_and_constraints",
            "decisions",
            "repository_facts",
            "changes",
            "commands_and_validation",
            "unresolved_problems",
            "execution_state",
            "important_paths_and_symbols",
        )
    )
    return (
        "Summarize these complete Conversation units without inventing facts. "
        f"Return only one JSON object with string fields: {schema}. "
        f"Prompt version: {request.prompt_version}.\n"
        + json.dumps(source, separators=(",", ":"))
    )


def render_summary(summary: "CompactionSummary") -> str:
    sections = (
        ("User objective and constraints", summary.objective_and_constraints),
        ("Settled decisions", summary.decisions),
        ("Repository facts", summary.repository_facts),
        ("Changes made", summary.changes),
        ("Commands and Validation", summary.commands_and_validation),
        ("Unresolved problems", summary.unresolved_problems),
        ("Current execution state", summary.execution_state),
        ("Important paths and symbols", summary.important_paths_and_symbols),
    )
    return "\n\n".join(f"## {heading}\n{body}" for heading, body in sections)
