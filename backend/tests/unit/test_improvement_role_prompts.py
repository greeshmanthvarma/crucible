"""Role objectives are trusted instructions, not implicit schema semantics."""

import pytest


@pytest.mark.parametrize("kind", ["mining", "format_correction", "proposal"])
def test_role_prompt_states_harness_target_and_evidence_boundary(kind):
    from crucible.improvement.role_prompts import role_system_prompt

    prompt = role_system_prompt(kind)
    for required in (
        "Crucible",
        "benchmark",
        "untrusted",
        "allowed_paths",
        "read_file_chunk",
        "next_offset",
        "JSON",
        "validation",
        "final-test",
        "budget",
    ):
        assert required in prompt


def test_mining_prompt_requires_hypotheses_abstention_and_no_edits():
    from crucible.improvement.role_prompts import role_system_prompt

    prompt = role_system_prompt("mining")
    for required in (
        "read-only",
        "generalizable",
        "observations",
        "hypotheses",
        "insufficient_evidence",
        "out_of_scope",
        "no_actionable_weakness",
        "Do not manufacture",
        "passing",
    ):
        assert required in prompt
    assert "format correction" in role_system_prompt("format_correction")


def test_proposer_prompt_requires_real_scoped_edits_and_honest_validation():
    from crucible.improvement.role_prompts import role_system_prompt

    prompt = role_system_prompt("proposal")
    for required in (
        "one weakness",
        "hypothesis",
        "source/",
        "actual file edits",
        "smallest",
        "task IDs",
        "evaluation gates",
        "passing",
        "claimed_paths",
        "controller",
        "Do not commit",
        "unverified",
    ):
        assert required in prompt
