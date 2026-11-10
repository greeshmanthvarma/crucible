"""Exercise case mining from verified packages through the grouped report."""

import json
from dataclasses import replace
from uuid import UUID

import litellm
import pytest

from crucible.improvement.case_evidence import CasePacketError
from crucible.improvement.case_mining import PinnedPackage, mine_development_cases
from crucible.improvement.cli import run_cli
from crucible.models import responses_gateway
from tests.unit import test_case_diagnosis as scripted
from tests.unit import test_case_evidence as fixtures

patch_package = fixtures.patch_package
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


@pytest.fixture
def packages(tmp_path, patch_package, verified_trial, parallel_records):
    first_root, first, _ = patch_package
    parallel_records["tool_calls"] = tuple(
        replace(call, name="apply_patch") for call in parallel_records["tool_calls"]
    )
    first_result, second_result = parallel_records["tool_results"]
    parallel_records["tool_results"] = (
        replace(first_result, status=fixtures.ToolResultStatus.FAILED),
        second_result,
    )
    template = fixtures.with_evidence(verified_trial, parallel_records, b"")
    other_root = tmp_path / "other-package"
    other = fixtures.seal(
        other_root,
        tuple(
            replace(
                fixtures.passing(template, uid, "second-case", repeat), verdict="fail"
            )
            for uid, repeat in ((91, 1), (92, 2))
        ),
    )
    return (
        PinnedPackage(first_root, first.bundle_digest),
        PinnedPackage(other_root, other.bundle_digest),
    )


def reply(*refs, relationship="linked"):
    return scripted.completed(scripted.diagnosis(relationship, refs))


async def test_mines_failed_attempts_and_counts_distinct_cases(packages):
    gateway = scripted.RecordingGateway(
        [
            reply("E6", relationship="recovered"),
            scripted.completed(scripted.disclosure("E6")),
            reply("E6"),
            reply("E6"),
        ]
    )
    report = await mine_development_cases(
        packages[::-1], gateway=gateway, model="scripted", batch_token_cap=40_000
    )
    assert len(gateway.requests) == 4
    assert [attempt.trial_id for attempt in report.attempts] == [
        UUID(int=40),
        UUID(int=91),
        UUID(int=92),
    ]
    assert [attempt.outcome for attempt in report.attempts] == ["grouped"] * 3
    assert report.reserved_tokens == sum(
        attempt.reserved_tokens for attempt in report.attempts
    )
    assert report.attempts[1].disclosed_refs == ("E6",)
    assert report.attempts[1].bundle_digest == packages[1].bundle_digest
    (group,) = report.groups
    assert (group.label, group.attempt_count, group.distinct_case_count) == (
        "apply_patch_failure",
        3,
        2,
    )
    assert group.representative_trial_id == UUID(int=91)
    assert group.attempts[0].diagnosis.relationship == "recovered"
    assert group.attempts[1].cited_results[0].excerpt


async def test_failure_spends_shared_reservation_and_preserves_unresolved(packages):
    replies = [scripted.completed("{"), reply(relationship="unknown")]
    full_gateway = scripted.RecordingGateway([*replies, reply("E6")])
    full = await mine_development_cases(
        packages, gateway=full_gateway, model="scripted", batch_token_cap=40_000
    )
    cap = sum(attempt.reserved_tokens for attempt in full.attempts[:2])
    gateway = scripted.RecordingGateway(replies)
    report = await mine_development_cases(
        packages, gateway=gateway, model="scripted", batch_token_cap=cap
    )
    assert len(gateway.requests) == 2
    assert report.reserved_tokens == cap
    assert [(attempt.outcome, attempt.error_code) for attempt in report.attempts] == [
        ("failed", "invalid_response"),
        ("unresolved", None),
        ("skipped", "budget_exceeded"),
    ]
    assert report.unresolved[0].reason == "no_eligible_failure"


async def test_preflights_every_package_before_dispatch(packages):
    gateway = scripted.RecordingGateway([])
    for inputs, error, code in (
        ((packages[0], packages[0]), ValueError, "duplicate_trial"),
        (
            (packages[0], replace(packages[1], bundle_digest="0" * 64)),
            CasePacketError,
            "package_mismatch",
        ),
    ):
        with pytest.raises(error, match=code):
            await mine_development_cases(
                inputs, gateway=gateway, model="scripted", batch_token_cap=40_000
            )
    assert gateway.requests == []


async def test_cli_requires_opt_in_and_returns_report_without_experiment_state(
    packages, monkeypatch, capsys
):
    gateway = scripted.RecordingGateway([reply("E6")])

    constructions = []

    def factory(*, disable_retries):
        assert disable_retries
        assert litellm.suppress_debug_info
        assert not gateway.requests
        constructions.append(True)
        return gateway

    monkeypatch.setattr(responses_gateway, "LiteLLMResponsesGateway", factory)
    monkeypatch.setattr(litellm, "suppress_debug_info", False)
    package = packages[0]
    args = [
        "case-mine",
        "--package",
        str(package.root),
        package.bundle_digest,
        "--model",
        "scripted",
        "--batch-token-cap",
        "24000",
    ]
    assert await run_cli(args) == 2
    assert "requires --authorize-paid-models" in capsys.readouterr().err
    assert not constructions and not gateway.requests
    assert await run_cli([*args, "--authorize-paid-models"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["groups"][0]["label"] == "apply_patch_failure"
    assert len(gateway.requests) == 1
