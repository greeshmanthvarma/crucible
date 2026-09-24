from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crucible.domain.approvals import (
    Approval,
    ApprovalStatus,
    InvalidApprovalTransition,
)
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id

NOW = datetime(2026, 9, 23, tzinfo=UTC)


def command(**changes: object) -> CommandSpec:
    values: dict[str, object] = {
        "executable": "python",
        "arguments": ("-m", "pytest"),
        "cwd": ".",
        "timeout_seconds": 60,
        "network": CommandNetwork.NONE,
        "environment": {"CI": "1"},
        "image": "runner@sha256:" + "a" * 64,
        "reason": "Run tests",
        "limits": CommandLimits(2.0, 2 * 1024**3, 256, 1_000_000),
    }
    values.update(changes)
    return CommandSpec(**values)  # type: ignore[arg-type]


def test_command_digest_covers_every_authority_field() -> None:
    baseline = command()

    assert baseline.digest == command().digest
    assert baseline.digest != replace(baseline, network=CommandNetwork.OUTBOUND).digest
    assert baseline.digest != replace(baseline, arguments=("-V",)).digest
    assert (
        baseline.digest
        != replace(baseline, limits=replace(baseline.limits, memory_bytes=1024)).digest
    )


def test_command_rejects_shell_traversal_and_secret_environment() -> None:
    with pytest.raises(ValueError, match="relative"):
        command(cwd="../outside")
    with pytest.raises(ValueError, match="credential"):
        command(environment={"OPENAI_API_KEY": "secret"})
    with pytest.raises(ValueError, match="executable"):
        command(executable="")


def test_approval_is_digest_bound_and_single_use() -> None:
    spec = command()
    approval = Approval.requested(
        new_id(), new_id(), new_id(), new_id(), new_id(), spec, NOW
    )

    approved = approval.approve("local-user", NOW)

    assert approval.status is ApprovalStatus.PENDING
    assert approved.status is ApprovalStatus.APPROVED
    assert approved.spec_digest == spec.digest
    with pytest.raises(InvalidApprovalTransition):
        approved.approve("local-user", NOW)
