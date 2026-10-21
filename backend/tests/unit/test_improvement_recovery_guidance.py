import pytest

from crucible.improvement.recovery_guidance import recovery_actions


@pytest.mark.parametrize(
    "status,cleanup,unsettled,budget,retry,complete,expected",
    [
        ("ready", True, False, 1, False, False, ["continue"]),
        ("ready", True, False, 1, True, False, ["authorize_retry"]),
        ("ready", True, False, 0, True, False, ["remain_blocked"]),
        ("error", True, True, 0, True, False, ["acknowledge_accounting", "recover"]),
        ("error", False, True, 1, False, False, ["investigate", "recover"]),
        ("interrupted", True, False, 1, False, False, ["recover"]),
        ("running", True, False, 1, False, False, []),
        ("closed", False, True, 0, True, False, []),
        ("ready", True, False, 0, False, True, []),
    ],
)
def test_guidance_preserves_recovery_and_execution_gates(
    status, cleanup, unsettled, budget, retry, complete, expected
):
    remaining = dict.fromkeys(
        ("tokens", "requests", "active_seconds", "worker_wall_seconds", "trial_starts"),
        budget,
    )
    assert (
        recovery_actions(
            status,
            cleanup_confirmed=cleanup,
            unsettled_usage=unsettled,
            remaining=remaining,
            retry_required=retry,
            work_complete=complete,
        )
        == expected
    )
