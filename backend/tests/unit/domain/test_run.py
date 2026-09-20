from datetime import UTC, datetime, timedelta

import pytest

from crucible.domain.errors import InvalidTransition
from crucible.domain.ids import new_id
from crucible.domain.run import Run, RunStatus


def queued_run(now: datetime) -> Run:
    return Run.queued(
        run_id=new_id(),
        task_id=new_id(),
        triggering_message_id=new_id(),
        created_at=now,
    )


def test_run_claim_is_single_transition() -> None:
    now = datetime(2026, 9, 20, tzinfo=UTC)
    future = now + timedelta(minutes=1)
    run = queued_run(now)

    running = run.claim(
        execution_id=new_id(),
        now=now,
        lease_expires_at=future,
    )

    assert running.status is RunStatus.RUNNING
    with pytest.raises(InvalidTransition):
        running.claim(
            execution_id=new_id(),
            now=now,
            lease_expires_at=future,
        )


def test_run_rejects_non_utc_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        queued_run(datetime(2026, 9, 20))


@pytest.mark.parametrize("transition", ["complete", "fail"])
def test_running_run_reaches_one_terminal_state(transition: str) -> None:
    now = datetime(2026, 9, 20, tzinfo=UTC)
    running = queued_run(now).claim(
        execution_id=new_id(),
        now=now,
        lease_expires_at=now + timedelta(minutes=1),
    )

    if transition == "complete":
        terminal = running.complete(now=now)
        assert terminal.status is RunStatus.COMPLETED
    else:
        terminal = running.fail("model_error", "gateway unavailable", now=now)
        assert terminal.status is RunStatus.FAILED
        assert terminal.outcome_code == "model_error"
        assert terminal.outcome_detail == "gateway unavailable"

    with pytest.raises(InvalidTransition):
        terminal.complete(now=now)
    with pytest.raises(InvalidTransition):
        terminal.fail("other", "other detail", now=now)
