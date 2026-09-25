from datetime import UTC, datetime

import pytest

from crucible.domain.evals import EvalTrial, TrialStatus
from crucible.domain.ids import new_id


def test_trial_transitions_preserve_lineage_and_reject_terminal_rewrite() -> None:
    now = datetime.now(UTC)
    trial = EvalTrial.queued(
        new_id(),
        new_id(),
        new_id(),
        new_id(),
        1,
        "development",
        "case-digest",
        "configuration-digest",
        now,
    )
    trial = trial.prepare(now).run(new_id(), new_id(), new_id(), "a" * 40, now)
    trial = trial.evaluate(now).complete(now)
    assert trial.status is TrialStatus.COMPLETED
    assert trial.task_id is not None and trial.run_id is not None
    with pytest.raises(ValueError, match="terminal"):
        trial.fail("later_error", now)


def test_trial_cannot_complete_without_evaluation() -> None:
    now = datetime.now(UTC)
    trial = EvalTrial.queued(
        new_id(),
        new_id(),
        new_id(),
        new_id(),
        1,
        "development",
        "case-digest",
        "configuration-digest",
        now,
    )
    with pytest.raises(ValueError):
        trial.complete(now)
