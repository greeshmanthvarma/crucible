from datetime import UTC, datetime
from pathlib import Path

import pytest

from crucible.domain.errors import InvalidTransition
from crucible.domain.ids import new_id
from crucible.domain.task import Task, TaskStatus


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


def test_task_activates_only_from_provisioning() -> None:
    clock = FixedClock(datetime(2026, 9, 20, tzinfo=UTC))
    task = Task.provisioning(
        task_id=new_id(),
        repository_id=new_id(),
        source_ref="main",
        base_revision="a" * 40,
        workspace_path=Path("/tmp/pending-workspace"),
        clock=clock,
    )

    active = task.activate(workspace_path=Path("/tmp/workspace"), clock=clock)

    assert active.status is TaskStatus.ACTIVE
    assert active.workspace_path == Path("/tmp/workspace")
    with pytest.raises(InvalidTransition):
        active.activate(workspace_path=Path("/tmp/other"), clock=clock)


def test_task_records_provisioning_failure_only_while_provisioning() -> None:
    clock = FixedClock(datetime(2026, 9, 20, tzinfo=UTC))
    task = Task.provisioning(
        task_id=new_id(),
        repository_id=new_id(),
        source_ref="missing",
        base_revision="a" * 40,
        workspace_path=Path("/tmp/workspace"),
        clock=clock,
    )

    failed = task.fail_provisioning("revision_not_found", "missing ref", clock)

    assert failed.status is TaskStatus.PROVISIONING_FAILED
    assert failed.failure_code == "revision_not_found"
    assert failed.failure_detail == "missing ref"
    with pytest.raises(InvalidTransition):
        failed.fail_provisioning("other", "other detail", clock)
