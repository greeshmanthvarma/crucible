import subprocess
from pathlib import Path

import pytest

from crucible.application.integration_service import (
    IntegrationService,
    IntegrationTarget,
)
from crucible.application.task_service import TaskService
from crucible.domain.ids import new_id
from crucible.domain.results import Integration, IntegrationStatus
from crucible.storage.database import Database
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager
from tests.contract.api.test_approvals import pending_approval as _pending_approval
from tests.integration.application import test_acceptance as acceptance_tests
from tests.integration.application import test_event_replay as event_replay_tests
from tests.integration.application.test_integration import accepted_result
from tests.integration.context.test_compaction_lifecycle import (
    test_successful_compaction_persists_private_artifact_and_lineage,
)
from tests.integration.engine.conftest import FixedClock, uow_factory
from tests.integration.engine.test_steering import (
    test_steering_enters_only_the_next_prepared_model_boundary as _steering_recovery,
)
from tests.integration.engine.test_supervisor import (
    test_reconcile_terminalizes_orphaned_tool_calls as _validation_recovery,
)

_acceptance_recovery = getattr(
    acceptance_tests,
    "test_acceptance_recovers_owned_commit_created_before_database_record",
)
_event_replay = getattr(
    event_replay_tests,
    "test_event_replay_is_ordered_and_validates_cursor_ownership",
)


# Keep the restart matrix explicit in one place. These scenarios are also collected
# in their feature-focused modules; invoking them here proves that the complete
# recovery contract stays green as a unit.
@pytest.mark.parametrize(
    "checkpoint",
    [
        "steering_before_step",
        "compaction_completed",
        "validation_approval_pending",
        "validation_command_active",
        "acceptance_commit_before_sqlite",
        "sse_replay",
    ],
)
async def test_non_integration_recovery_matrix(
    database: Database, tmp_path: Path, checkpoint: str
) -> None:
    if checkpoint == "steering_before_step":
        await _steering_recovery(database, tmp_path)
    elif checkpoint == "compaction_completed":
        await test_successful_compaction_persists_private_artifact_and_lineage(
            database, tmp_path
        )
    elif checkpoint == "validation_approval_pending":
        approval = await _pending_approval(database)
        async with uow_factory(database)() as uow:
            retained = await uow.approvals.list_for_task(approval.task_id)
        assert retained == (approval,)
    elif checkpoint == "validation_command_active":
        await _validation_recovery(database)
    elif checkpoint == "acceptance_commit_before_sqlite":
        await _acceptance_recovery(database, tmp_path)
    else:
        await _event_replay(database, tmp_path)


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.mark.parametrize("checkpoint", ["before_apply", "after_apply"])
async def test_pending_integration_recovers_only_a_provable_git_effect(
    database: Database, tmp_path: Path, checkpoint: str
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, f"restart-{checkpoint}"
    )
    root = repository.root_path
    expected = git(root, "rev-parse", "HEAD")
    target_ref = git(root, "symbolic-ref", "--short", "HEAD")
    pending = Integration.pending(
        new_id(),
        result.id,
        repository.id,
        target_ref,
        expected,
        f"restart-{checkpoint}",
        FixedClock().now(),
    )
    async with factory() as uow:
        await uow.integrations.add(pending)
        await uow.commit()
    if checkpoint == "after_apply":
        git(root, "cherry-pick", "-x", result.commit_sha)

    recovered = await IntegrationService(
        SubprocessGitClient(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        f"restart-{checkpoint}",
    )

    assert recovered.status is IntegrationStatus.COMPLETED
    assert git(root, "rev-list", "--count", "HEAD") == "2"
    reopened = TaskService(
        WorkspaceManager(SubprocessGitClient(), tmp_path / "unused"),
        factory,
        FixedClock(),
    )
    stored_task = await reopened.get(task.id)
    review = await reopened.review(task.id)
    assert stored_task.status.value == "integrated"
    assert [revision.id for revision in review.result_revisions] == [result.id]
    assert [integration.id for integration in review.integrations] == [recovered.id]
    assert review.integrations[0].observed_after_revision == git(
        root, "rev-parse", "HEAD"
    )
