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
from tests.integration.application.test_integration import accepted_result
from tests.integration.engine.conftest import FixedClock


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
