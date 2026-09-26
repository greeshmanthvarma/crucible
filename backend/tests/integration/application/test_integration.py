import asyncio
import subprocess
from pathlib import Path

import pytest

from crucible.application.errors import IdempotencyConflict
from crucible.application.integration_service import (
    IntegrationService,
    IntegrationTarget,
)
from crucible.application.message_service import MessageService
from crucible.application.reconciliation import StartupReconciler
from crucible.application.repository_service import RepositoryService
from crucible.domain.ids import new_id
from crucible.domain.results import Integration, IntegrationStatus
from crucible.storage.database import Database
from crucible.workspaces.git import GitResult, SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager
from tests.contract.api.test_repositories import create_repository
from tests.integration.application.test_acceptance import (
    acceptance_service,
    completed_task,
)
from tests.integration.application.test_message_submission import RecordingSupervisor
from tests.integration.engine.conftest import FixedClock, uow_factory


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


class UnrestorableGit(SubprocessGitClient):
    async def abort_cherry_pick(self, root: Path) -> GitResult:
        return GitResult(1, "", "simulated abort failure")


class BlockingGit(SubprocessGitClient):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active_mutations = 0
        self.maximum_active_mutations = 0

    async def cherry_pick(self, root: Path, commit_sha: str) -> GitResult:
        self.active_mutations += 1
        self.maximum_active_mutations = max(
            self.maximum_active_mutations, self.active_mutations
        )
        self.entered.set()
        await self.release.wait()
        try:
            return await super().cherry_pick(root, commit_sha)
        finally:
            self.active_mutations -= 1


async def accepted_result(database: Database, tmp_path: Path, name: str):
    factory, task, repository = await completed_task(database, tmp_path, name)
    (task.workspace_path / "README.md").write_text(f"integrated {name}\n")
    (task.workspace_path / "collision.txt").write_text("result version\n")
    acceptance, _artifacts = acceptance_service(factory, tmp_path)
    result = await acceptance.accept(task.id, f"accept-{name}")
    return factory, task, repository, result


async def test_clean_target_integrates_selected_result_revision(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, "integration-success"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD")
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    workspace_head = git(task.workspace_path, "rev-parse", "HEAD")
    service = IntegrationService(SubprocessGitClient(), factory, FixedClock())

    integration = await service.integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "integrate-once",
    )

    assert integration.status is IntegrationStatus.COMPLETED
    assert integration.observed_before_revision == expected
    assert integration.observed_after_revision == git(
        repository.root_path, "rev-parse", "HEAD"
    )
    assert git(repository.root_path, "show", "HEAD:README.md") == (
        "integrated integration-success"
    )
    assert git(task.workspace_path, "rev-parse", "HEAD") == workspace_head
    async with factory() as uow:
        stored_task = await uow.tasks.get(task.id)
        events = await uow.events.list_after(task.id, 0)
    assert stored_task is not None and stored_task.status.value == "integrated"
    assert events[-1].type.value == "task.integrated"


async def test_integrated_chat_continues_in_new_workspace_generation(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, "continuation"
    )
    git_client = SubprocessGitClient()
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    await IntegrationService(git_client, factory, FixedClock()).integrate(
        result.id,
        IntegrationTarget(
            repository.id, target_ref, git(repository.root_path, "rev-parse", "HEAD")
        ),
        "integrate-continuation",
    )
    (repository.root_path / "later.txt").write_text("later\n")
    git(repository.root_path, "add", "later.txt")
    git(repository.root_path, "commit", "-m", "advance target")
    current_head = git(repository.root_path, "rev-parse", "HEAD")
    workspaces = WorkspaceManager(git_client, tmp_path / "data")
    supervisor = RecordingSupervisor()
    messages = MessageService(
        factory, FixedClock(), supervisor, git=git_client, workspaces=workspaces
    )

    submitted = await messages.submit(task.id, "Continue in this chat", "continue-1")
    retried = await messages.submit(task.id, "Continue in this chat", "continue-1")
    assert retried == submitted
    assert submitted.run_id in supervisor.submissions
    async with factory() as uow:
        continued = await uow.tasks.get(task.id)
        conversation = await uow.messages.list_for_task(task.id)
    assert continued is not None
    assert continued.status.value == "active"
    assert continued.workspace_generation == 1
    assert continued.source_ref == task.source_ref
    assert continued.base_revision == task.base_revision
    assert continued.workspace_base_revision == current_head
    assert continued.workspace_path != task.workspace_path
    assert git(continued.workspace_path, "rev-parse", "HEAD") == current_head
    assert git(task.workspace_path, "rev-parse", "HEAD") == result.commit_sha
    assert conversation[-1].parts[0].text_content == "Continue in this chat"
    assert sum(
        message.parts[0].text_content == "Continue in this chat"
        for message in conversation
    ) == 1


async def test_startup_recovers_a_recorded_continuation(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, "continuation-recovery"
    )
    git_client = SubprocessGitClient()
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    await IntegrationService(git_client, factory, FixedClock()).integrate(
        result.id,
        IntegrationTarget(
            repository.id, target_ref, git(repository.root_path, "rev-parse", "HEAD")
        ),
        "integrate-recovery",
    )
    workspaces = WorkspaceManager(git_client, tmp_path / "data")
    async with factory() as uow:
        integrated = await uow.tasks.get(task.id)
        assert integrated is not None
        continuing = integrated.begin_continuation(
            base_revision=git(repository.root_path, "rev-parse", "HEAD"),
            workspace_path=workspaces.destination_for(task.id, 1),
            clock=FixedClock(),
        )
        await uow.tasks.update(continuing)
        await uow.commit()

    await StartupReconciler(workspaces, factory, FixedClock()).reconcile()

    async with factory() as uow:
        recovered = await uow.tasks.get(task.id)
    assert recovered is not None and recovered.status.value == "active"
    assert recovered.workspace_generation == 1
    assert (
        git(recovered.workspace_path, "rev-parse", "HEAD")
        == continuing.workspace_base_revision
    )


@pytest.mark.parametrize("dirty_kind", ["tracked", "untracked"])
async def test_dirty_target_is_rejected_without_mutation(
    database: Database, tmp_path: Path, dirty_kind: str
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, f"integration-dirty-{dirty_kind}"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD")
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    if dirty_kind == "tracked":
        (repository.root_path / "README.md").write_text("dirty\n")
    else:
        (repository.root_path / "collision.txt").write_text("untracked\n")
    before_status = git(repository.root_path, "status", "--porcelain=v2")

    integration = await IntegrationService(
        SubprocessGitClient(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        f"dirty-{dirty_kind}",
    )

    assert integration.status is IntegrationStatus.FAILED
    assert integration.failure_code == "target_dirty"
    assert git(repository.root_path, "rev-parse", "HEAD") == expected
    assert git(repository.root_path, "status", "--porcelain=v2") == before_status


async def test_target_revision_drift_is_rejected_without_mutation(
    database: Database, tmp_path: Path
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, "integration-drift"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD")
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    (repository.root_path / "target.txt").write_text("advanced\n")
    git(repository.root_path, "add", "target.txt")
    git(repository.root_path, "commit", "-m", "advance target")
    advanced = git(repository.root_path, "rev-parse", "HEAD")

    integration = await IntegrationService(
        SubprocessGitClient(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "drift",
    )

    assert integration.status is IntegrationStatus.FAILED
    assert integration.failure_code == "target_revision_drift"
    assert git(repository.root_path, "rev-parse", "HEAD") == advanced


async def test_conflict_is_aborted_and_target_is_exactly_restored(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, "integration-conflict"
    )
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    (repository.root_path / "README.md").write_text("target version\n")
    git(repository.root_path, "add", "README.md")
    git(repository.root_path, "commit", "-m", "conflicting target")
    expected = git(repository.root_path, "rev-parse", "HEAD")
    before_status = git(repository.root_path, "status", "--porcelain=v2")
    workspace_head = git(task.workspace_path, "rev-parse", "HEAD")

    integration = await IntegrationService(
        SubprocessGitClient(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "conflict",
    )

    assert integration.status is IntegrationStatus.CONFLICT
    assert integration.failure_code == "integration_conflict"
    assert git(repository.root_path, "rev-parse", "HEAD") == expected
    assert git(repository.root_path, "status", "--porcelain=v2") == before_status
    assert (repository.root_path / "README.md").read_text() == "target version\n"
    assert git(task.workspace_path, "rev-parse", "HEAD") == workspace_head


async def test_wrong_repository_and_already_integrated_result_are_rejected(
    database: Database, tmp_path: Path
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, "integration-ownership"
    )
    other_root = tmp_path / "other-repository"
    create_repository(other_root)
    other = await RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    ).register(other_root)
    other_ref = git(other_root, "symbolic-ref", "--short", "HEAD")
    other_head = git(other_root, "rev-parse", "HEAD")
    service = IntegrationService(SubprocessGitClient(), factory, FixedClock())

    wrong = await service.integrate(
        result.id,
        IntegrationTarget(other.repository.id, other_ref, other_head),
        "wrong-repository",
    )
    assert wrong.failure_code == "wrong_repository"
    assert git(other_root, "rev-parse", "HEAD") == other_head

    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    expected = git(repository.root_path, "rev-parse", "HEAD")
    completed = await service.integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "first-integration",
    )
    retry = await service.integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "first-integration",
    )
    assert retry == completed
    with pytest.raises(IdempotencyConflict):
        await service.integrate(
            result.id,
            IntegrationTarget(
                repository.id, target_ref, completed.observed_after_revision
            ),
            "first-integration",
        )
    after = git(repository.root_path, "rev-parse", "HEAD")
    duplicate = await service.integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, after),
        "duplicate-integration",
    )
    assert duplicate.status is IntegrationStatus.FAILED
    assert duplicate.failure_code == "already_integrated"
    assert git(repository.root_path, "rev-parse", "HEAD") == after


async def test_pending_integration_recovers_an_already_applied_cherry_pick(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository, result = await accepted_result(
        database, tmp_path, "integration-recovery"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD")
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    pending = Integration.pending(
        new_id(),
        result.id,
        repository.id,
        target_ref,
        expected,
        "recover-integration",
        FixedClock().now(),
    )
    async with factory() as uow:
        await uow.integrations.add(pending)
        await uow.commit()
    git(repository.root_path, "cherry-pick", "-x", result.commit_sha)
    applied = git(repository.root_path, "rev-parse", "HEAD")

    recovered = await IntegrationService(
        SubprocessGitClient(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "recover-integration",
    )

    assert recovered.status is IntegrationStatus.COMPLETED
    assert recovered.observed_before_revision == expected
    assert recovered.observed_after_revision == applied
    assert git(repository.root_path, "rev-list", "--count", "HEAD") == "2"
    async with factory() as uow:
        stored_task = await uow.tasks.get(task.id)
    assert stored_task is not None and stored_task.status.value == "integrated"


async def test_target_mutations_are_serialized_across_distinct_requests(
    database: Database, tmp_path: Path
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, "integration-serialized"
    )
    expected = git(repository.root_path, "rev-parse", "HEAD")
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    target = IntegrationTarget(repository.id, target_ref, expected)
    blocking_git = BlockingGit()
    service = IntegrationService(blocking_git, factory, FixedClock())

    first = asyncio.create_task(service.integrate(result.id, target, "concurrent-a"))
    await blocking_git.entered.wait()
    second = asyncio.create_task(service.integrate(result.id, target, "concurrent-b"))
    await asyncio.sleep(0)

    assert not second.done()
    assert blocking_git.maximum_active_mutations == 1
    blocking_git.release.set()
    first_result, second_result = await asyncio.gather(first, second)

    assert first_result.status is IntegrationStatus.COMPLETED
    assert second_result.status is IntegrationStatus.FAILED
    assert second_result.failure_code == "already_integrated"
    assert blocking_git.maximum_active_mutations == 1
    assert git(repository.root_path, "rev-list", "--count", "HEAD") == "2"


async def test_unprovable_conflict_restoration_requires_recovery(
    database: Database, tmp_path: Path
) -> None:
    factory, _task, repository, result = await accepted_result(
        database, tmp_path, "integration-recovery-required"
    )
    target_ref = git(repository.root_path, "symbolic-ref", "--short", "HEAD")
    (repository.root_path / "README.md").write_text("target version\n")
    git(repository.root_path, "add", "README.md")
    git(repository.root_path, "commit", "-m", "conflicting target")
    expected = git(repository.root_path, "rev-parse", "HEAD")

    integration = await IntegrationService(
        UnrestorableGit(), factory, FixedClock()
    ).integrate(
        result.id,
        IntegrationTarget(repository.id, target_ref, expected),
        "recovery-required",
    )

    assert integration.status is IntegrationStatus.RECOVERY_REQUIRED
    assert integration.failure_code == "integration_recovery_required"
