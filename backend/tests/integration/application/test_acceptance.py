import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from crucible.application.acceptance_service import AcceptanceService
from crucible.application.artifact_service import ArtifactService
from crucible.application.errors import AcceptanceRejected
from crucible.application.message_service import MessageService
from crucible.application.validation_service import ValidationService
from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.repository import RepositorySettings
from crucible.domain.tools import ToolResultStatus
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.storage.database import Database
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import ToolRegistry
from crucible.workspaces.git import SubprocessGitClient
from tests.integration.application.test_validation_service import (
    ScriptedValidationTool,
)
from tests.integration.engine.conftest import (
    FixedClock,
    PassiveSupervisor,
    queued_run,
    uow_factory,
)
from tests.integration.engine.test_validation_repair import (
    RepairGateway,
    configured_run,
)


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout


async def completed_task(database: Database, tmp_path: Path, name: str):
    submitted = await queued_run(database, tmp_path, name)
    factory = uow_factory(database)
    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(
        submitted.run_id
    )
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        task = await uow.tasks.get(run.task_id)
        assert task is not None
        repository = await uow.repositories.get(task.repository_id)
        assert repository is not None
    return factory, task, repository


def acceptance_service(
    factory, tmp_path: Path
) -> tuple[AcceptanceService, ArtifactService]:
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
    )
    return (
        AcceptanceService(SubprocessGitClient(), artifacts, factory, FixedClock()),
        artifacts,
    )


async def test_acceptance_commits_evidence_without_touching_registered_checkout(
    database: Database, tmp_path: Path
) -> None:
    factory, task, repository = await completed_task(database, tmp_path, "acceptance")
    (task.workspace_path / "README.md").write_text("changed\n")
    (task.workspace_path / "new.txt").write_text("untracked\n")
    checkout_head = git(repository.root_path, "rev-parse", "HEAD")
    checkout_status = git(repository.root_path, "status", "--porcelain=v2")
    service, artifacts = acceptance_service(factory, tmp_path)

    result = await service.accept(task.id, "accept-once")

    assert result.parent_revision == task.base_revision
    assert git(task.workspace_path, "rev-parse", "HEAD").strip() == result.commit_sha
    assert git(
        task.workspace_path, "show", "--format=", "--name-only", "HEAD"
    ).split() == [
        "README.md",
        "new.txt",
    ]
    artifact, diff = await artifacts.read(result.diff_artifact_id)
    assert artifact.media_type == "application/vnd.git-diff"
    assert b"README.md" in diff and b"new.txt" in diff
    assert result.validation_snapshot["status"] == "not_configured"
    assert result.summary.startswith("Fake response")
    async with factory() as uow:
        accepted_task = await uow.tasks.get(task.id)
        events = await uow.events.list_after(task.id, 0)
    assert accepted_task is not None and accepted_task.status.value == "accepted"
    assert events[-1].type.value == "task.accepted"
    assert git(repository.root_path, "rev-parse", "HEAD") == checkout_head
    assert git(repository.root_path, "status", "--porcelain=v2") == checkout_status


async def test_acceptance_rejects_active_run_and_workspace_without_changes(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "acceptance-active")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    service, _artifacts = acceptance_service(factory, tmp_path)

    with pytest.raises(AcceptanceRejected, match="Run is active"):
        await service.accept(run.task_id, "active")

    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(run.id)
    with pytest.raises(AcceptanceRejected, match="no changes"):
        await service.accept(run.task_id, "clean")


async def test_acceptance_rejects_failed_required_validation(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "acceptance-invalid")
    factory = uow_factory(database)
    spec = CommandSpec(
        "python",
        ("-m", "pytest"),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        "runner@sha256:" + "a" * 64,
        "Authoritative tests",
        CommandLimits(1, 1024**3, 64, 100_000),
    )
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        await uow.runs.update(
            replace(
                run,
                settings_snapshot=RepositorySettings(validation_commands=(spec,)),
            )
        )
        await uow.commit()
    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(run.id)
    async with factory() as uow:
        task = await uow.tasks.get(run.task_id)
        assert task is not None
    (task.workspace_path / "README.md").write_text("invalid\n")
    service, _artifacts = acceptance_service(factory, tmp_path)

    with pytest.raises(AcceptanceRejected, match="did not complete successfully"):
        await service.accept(task.id, "invalid")


async def test_acceptance_snapshots_authoritative_command_evidence(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=0)
    tool = ScriptedValidationTool((ToolResultStatus.SUCCEEDED,), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())
    assert await RunEngine(
        factory,
        FixedClock(),
        RepairGateway(("complete",)),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
    ).execute(submitted.run_id)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        task = await uow.tasks.get(run.task_id)
        assert task is not None
    (task.workspace_path / "README.md").write_text("validated\n")
    service, _artifacts = acceptance_service(factory, tmp_path)

    result = await service.accept(task.id, "validated-acceptance")

    command = result.validation_snapshot["commands"][0]
    assert command["status"] == "passed"
    assert command["approvalId"]
    assert command["toolCallId"]
    assert command["artifactId"]


async def test_acceptance_retry_and_later_correction_preserve_both_revisions(
    database: Database, tmp_path: Path
) -> None:
    factory, task, _repository = await completed_task(
        database, tmp_path, "acceptance-correction"
    )
    (task.workspace_path / "README.md").write_text("first\n")
    service, _artifacts = acceptance_service(factory, tmp_path)

    first = await service.accept(task.id, "first-acceptance")
    retry = await service.accept(task.id, "first-acceptance")
    assert retry == first

    submitted = await MessageService(factory, FixedClock(), PassiveSupervisor()).submit(
        task.id, "Correct the result", "correction-run"
    )
    assert submitted.run_id != first.id
    assert await RunEngine(factory, FixedClock(), FakeModelGateway()).execute(
        submitted.run_id
    )
    (task.workspace_path / "README.md").write_text("second\n")
    second = await service.accept(task.id, "second-acceptance")

    assert second.id != first.id
    assert second.previous_result_revision_id == first.id
    assert second.parent_revision == first.commit_sha
    async with factory() as uow:
        revisions = await uow.result_revisions.list_for_task(task.id)
    assert [revision.id for revision in revisions] == [first.id, second.id]


async def test_acceptance_recovers_owned_commit_created_before_database_record(
    database: Database, tmp_path: Path
) -> None:
    factory, task, _repository = await completed_task(
        database, tmp_path, "acceptance-recovery"
    )
    (task.workspace_path / "README.md").write_text("recovered\n")
    git_client = SubprocessGitClient()
    orphan = await git_client.create_acceptance_commit(
        task.workspace_path, str(task.id), "recover-acceptance", FixedClock().now()
    )
    service = AcceptanceService(
        git_client,
        ArtifactService(
            LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
        ),
        factory,
        FixedClock(),
    )

    recovered = await service.accept(task.id, "recover-acceptance")

    assert recovered.commit_sha == orphan.commit_sha
    assert git(task.workspace_path, "rev-list", "--count", "HEAD").strip() == "2"
