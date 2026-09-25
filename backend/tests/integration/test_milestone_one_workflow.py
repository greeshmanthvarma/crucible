import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

from crucible.application.acceptance_service import AcceptanceService
from crucible.application.artifact_service import ArtifactService
from crucible.application.integration_service import (
    IntegrationService,
    IntegrationTarget,
)
from crucible.application.message_service import MessageService, MessageSubmissionKind
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.application.validation_service import ValidationService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.compaction import CompactionLifecycle, ScriptedCompactionGateway
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.repository import RepositorySettings
from crucible.domain.results import IntegrationStatus
from crucible.domain.tools import ToolResultStatus
from crucible.engine.run_engine import RunEngine
from crucible.storage.database import Database
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import ToolRegistry
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager
from tests.integration.application.test_validation_service import ScriptedValidationTool
from tests.integration.context.test_compaction_lifecycle import summary
from tests.integration.engine.conftest import FixedClock, PassiveSupervisor, uow_factory
from tests.integration.engine.test_validation_repair import (
    RepairGateway,
    validation_command,
)


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def fixture_repository(tmp_path: Path) -> Path:
    source = Path(__file__).parents[1] / "fixtures" / "milestone_repository"
    root = tmp_path / "repository"
    shutil.copytree(source, root)
    git(root, "init", "-q")
    git(root, "config", "user.email", "test@example.com")
    git(root, "config", "user.name", "Test User")
    git(root, "add", "README.md")
    git(root, "commit", "-qm", "fixture")
    return root


async def test_complete_milestone_one_workflow_is_deterministic_and_retained(
    database: Database, tmp_path: Path
) -> None:
    root = fixture_repository(tmp_path)
    original_head = git(root, "rev-parse", "HEAD")
    original_status = git(root, "status", "--porcelain=v2")
    factory = uow_factory(database)
    clock = FixedClock()
    git_client = SubprocessGitClient()
    repositories = RepositoryService(git_client, factory, clock)
    tasks = TaskService(WorkspaceManager(git_client, tmp_path / "data"), factory, clock)
    messages = MessageService(factory, clock, PassiveSupervisor())
    registered = await repositories.register(root)
    task = await tasks.create(registered.repository.id, "HEAD", "milestone-task")

    first = await messages.submit(task.id, "Implement " + "x" * 2000, "first-run")
    steering = await messages.submit(task.id, "Keep the public API small", "steer")
    assert steering.kind is MessageSubmissionKind.STEERING
    assert steering.run_id == first.run_id
    assert await RunEngine(
        factory, clock, RepairGateway(("first completion",))
    ).execute(first.run_id)

    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "data" / "artifacts", clock), factory
    )
    context_manager = ContextManager(
        factory,
        clock,
        SimpleTokenEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=450,
        output_reserve=10,
        threshold=0.8,
        recent_complete_units=0,
        compaction_lifecycle=CompactionLifecycle(
            factory,
            artifacts,
            ScriptedCompactionGateway((summary("milestone objective retained"),)),
            clock,
        ),
    )
    second = await messages.submit(task.id, "Validate and finish", "second-run")
    assert second.kind is MessageSubmissionKind.NEW_RUN
    async with factory() as uow:
        run = await uow.runs.get(second.run_id)
        assert run is not None
        await uow.runs.update(
            replace(
                run,
                settings_snapshot=RepositorySettings(
                    validation_commands=(validation_command(),),
                    validation_repair_limit=1,
                ),
            )
        )
        await uow.commit()
    validation_tool = ScriptedValidationTool(
        (ToolResultStatus.FAILED, ToolResultStatus.SUCCEEDED), factory
    )
    dispatcher = ToolDispatcher(ToolRegistry((validation_tool,)), factory, clock)
    assert await RunEngine(
        factory,
        clock,
        RepairGateway(("needs repair", "repaired completion")),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, clock, dispatcher),
        context_manager=context_manager,
    ).execute(second.run_id)

    async with factory() as uow:
        run = await uow.runs.get(second.run_id)
        assert run is not None
        attempts = await uow.validation_attempts.list_for_run(run.id)
        compaction = await uow.compactions.latest_for_task(task.id)
    assert [attempt.status.value for attempt in attempts] == ["failed", "passed"]
    assert compaction is not None

    (task.workspace_path / "README.md").write_text("# Completed milestone\n")
    assert git(root, "rev-parse", "HEAD") == original_head
    assert git(root, "status", "--porcelain=v2") == original_status
    result = await AcceptanceService(git_client, artifacts, factory, clock).accept(
        task.id, "accept-milestone"
    )
    assert git(root, "rev-parse", "HEAD") == original_head
    assert git(root, "status", "--porcelain=v2") == original_status

    target_ref = git(root, "symbolic-ref", "--short", "HEAD")
    dirty = root / "local-only.txt"
    dirty.write_text("do not overwrite\n")
    integration_service = IntegrationService(git_client, factory, clock)
    rejected = await integration_service.integrate(
        result.id,
        IntegrationTarget(registered.repository.id, target_ref, original_head),
        "dirty-integration",
    )
    assert rejected.status is IntegrationStatus.FAILED
    assert rejected.failure_code == "target_dirty"
    assert dirty.read_text() == "do not overwrite\n"
    assert git(root, "rev-parse", "HEAD") == original_head
    dirty.unlink()

    (root / "README.md").write_text("conflicting target\n")
    git(root, "add", "README.md")
    git(root, "commit", "-m", "conflicting target")
    conflict_head = git(root, "rev-parse", "HEAD")
    conflicted = await integration_service.integrate(
        result.id,
        IntegrationTarget(registered.repository.id, target_ref, conflict_head),
        "conflicting-integration",
    )
    assert conflicted.status is IntegrationStatus.CONFLICT
    assert conflicted.failure_code == "integration_conflict"
    assert git(root, "rev-parse", "HEAD") == conflict_head
    assert git(root, "status", "--porcelain=v2") == ""
    assert (root / "README.md").read_text() == "conflicting target\n"

    git(root, "revert", "--no-edit", "HEAD")
    clean_head = git(root, "rev-parse", "HEAD")

    integrated = await integration_service.integrate(
        result.id,
        IntegrationTarget(registered.repository.id, target_ref, clean_head),
        "clean-integration",
    )
    assert integrated.status is IntegrationStatus.COMPLETED
    assert git(root, "rev-parse", "HEAD") == integrated.observed_after_revision
    assert (root / "README.md").read_text() == "# Completed milestone\n"

    reloaded = TaskService(
        WorkspaceManager(git_client, tmp_path / "reloaded-data"), factory, clock
    )
    retained_task = await reloaded.get(task.id)
    retained = await reloaded.review(task.id)
    retained_messages = await messages.list(task.id)
    async with factory() as uow:
        compaction = await uow.compactions.latest_for_task(task.id)
        approvals = await uow.approvals.list_for_task(task.id)
    assert retained_task.status.value == "integrated"
    assert len(retained_messages) >= 6
    assert compaction is not None
    assert len(approvals) == 2
    assert [item.attempt.status.value for item in retained.validations] == [
        "not_configured",
        "failed",
        "passed",
    ]
    assert retained.result_revisions == (result,)
    assert retained.integrations[-1] == integrated
