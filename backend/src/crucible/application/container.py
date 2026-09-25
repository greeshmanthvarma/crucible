import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from crucible.application.acceptance_service import AcceptanceService
from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.command_authority import CommandAuthority
from crucible.application.event_service import TaskEventSource
from crucible.application.integration_service import IntegrationService
from crucible.application.message_service import MessageService
from crucible.application.ports import UnitOfWork
from crucible.application.reconciliation import StartupReconciler
from crucible.application.repository_service import RepositoryService
from crucible.application.run_service import RunService
from crucible.application.sandbox_reconciliation import SandboxReconciler
from crucible.application.task_service import TaskService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.compaction import CompactionLifecycle
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.clock import SystemClock
from crucible.domain.evals import EvalBudgets
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.compaction_gateway import ModelCompactionGateway
from crucible.engine.fake_gateway import FakeCompactionGateway, FakeModelGateway
from crucible.engine.gateway import ModelGateway
from crucible.engine.journal import RunJournal
from crucible.engine.notifier import TaskEventNotifier
from crucible.engine.run_engine import RunEngine
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.models.litellm_gateway import LiteLLMModelGateway
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.sandbox.docker import DockerSandboxBackend
from crucible.sandbox.docker_client import DockerClient, SubprocessDockerClient
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.command import ExecuteCommandTool
from crucible.tools.definitions import Tool
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import default_registry
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


@dataclass(frozen=True)
class ApplicationContainer:
    model_id: str
    database: Database
    repository_service: RepositoryService
    task_service: TaskService
    message_service: MessageService
    supervisor: LocalRunSupervisor
    event_source: TaskEventSource
    approval_service: ApprovalService
    artifact_service: ArtifactService
    run_service: RunService
    acceptance_service: AcceptanceService
    integration_service: IntegrationService
    sandbox_reconciler: SandboxReconciler
    reconciler: StartupReconciler
    unit_of_work: Callable[[], UnitOfWork]
    eval_sandbox: DockerSandboxBackend

    @classmethod
    async def create(
        cls,
        database_url: str,
        data_dir: Path,
        *,
        gateway_factory: Callable[[], ModelGateway] | None = None,
        model_id: str | None = None,
        docker_client: DockerClient | None = None,
        run_budgets: EvalBudgets | None = None,
        context_limits: tuple[int, int] | None = None,
    ) -> "ApplicationContainer":
        database = await Database.create(database_url)
        await _verify_current_revision(database)

        def unit_of_work() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(database)

        clock = SystemClock()
        git = SubprocessGitClient()
        notifier = TaskEventNotifier()
        approval_broker = InMemoryApprovalBroker()
        journal = RunJournal(unit_of_work, clock, notifier)
        workspaces = WorkspaceManager(git, data_dir)
        docker = docker_client or SubprocessDockerClient()
        resource_manager = TaskResourceManager(docker, unit_of_work, clock)
        approval_service = ApprovalService(
            unit_of_work, clock, approval_broker, notifier
        )
        artifact_service = ArtifactService(
            LocalArtifactStore(data_dir / "artifacts", clock), unit_of_work
        )
        sandbox_backend = DockerSandboxBackend(docker, unit_of_work, clock)
        command_tool = ExecuteCommandTool(
            CommandAuthority(approval_service, approval_broker, clock),
            sandbox_backend,
            resource_manager,
            artifact_service,
            journal=journal,
        )
        model = model_id or os.environ.get("CRUCIBLE_MODEL", "fake")
        gateway: ModelGateway = (
            gateway_factory()
            if gateway_factory is not None
            else LiteLLMResponsesGateway()
            if model.startswith("openai/")
            else LiteLLMModelGateway()
            if model != "fake"
            else FakeModelGateway()
        )
        compaction_gateway = (
            FakeCompactionGateway()
            if model == "fake"
            else ModelCompactionGateway(gateway)
        )
        registry = default_registry(cast(Tool, command_tool))
        context_manager = ContextManager(
            unit_of_work,
            clock,
            SimpleTokenEstimator(),
            harness_policy="Follow harness safety and execution policy.",
            tool_contract="Use only the structured tools supplied by the harness.",
            model=model,
            input_limit=(
                context_limits[0]
                if context_limits
                else int(os.environ.get("CRUCIBLE_MODEL_INPUT_LIMIT", "100000"))
            ),
            output_reserve=(
                context_limits[1]
                if context_limits
                else int(os.environ.get("CRUCIBLE_MODEL_OUTPUT_RESERVE", "4096"))
            ),
            tools=registry.definitions,
            journal=journal,
            compaction_lifecycle=CompactionLifecycle(
                unit_of_work,
                artifact_service,
                compaction_gateway,
                clock,
                model=model,
            ),
        )
        engine = RunEngine(
            unit_of_work,
            clock,
            gateway,
            notifier,
            journal=journal,
            context_manager=context_manager,
            dispatcher=ToolDispatcher(registry, unit_of_work, clock, journal=journal),
            max_steps=run_budgets.max_steps
            if run_budgets
            else int(os.environ.get("CRUCIBLE_MAX_STEPS", "20")),
            max_tool_calls=run_budgets.max_tool_calls
            if run_budgets
            else int(os.environ.get("CRUCIBLE_MAX_TOOL_CALLS", "100")),
            max_model_tokens=run_budgets.max_model_tokens
            if run_budgets
            else int(os.environ.get("CRUCIBLE_MAX_MODEL_TOKENS", "200000")),
            max_active_seconds=float(
                run_budgets.max_active_seconds
                if run_budgets
                else os.environ.get("CRUCIBLE_MAX_ACTIVE_SECONDS", "600")
            ),
        )
        supervisor = LocalRunSupervisor(
            engine,
            unit_of_work,
            clock,
            notifier,
            journal=journal,
            approval_broker=approval_broker,
        )
        run_service = RunService(unit_of_work, clock, supervisor, notifier)
        acceptance_service = AcceptanceService(
            git, artifact_service, unit_of_work, clock, notifier
        )
        integration_service = IntegrationService(git, unit_of_work, clock, notifier)
        sandbox_reconciler = SandboxReconciler(
            sandbox_backend,
            resource_manager,
            unit_of_work,
            clock,
            approval_broker,
        )
        return cls(
            model_id=model,
            database=database,
            repository_service=RepositoryService(git, unit_of_work, clock),
            task_service=TaskService(
                workspaces, unit_of_work, clock, notifier, resource_manager
            ),
            message_service=MessageService(unit_of_work, clock, supervisor, notifier),
            supervisor=supervisor,
            event_source=TaskEventSource(unit_of_work, notifier),
            approval_service=approval_service,
            artifact_service=artifact_service,
            run_service=run_service,
            acceptance_service=acceptance_service,
            integration_service=integration_service,
            sandbox_reconciler=sandbox_reconciler,
            reconciler=StartupReconciler(
                workspaces, unit_of_work, clock, notifier, resource_manager
            ),
            unit_of_work=unit_of_work,
            eval_sandbox=sandbox_backend,
        )

    async def start(self) -> None:
        await self.sandbox_reconciler.reconcile()
        await self.reconciler.reconcile()
        await self.supervisor.reconcile()

    async def close(self) -> None:
        await self.supervisor.close()
        await self.database.dispose()


async def _verify_current_revision(database: Database) -> None:
    config_path = Path(__file__).parents[3] / "alembic.ini"
    expected = ScriptDirectory.from_config(Config(config_path)).get_current_head()
    async with database.engine.connect() as connection:
        actual = await connection.scalar(
            text("SELECT version_num FROM alembic_version")
        )
    if actual != expected:
        await database.dispose()
        raise RuntimeError(
            f"Database schema is not current: expected {expected}, found {actual}"
        )
