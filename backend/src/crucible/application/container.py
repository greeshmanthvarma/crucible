import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.event_service import TaskEventSource
from crucible.application.message_service import MessageService
from crucible.application.ports import UnitOfWork
from crucible.application.reconciliation import StartupReconciler
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.clock import SystemClock
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.journal import RunJournal
from crucible.engine.notifier import TaskEventNotifier
from crucible.engine.run_engine import RunEngine
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.models.litellm_gateway import LiteLLMModelGateway
from crucible.sandbox.docker_client import SubprocessDockerClient
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import default_registry
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


@dataclass(frozen=True)
class ApplicationContainer:
    database: Database
    repository_service: RepositoryService
    task_service: TaskService
    message_service: MessageService
    supervisor: LocalRunSupervisor
    event_source: TaskEventSource
    approval_service: ApprovalService
    artifact_service: ArtifactService
    reconciler: StartupReconciler
    unit_of_work: Callable[[], UnitOfWork]

    @classmethod
    async def create(cls, database_url: str, data_dir: Path) -> "ApplicationContainer":
        database = await Database.create(database_url)
        await _verify_current_revision(database)

        def unit_of_work() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(database)

        clock = SystemClock()
        git = SubprocessGitClient()
        notifier = TaskEventNotifier()
        approval_broker = InMemoryApprovalBroker()
        journal = RunJournal(unit_of_work, clock, notifier)
        model = os.environ.get("CRUCIBLE_MODEL", "fake")
        gateway = LiteLLMModelGateway() if model != "fake" else FakeModelGateway()
        registry = default_registry()
        context_manager = ContextManager(
            unit_of_work,
            clock,
            SimpleTokenEstimator(),
            harness_policy="Follow harness safety and execution policy.",
            tool_contract="Use only the structured tools supplied by the harness.",
            model=model,
            input_limit=int(os.environ.get("CRUCIBLE_MODEL_INPUT_LIMIT", "100000")),
            output_reserve=int(os.environ.get("CRUCIBLE_MODEL_OUTPUT_RESERVE", "4096")),
            tools=registry.definitions,
            journal=journal,
        )
        engine = RunEngine(
            unit_of_work,
            clock,
            gateway,
            notifier,
            journal=journal,
            context_manager=context_manager,
            dispatcher=ToolDispatcher(registry, unit_of_work, clock, journal=journal),
            max_steps=int(os.environ.get("CRUCIBLE_MAX_STEPS", "20")),
            max_tool_calls=int(os.environ.get("CRUCIBLE_MAX_TOOL_CALLS", "100")),
            max_model_tokens=int(os.environ.get("CRUCIBLE_MAX_MODEL_TOKENS", "200000")),
            max_active_seconds=float(
                os.environ.get("CRUCIBLE_MAX_ACTIVE_SECONDS", "600")
            ),
        )
        supervisor = LocalRunSupervisor(
            engine, unit_of_work, clock, notifier, journal=journal
        )
        workspaces = WorkspaceManager(git, data_dir)
        resource_manager = TaskResourceManager(
            SubprocessDockerClient(), unit_of_work, clock
        )

        return cls(
            database=database,
            repository_service=RepositoryService(git, unit_of_work, clock),
            task_service=TaskService(
                workspaces, unit_of_work, clock, notifier, resource_manager
            ),
            message_service=MessageService(unit_of_work, clock, supervisor, notifier),
            supervisor=supervisor,
            event_source=TaskEventSource(unit_of_work, notifier),
            approval_service=ApprovalService(
                unit_of_work, clock, approval_broker, notifier
            ),
            artifact_service=ArtifactService(
                LocalArtifactStore(data_dir / "artifacts", clock), unit_of_work
            ),
            reconciler=StartupReconciler(
                workspaces, unit_of_work, clock, notifier, resource_manager
            ),
            unit_of_work=unit_of_work,
        )

    async def start(self) -> None:
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
