import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import TypeAdapter
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
from crucible.application.repair_service import RepairService
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
from crucible.engine.gateway import ModelGateway, ModelMessage
from crucible.engine.journal import RunJournal
from crucible.engine.notifier import TaskEventNotifier
from crucible.engine.run_engine import RunEngine
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.repair.admission import (
    LocalRepairAdmissionGit,
    LocalRepairAdmissionGitHub,
    LocalRepairWorkerRecovery,
)
from crucible.repair.controller import RepairController
from crucible.repair.evidence import EvidenceRecorder
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.identity import RuntimeIdentity, capture_runtime_identity, digest
from crucible.repair.incident_context import IncidentContext
from crucible.repair.recovery import ManagedRepairWorkspace
from crucible.repair.roles import RoleExecutor
from crucible.repair.worker_protocol import WorkerJob
from crucible.repair.worker_supervisor import WorkerSupervisor
from crucible.sandbox.docker import DockerSandboxBackend
from crucible.sandbox.docker_client import DockerClient, SubprocessDockerClient
from crucible.sandbox.protocol import SandboxBackend
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
    repair_controller: RepairController | None
    repair_service: RepairService
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
    runtime_identity: RuntimeIdentity
    evidence: EvidenceRecorder
    repair_gateway: ScopedModelGateway
    context_manager: ContextManager
    tool_dispatcher: ToolDispatcher
    command_sandbox: SandboxBackend

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
        worker_job: WorkerJob | None = None,
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
        if worker_job is None:
            runtime_identity = await capture_runtime_identity(Path(__file__).parents[4])
        else:
            # Worker exports intentionally have no source .git metadata.
            original = WorkerJob.model_validate_json(
                Path("/job/request.json").read_bytes()
            )
            if original != worker_job or data_dir.resolve() != Path("/state"):
                raise ValueError(
                    "Worker mode requires the controller-mounted job and private state"
                )
            runtime_identity = RuntimeIdentity(
                "worker-export", worker_job.source_digest, "image-pinned"
            )
        secret_values = tuple(
            value
            for name, value in os.environ.items()
            if value and name.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD"))
        )
        evidence = EvidenceRecorder(
            unit_of_work,
            artifact_service,
            clock,
            secret_values=secret_values,
            runtime_identity=runtime_identity,
        )
        sandbox_backend = DockerSandboxBackend(docker, unit_of_work, clock)
        command_resources = resource_manager
        command_sandbox: SandboxBackend = sandbox_backend
        command_authority = CommandAuthority(
            approval_service,
            approval_broker,
            clock,
            unit_of_work,
            lambda: repair_service.execution_id,
        )
        if worker_job is not None:
            from crucible.repair.incontainer_commands import (
                InContainerSandbox,
                WorkerCommandAuthority,
                WorkerResourceManager,
            )

            command_sandbox = InContainerSandbox(
                Path("/workspace"), os.environ["CRUCIBLE_REPAIR_IMAGE"]
            )
            command_resources = cast(
                TaskResourceManager, WorkerResourceManager(worker_job.job_id)
            )
            command_authority = cast(
                CommandAuthority,
                WorkerCommandAuthority(approval_service, clock, worker_job.job_id),
            )
        command_tool = ExecuteCommandTool(
            command_authority,
            command_sandbox,
            command_resources,
            artifact_service,
            journal=journal,
        )
        model = model_id or os.environ.get("CRUCIBLE_MODEL", "fake")
        gateway: ModelGateway = (
            gateway_factory()
            if gateway_factory is not None
            else LiteLLMResponsesGateway()
            if model != "fake"
            else FakeModelGateway()
        )
        compaction_gateway = (
            FakeCompactionGateway()
            if model == "fake"
            else ModelCompactionGateway(gateway, evidence=evidence)
        )
        registry = default_registry(cast(Tool, command_tool))
        threshold = worker_job.settings.get("threshold", 0.8) if worker_job else 0.8
        if (
            not isinstance(threshold, (float, int))
            or isinstance(threshold, bool)
            or not 0 < threshold <= 1
        ):
            raise ValueError("Invalid controlled context threshold")
        context_manager = ContextManager(
            unit_of_work,
            clock,
            SimpleTokenEstimator(),
            harness_policy=str(
                worker_job.settings.get(
                    "harness_policy", "Follow harness safety and execution policy."
                )
            )
            if worker_job
            else "Follow harness safety and execution policy.",
            tool_contract=str(
                worker_job.settings.get(
                    "tool_contract",
                    "Use only the structured tools supplied by the harness.",
                )
            )
            if worker_job
            else "Use only the structured tools supplied by the harness.",
            threshold=float(threshold),
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
            evidence=evidence,
            replay_instructions=TypeAdapter(tuple[str, ...]).validate_python(
                worker_job.settings["required_instructions"]
            )
            if worker_job and "required_instructions" in worker_job.settings
            else None,
            replay_repair_context=TypeAdapter(tuple[ModelMessage, ...]).validate_python(
                worker_job.settings["repair_context"]
            )
            if worker_job and "repair_context" in worker_job.settings
            else None,
            recent_complete_units=TypeAdapter(int).validate_python(
                worker_job.settings.get("recent_complete_units", 2)
            )
            if worker_job
            else 2,
            compaction_lifecycle=CompactionLifecycle(
                unit_of_work,
                artifact_service,
                compaction_gateway,
                clock,
                model=model,
            ),
        )
        dispatcher = ToolDispatcher(
            registry, unit_of_work, clock, journal=journal, evidence=evidence
        )
        engine = RunEngine(
            unit_of_work,
            clock,
            gateway,
            notifier,
            journal=journal,
            context_manager=context_manager,
            dispatcher=dispatcher,
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
        repair_service = RepairService(
            unit_of_work,
            clock,
            LocalRepairAdmissionGit(Path(__file__).parents[4], data_dir),
            LocalRepairAdmissionGitHub(Path(__file__).parents[4]),
            runtime_identity,
            notifier=notifier,
            worker_recovery=LocalRepairWorkerRecovery(Path(__file__).parents[4]),
        )
        scoped_gateway = ScopedModelGateway(
            gateway,
            clock=clock,
            endpoint=os.environ.get(
                "CRUCIBLE_REPAIR_GATEWAY_ENDPOINT",
                "http://host.docker.internal:8000/internal/repair-model/responses",
            ),
        )
        incident = IncidentContext(unit_of_work, artifact_service)

        def role_engine(role_registry: object) -> RunEngine:
            from crucible.tools.registry import ToolRegistry

            assert isinstance(role_registry, ToolRegistry)
            role_context = ContextManager(
                unit_of_work,
                clock,
                SimpleTokenEstimator(),
                harness_policy="Follow harness safety and execution policy.",
                tool_contract="Use only the structured tools supplied by the harness.",
                model=model,
                input_limit=context_limits[0]
                if context_limits
                else int(os.environ.get("CRUCIBLE_MODEL_INPUT_LIMIT", "100000")),
                output_reserve=context_limits[1]
                if context_limits
                else int(os.environ.get("CRUCIBLE_MODEL_OUTPUT_RESERVE", "4096")),
                tools=role_registry.definitions,
                journal=journal,
                evidence=evidence,
                compaction_lifecycle=CompactionLifecycle(
                    unit_of_work,
                    artifact_service,
                    compaction_gateway,
                    clock,
                    model=model,
                ),
            )
            return RunEngine(
                unit_of_work,
                clock,
                gateway,
                notifier,
                journal=journal,
                context_manager=role_context,
                dispatcher=ToolDispatcher(
                    role_registry,
                    unit_of_work,
                    clock,
                    journal=journal,
                    evidence=evidence,
                ),
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

        roles = RoleExecutor(
            unit_of_work,
            clock,
            artifact_service,
            role_engine,
            incident,
            command_tool=cast(Tool, command_tool),
            resources=resource_manager,
            notifier=notifier,
            configuration={
                "model": model,
                "max_steps": run_budgets.max_steps
                if run_budgets
                else int(os.environ.get("CRUCIBLE_MAX_STEPS", "20")),
                "max_tool_calls": run_budgets.max_tool_calls
                if run_budgets
                else int(os.environ.get("CRUCIBLE_MAX_TOOL_CALLS", "100")),
                "max_model_tokens": run_budgets.max_model_tokens
                if run_budgets
                else int(os.environ.get("CRUCIBLE_MAX_MODEL_TOKENS", "200000")),
                "max_active_seconds": run_budgets.max_active_seconds
                if run_budgets
                else float(os.environ.get("CRUCIBLE_MAX_ACTIVE_SECONDS", "600")),
                "context_limits": context_limits
                or (
                    int(os.environ.get("CRUCIBLE_MODEL_INPUT_LIMIT", "100000")),
                    int(os.environ.get("CRUCIBLE_MODEL_OUTPUT_RESERVE", "4096")),
                ),
            },
        )
        repair_controller: RepairController | None = None
        if worker_job is None:
            # Private evaluator libraries never enter candidate workers. Import
            # and compose the control-plane evaluator only on the host.
            from crucible.evals.evaluators import EvaluatorRegistry
            from crucible.evals.fixtures import FixturePreparer
            from crucible.repair.evaluation import RepairEvaluation
            from crucible.repair.git import LocalRepairGit, RepairAcceptanceService
            from crucible.repair.github import LocalRepairGitHub
            from crucible.repair.incident_suite import IncidentSuite
            from crucible.repair.publication import PublicationService
            from crucible.repair.reporting import RepairReporter

            repair_image = os.environ.get("CRUCIBLE_REPAIR_IMAGE", "")
            evaluation = RepairEvaluation(
                unit_of_work,
                clock,
                artifact_service,
                main_root=Path(__file__).parents[4],
                data_dir=data_dir,
                runtime_identity=runtime_identity,
                model=model,
                worker_factory=lambda: WorkerSupervisor(
                    docker,
                    image=repair_image,
                    root=data_dir / "repair-workers",
                    gateway=scoped_gateway,
                ),
                gateway=scoped_gateway,
                fixtures=FixturePreparer(data_dir),
                evaluators=lambda partition: EvaluatorRegistry(
                    data_dir=data_dir,
                    artifacts=artifact_service,
                    sandbox=sandbox_backend,
                    partition_root=Path(__file__).parents[4]
                    / "evals"
                    / partition.value,
                ),
                image=repair_image,
                instruction_digest=digest(
                    "Follow harness safety and execution policy."
                ),
                tool_schema_digest=digest(registry.definitions),
                prompt_digest=digest("compaction-v1"),
                budgets=run_budgets
                or EvalBudgets(
                    max_steps=int(os.environ.get("CRUCIBLE_MAX_STEPS", "20")),
                    max_tool_calls=int(
                        os.environ.get("CRUCIBLE_MAX_TOOL_CALLS", "100")
                    ),
                    max_model_tokens=int(
                        os.environ.get("CRUCIBLE_MAX_MODEL_TOKENS", "200000")
                    ),
                    max_active_seconds=int(
                        os.environ.get("CRUCIBLE_MAX_ACTIVE_SECONDS", "600")
                    ),
                ),
            )
            repair_controller = RepairController(
                unit_of_work,
                clock,
                roles,
                incident,
                artifact_service,
                evaluation=evaluation,
                acceptance=RepairAcceptanceService(
                    unit_of_work,
                    LocalRepairGit(Path(__file__).parents[4], data_dir),
                    artifact_service,
                    clock,
                    IncidentSuite(
                        data_dir / "repair-incidents",
                        unit_of_work,
                        artifact_service,
                        clock,
                        fixtures={},
                    ),
                ),
                publication=PublicationService(
                    unit_of_work,
                    clock,
                    LocalRepairGitHub(Path(__file__).parents[4]),
                    RepairReporter(unit_of_work, artifact_service, clock),
                ),
                recovery=ManagedRepairWorkspace(
                    unit_of_work,
                    artifact_service,
                    main_root=Path(__file__).parents[4],
                    runtime_identity=runtime_identity,
                ),
                notifier=notifier,
                execution_id=repair_service.execution_id,
            )
            repair_service.bind_scheduler(repair_controller)
        return cls(
            repair_controller=repair_controller,
            repair_service=repair_service,
            model_id=model,
            database=database,
            repository_service=RepositoryService(git, unit_of_work, clock),
            task_service=TaskService(
                workspaces, unit_of_work, clock, notifier, resource_manager
            ),
            message_service=MessageService(
                unit_of_work,
                clock,
                supervisor,
                notifier,
                git,
                workspaces,
                repair_service,
            ),
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
            runtime_identity=runtime_identity,
            evidence=evidence,
            repair_gateway=scoped_gateway,
            context_manager=context_manager,
            tool_dispatcher=dispatcher,
            command_sandbox=command_sandbox,
        )

    async def start(self) -> None:
        await self.sandbox_reconciler.reconcile()
        await self.reconciler.reconcile()
        await self.repair_service.start()
        await self.supervisor.reconcile()

    async def close(self) -> None:
        await self.repair_service.close()
        await self.supervisor.close()
        await self.database.dispose()


async def _verify_current_revision(database: Database) -> None:
    config_path = Path(__file__).parents[3] / "alembic.ini"
    config = Config(config_path)
    config.set_main_option("script_location", str(config_path.parent / "migrations"))
    expected = ScriptDirectory.from_config(config).get_current_head()
    async with database.engine.connect() as connection:
        actual = await connection.scalar(
            text("SELECT version_num FROM alembic_version")
        )
    if actual != expected:
        await database.dispose()
        raise RuntimeError(
            f"Database schema is not current: expected {expected}, found {actual}"
        )
