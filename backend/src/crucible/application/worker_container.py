"""Minimal worker-local assembly of ordinary RunEngine and tool services."""

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, cast

from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.command_authority import CommandAuthority
from crucible.application.ports import UnitOfWork
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.compaction import CompactionLifecycle
from crucible.context.manager import ContextManager
from crucible.domain.clock import SystemClock
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.compaction_gateway import ModelCompactionGateway
from crucible.engine.gateway import ModelGateway
from crucible.engine.journal import RunJournal
from crucible.engine.run_engine import RunEngine
from crucible.improvement.proposal_context import (
    ProposalStepPolicy,
    prune_proposal_tool_results,
)
from crucible.improvement.worker_commands import (
    WorkerAuthority,
    WorkerCommands,
    WorkerResources,
)
from crucible.improvement.worker_protocol import RoleJob, WorkerRequest
from crucible.models.token_counting import LocalTokenEstimator
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.command import ExecuteCommandTool
from crucible.tools.definitions import Tool
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import default_registry

if TYPE_CHECKING:
    from crucible.improvement.mining_contracts import InspectionReceipt


def create_worker_engine(
    database: Database,
    job: WorkerRequest,
    gateway: ModelGateway,
    token: str,
    *,
    reduced_model_budget: int | None = None,
    state_root: Path = Path("/state"),
    job_path: Path = Path("/job/request.json"),
    inspection_receipts: list["InspectionReceipt"] | None = None,
) -> tuple[RunEngine, Callable[[], UnitOfWork]]:
    # This function is reached only inside the worker. No host identity relaxation.
    if job_path.read_bytes() != job.model_dump_json().encode():
        raise ValueError("worker requires immutable mounted job")

    def uow() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(database)

    clock = SystemClock()
    journal = RunJournal(uow, clock)
    approval = ApprovalService(uow, clock, InMemoryApprovalBroker())
    artifacts = ArtifactService(
        LocalArtifactStore(state_root / "artifacts", clock), uow
    )
    command = ExecuteCommandTool(
        cast(CommandAuthority, WorkerAuthority(approval, clock, job.job_id)),
        WorkerCommands(job.job_id, token),
        cast(TaskResourceManager, WorkerResources(job.job_id)),
        artifacts,
        journal=journal,
    )
    harness_policy = "Follow harness safety and execution policy."
    if isinstance(job, RoleJob):
        from crucible.improvement.role_prompts import role_system_prompt
        from crucible.improvement.role_worker import role_registry

        if job.budget_slot_id is not None:
            from crucible.improvement.mining_inspection import audited_role_registry

            if inspection_receipts is None:
                raise ValueError("diagnostic engine requires a protected receipt sink")
            registry = audited_role_registry(
                job.job_kind, inspection_receipts, job.job_id
            )
        elif job.job_kind in ("mining", "format_correction", "proposal"):
            registry = role_registry(job.job_kind, cast(Tool, command))
        else:
            raise ValueError("diagnostic role requires registered slot identity")
        harness_policy = role_system_prompt(
            job.job_kind, correction_target=job.correction_target
        )
    else:
        registry = default_registry(cast(Tool, command))
    is_proposer = isinstance(job, RoleJob) and job.job_kind == "proposal"
    context = ContextManager(
        uow,
        clock,
        LocalTokenEstimator(tools=registry.definitions),
        harness_policy=harness_policy,
        tool_contract="Use only the structured tools supplied by the harness.",
        model=job.model.model_id,
        input_limit=job.model.input_limit,
        output_reserve=job.model.output_reserve,
        tools=registry.definitions,
        journal=journal,
        project_tool_results=prune_proposal_tool_results if is_proposer else None,
        compaction_lifecycle=CompactionLifecycle(
            uow,
            artifacts,
            ModelCompactionGateway(gateway, max_output_tokens=job.model.output_reserve),
            clock,
            model=job.model.model_id,
        ),
    )
    if (
        reduced_model_budget is not None
        and not 0 < reduced_model_budget <= job.limits.max_model_tokens
    ):
        raise ValueError("deterministic probe may only reduce model budget")
    model_budget = reduced_model_budget or job.limits.max_model_tokens
    engine = RunEngine(
        uow,
        clock,
        gateway,
        journal=journal,
        context_manager=context,
        dispatcher=ToolDispatcher(registry, uow, clock, journal=journal),
        max_steps=job.limits.max_steps,
        max_tool_calls=job.limits.max_tool_calls,
        max_model_tokens=model_budget,
        max_active_seconds=job.limits.max_active_seconds,
        step_policy_factory=(
            (lambda: ProposalStepPolicy(model_budget, job.model.input_limit))
            if is_proposer
            else None
        ),
    )
    return engine, uow
