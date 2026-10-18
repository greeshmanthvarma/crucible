"""One current-baseline task through ordinary services, then protected grading."""

import asyncio
import json
import os
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID

from crucible.application.container import ApplicationContainer
from crucible.application.message_service import SubmittedRun
from crucible.benchmarks.admission import validate_admission
from crucible.benchmarks.contracts import (
    BenchmarkCase,
    SolverResult,
    TrialIdentity,
    TrialLimits,
    TrialResult,
)
from crucible.benchmarks.manifests import _git, case_digest, export_solver_case
from crucible.benchmarks.patches import collect_solution_patch
from crucible.benchmarks.swebench import SwebenchGrader
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.clock import SystemClock
from crucible.domain.repository import RepositorySettings
from crucible.domain.run import Run
from crucible.engine.gateway import (
    ModelError,
    ModelGateway,
    ModelStreamItem,
    PreparedModelRequest,
)
from crucible.engine.run_engine import RunBudgetExceeded
from crucible.evals.runner import ApprovalPrompter
from crucible.repair.gateway import GatewayDenied, ScopedModelGateway, TrustedUsage
from crucible.repair.identity import canonical_json, capture_runtime_identity, digest
from crucible.sandbox.docker import DockerSandboxBackend


class BudgetedTrialGateway:
    """Use the existing trusted ledger for both ordinary and compaction requests."""

    def __init__(
        self, upstream: ModelGateway, model: str, limits: TrialLimits, trial_id: UUID
    ) -> None:
        self.scoped = ScopedModelGateway(
            upstream, clock=SystemClock(), endpoint="local"
        )
        self.trial_id = trial_id
        self.credential = self.scoped.issue(
            trial_id,
            model,
            {
                "max_requests": limits.max_steps * 3,
                "max_tokens": limits.max_model_tokens,
            },
            timedelta(seconds=limits.wall_timeout_seconds),
        )
        self.exhausted = False
        self.provider_failed = False

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        if self.provider_failed:
            raise RuntimeError("previous provider failure; explicit recovery required")
        try:
            reservation = await self.scoped.reserve(
                self.credential.token, self.trial_id, request
            )
        except GatewayDenied as error:
            if error.status_code == 429:
                self.exhausted = True
                raise RunBudgetExceeded("aggregate gateway budget exhausted") from error
            raise
        async for item in self.scoped.stream(reservation):
            if isinstance(item, ModelError):
                self.provider_failed = True
            yield item

    def usage(self) -> TrustedUsage:
        return self.scoped.usage(self.trial_id)


def solver_termination(
    status: str, code: str | None
) -> Literal[
    "completed",
    "budget_exhausted",
    "cancelled",
    "provider_error",
    "infrastructure_error",
]:
    if status == "completed":
        return "completed"
    if code == "budget_exhausted":
        return "budget_exhausted"
    if status in {"cancelled", "interrupted"}:
        return "cancelled"
    if code == "model_gateway_error" or (code and code.startswith("provider")):
        return "provider_error"
    return "infrastructure_error"


async def verify_harness(
    container: ApplicationContainer, identity: TrialIdentity
) -> None:
    root = Path(__file__).parents[4]
    current = await capture_runtime_identity(root)
    if (
        _git(root, "status", "--porcelain")
        or current != container.runtime_identity
        or current.commit != identity.harness_commit
        or current.source_digest != identity.harness_tree_digest
    ):
        raise ValueError("current harness identity is mutable or mismatched")


def _record(path: Path, value: object) -> None:
    content = canonical_json(value)
    for key, secret in os.environ.items():
        if secret and key.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD")):
            escaped = json.dumps(secret, ensure_ascii=False)[1:-1].encode()
            content = content.replace(escaped, b"[REDACTED]")
    path.write_bytes(content)
    path.chmod(0o600)


class BenchmarkTrialRunner:
    def __init__(
        self,
        container: ApplicationContainer,
        grader: SwebenchGrader,
        approval_prompter: ApprovalPrompter,
        data_root: Path,
        *,
        admission_path: Path,
        model_configuration: dict[str, object],
        stage_callback: Callable[[str], None] | None = None,
        gateway_accounting: BudgetedTrialGateway | None = None,
    ) -> None:
        self.container = container
        self.grader = grader
        self.approval_prompter = approval_prompter
        self.data_root = data_root
        self.admission_path = admission_path
        self.model_configuration = model_configuration
        self.stage_callback = stage_callback
        self.gateway_accounting = gateway_accounting

    async def run(
        self,
        case: BenchmarkCase,
        identity: TrialIdentity,
        limits: TrialLimits,
        output_dir: Path,
    ) -> TrialResult:
        container = self.container
        if (
            self.gateway_accounting is None
            or container.execution_gateway is not self.gateway_accounting
            or self.gateway_accounting.trial_id != identity.trial_id
        ):
            raise ValueError("aggregate trusted gateway accounting required")
        await verify_harness(container, identity)
        validate_admission(self.admission_path, case, self.grader.config)
        if (
            case_digest(case) != identity.case_digest
            or digest(limits) != identity.limits_digest
        ):
            raise ValueError("trial inputs differ from identity")
        if digest(self.model_configuration) != identity.model_config_digest:
            raise ValueError("model configuration identity mismatch")
        if (
            self.model_configuration.get("model_id") != container.model_id
            or self.model_configuration.get("provider_defaults") != "unspecified"
            or container.model_id == "fake"
            or container.execution_context_limits
            != (
                self.model_configuration.get("input_limit"),
                self.model_configuration.get("output_reserve"),
            )
        ):
            raise ValueError("explicit effective model settings required")
        budgets = container.execution_budgets
        if budgets is None or (
            budgets.max_steps,
            budgets.max_tool_calls,
            budgets.max_model_tokens,
            budgets.max_active_seconds,
        ) != (
            limits.max_steps,
            limits.max_tool_calls,
            limits.max_model_tokens,
            limits.max_active_seconds,
        ):
            raise ValueError("effective engine budgets differ from frozen limits")
        sandbox = container.command_sandbox
        if not isinstance(
            sandbox, DockerSandboxBackend
        ) or sandbox.repository_environment != (case.image, case.platform):
            raise ValueError("solver environment differs from admitted image/platform")
        if not output_dir.is_absolute() or any(
            p.is_symlink() for p in [output_dir, *output_dir.parents]
        ):
            raise ValueError("invalid controller output")
        for protected in (case.grader_input_path, self.admission_path):
            if protected.resolve().is_relative_to(
                (self.data_root / "workspaces").resolve()
            ):
                raise ValueError("protected input overlaps solver workspace root")
        output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        solver: SolverResult | None = None
        run_id: UUID | None = None
        task_id: UUID | None = None
        try:
            view = export_solver_case(case, output_dir / "source")
            source_revision = _git(view.workspace, "rev-parse", "HEAD").decode().strip()
            base_tree = (
                _git(view.workspace, "rev-parse", "HEAD^{tree}").decode().strip()
            )
            registered = await container.repository_service.register(view.workspace)
            context = container.execution_context_limits
            assert context is not None
            await container.repository_service.update_settings(
                registered.repository.id,
                RepositorySettings(
                    sandbox_image=case.image,
                    model_input_limit=context[0],
                    model_output_reserve=context[1],
                ),
            )
            task = await container.task_service.create(
                registered.repository.id,
                source_revision,
                f"benchmark-task-{identity.trial_id}",
            )
            task_id = task.id
            if view.prompt.strip().split(maxsplit=1)[:1] == ["/repair"]:
                raise ValueError("benchmark prompt cannot invoke repair")
            if self.stage_callback:
                self.stage_callback("solving")
            async with asyncio.timeout(limits.wall_timeout_seconds):
                submitted = await container.message_service.submit(
                    task.id, view.prompt, f"benchmark-message-{identity.trial_id}"
                )
                if not isinstance(submitted, SubmittedRun):
                    raise ValueError("benchmark prompt dispatched a non-solver action")
                run_id = submitted.run_id
                run = await self._await_terminal(run_id)
            await container.supervisor.cancel(run_id)
            await sandbox.stop_task_commands(task.id)
            if self.stage_callback:
                self.stage_callback("snapshotting")
            trace = await container.task_service.trace(task.id)
            async with container.unit_of_work() as uow:
                usage = await uow.evals.list_usage(run_id)
                resources = await uow.external_resources.list_for_task(task.id)
            if any(
                r.kind.value == "container" and r.status.value != "removed"
                for r in resources
            ):
                raise RuntimeError("solver processes not stopped")
            trace_path, usage_path = (
                output_dir / "trace.json",
                output_dir / "usage.json",
            )
            _record(trace_path, trace)
            _record(usage_path, self._usage_record(usage))
            termination = solver_termination(run.status.value, run.outcome_code)
            if self.gateway_accounting:
                if self.gateway_accounting.provider_failed:
                    termination = "provider_error"
                elif self.gateway_accounting.exhausted:
                    termination = "budget_exhausted"
            solver = SolverResult(
                termination,
                task.id,
                run_id,
                task.workspace_path,
                trace_path,
                usage_path,
            )
            if termination == "cancelled":
                return TrialResult(
                    identity, solver, None, None, "interrupted", "solver_cancelled"
                )
            if termination not in {"completed", "budget_exhausted"}:
                return TrialResult(identity, solver, None, None, "error", termination)
            await verify_harness(container, identity)
            validate_admission(self.admission_path, case, self.grader.config)
            patch = collect_solution_patch(
                task.workspace_path,
                base_tree,
                output_dir / "solution.diff",
                base_commit=case.base_commit,
                base_repository=view.workspace,
            )
            if self.stage_callback:
                self.stage_callback("grading")
            grade = await self.grader.grade(
                case, patch, identity.trial_id, output_dir / "grade"
            )
            return TrialResult(
                identity,
                solver,
                patch,
                grade,
                "error" if grade.verdict == "error" else "complete",
                grade.code if grade.verdict == "error" else None,
            )
        except asyncio.CancelledError:
            if run_id:
                await container.supervisor.cancel(run_id)
            if task_id:
                await sandbox.stop_task_commands(task_id)
            if task_id and run_id:
                solver = await self._capture_solver(
                    task_id, run_id, "cancelled", output_dir
                )
                _record(output_dir / "solver-interrupted.json", solver)
            _record(
                output_dir / "interrupted.json",
                {"trial_id": identity.trial_id, "status": "interrupted"},
            )
            raise
        except Exception as error:
            failure_code = (
                "solver_wall_timeout"
                if isinstance(error, TimeoutError)
                else type(error).__name__
            )
            if run_id:
                await container.supervisor.cancel(run_id)
            if task_id:
                try:
                    await sandbox.stop_task_commands(task_id)
                except Exception:
                    failure_code = "solver_cleanup_failed"
            if task_id and run_id:
                solver = await self._capture_solver(
                    task_id, run_id, "infrastructure_error", output_dir
                )
            return TrialResult(
                identity,
                solver,
                None,
                None,
                "error",
                failure_code,
            )

    def _usage_record(self, steps: object) -> object:
        return {
            "steps": steps,
            "gateway": self.gateway_accounting.usage()
            if self.gateway_accounting
            else None,
            "gateway_unavailable_reason": None
            if self.gateway_accounting
            else "test or unwrapped container",
        }

    async def _capture_solver(
        self,
        task_id: UUID,
        run_id: UUID,
        termination: Literal["cancelled", "infrastructure_error"],
        root: Path,
    ) -> SolverResult:
        trace = await self.container.task_service.trace(task_id)
        async with self.container.unit_of_work() as uow:
            task = await uow.tasks.get(task_id)
            usage = await uow.evals.list_usage(run_id)
        if task is None:
            raise RuntimeError("solver task disappeared")
        _record(root / "trace.json", trace)
        _record(root / "usage.json", self._usage_record(usage))
        return SolverResult(
            termination,
            task_id,
            run_id,
            task.workspace_path,
            root / "trace.json",
            root / "usage.json",
        )

    async def _await_terminal(self, run_id: UUID) -> Run:
        while True:
            async with self.container.unit_of_work() as uow:
                run = await uow.runs.get(run_id)
                pending = await uow.approvals.list_pending_for_run(run_id)
            if run is None:
                raise RuntimeError("solver Run disappeared")
            if run.status.value in {"completed", "failed", "cancelled", "interrupted"}:
                return run
            for approval in pending:
                choice = await self.approval_prompter(approval)
                if (
                    choice.decision
                    not in (ApprovalStatus.APPROVED, ApprovalStatus.DENIED)
                    or not choice.decision_key
                ):
                    raise ValueError("ordinary approval decision required")
                await self.container.approval_service.decide(
                    approval.id,
                    choice.decision,
                    approval.spec_digest,
                    choice.reason,
                    choice.decision_key,
                )
            await asyncio.sleep(0.05)
