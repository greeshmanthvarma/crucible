"""Explicit Trial orchestration through ordinary Crucible services."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from uuid import UUID

from crucible.application.container import ApplicationContainer
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.evals import (
    EvalCase,
    EvalResult,
    EvalSuite,
    EvalTrial,
    ResultVerdict,
)
from crucible.domain.ids import new_id
from crucible.domain.run import Run, RunStatus
from crucible.evals.evaluators import EvaluationContext, EvaluatorRegistry
from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import (
    EvalCaseDefinition,
    EvalPartition,
    EvalSuiteDefinition,
)
from crucible.evals.reporting import EvalReporter


@dataclass(frozen=True)
class TrialRequest:
    suite_id: str
    case_id: str
    repeat_index: int
    partition: EvalPartition
    configuration_digest: str
    invocation_id: UUID


@dataclass(frozen=True)
class ApprovalChoice:
    decision: ApprovalStatus
    reason: str | None
    decision_key: str


ApprovalPrompter = Callable[[Approval], Awaitable[ApprovalChoice]]


def configuration_snapshot(
    case: EvalCaseDefinition, gateway_version: str = "default"
) -> dict[str, object]:
    settings = case.repository_settings
    return {
        "schema_version": 1,
        "model": case.model,
        "gateway_version": gateway_version,
        "settings": {
            "validation_commands": [
                item.as_dict() for item in settings.validation_commands
            ],
            "sandbox_image": settings.sandbox_image,
            "sandbox_network": settings.sandbox_network,
            "validation_repair_limit": settings.validation_repair_limit,
            "default_cwd": settings.default_cwd,
            "compaction_threshold": settings.compaction_threshold,
            "compaction_model": settings.compaction_model,
            "compaction_prompt_version": settings.compaction_prompt_version,
            "compaction_attempt_limit": settings.compaction_attempt_limit,
            "model_input_limit": settings.model_input_limit,
            "model_output_reserve": settings.model_output_reserve,
        },
    }


def configuration_digest(
    case: EvalCaseDefinition, gateway_version: str = "default"
) -> str:
    value = configuration_snapshot(case, gateway_version)
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class EvalRunner:
    def __init__(
        self,
        container: ApplicationContainer,
        suite: EvalSuiteDefinition,
        fixtures: FixturePreparer,
        evaluators: EvaluatorRegistry,
        approval_prompter: ApprovalPrompter | None = None,
        reporter: EvalReporter | None = None,
        gateway_version: str = "default",
    ) -> None:
        self._container = container
        self._suite = suite
        self._fixtures = fixtures
        self._evaluators = evaluators
        self._approval_prompter = approval_prompter
        self._reporter = reporter or EvalReporter(
            container.unit_of_work, container.artifact_service
        )
        self._gateway_version = gateway_version
        models = {case.model for case in suite.cases}
        if len(models) != 1 or models != {container.model_id}:
            raise ValueError("selected Suite requires one matching effective model")

    async def run_trial(self, request: TrialRequest) -> EvalResult:
        if (
            request.suite_id != self._suite.suite_id
            or request.partition is not self._suite.partition
        ):
            raise ValueError("Trial request does not match selected Suite")
        case = next(
            (item for item in self._suite.cases if item.case_id == request.case_id),
            None,
        )
        if case is None:
            raise ValueError(f"Case is not in Suite: {request.case_id}")
        if request.configuration_digest != configuration_digest(
            case, self._gateway_version
        ):
            raise ValueError("Trial configuration digest does not match effective Case")
        suite_id, case_id = await self._ensure_definitions(case)
        now = datetime.now(UTC)
        trial = EvalTrial.queued(
            new_id(),
            suite_id,
            case_id,
            request.invocation_id,
            request.repeat_index,
            request.partition.value,
            case.case_digest,
            request.configuration_digest,
            now,
        )
        async with self._container.unit_of_work() as uow:
            await uow.evals.add_trial(trial)
            await uow.commit()
        try:
            trial = await self._update(trial.prepare(datetime.now(UTC)))
            fixture = self._fixtures.prepare(case, trial.id)
            registered = await self._container.repository_service.register(fixture.root)
            await self._container.repository_service.update_settings(
                registered.repository.id, case.repository_settings
            )
            task = await self._container.task_service.create(
                registered.repository.id, fixture.source_commit, f"eval-task-{trial.id}"
            )
            submitted = await self._container.message_service.submit(
                task.id, case.prompt, f"eval-message-{trial.id}"
            )
            trial = await self._update(
                trial.run(
                    registered.repository.id,
                    task.id,
                    submitted.run_id,
                    fixture.source_commit,
                    datetime.now(UTC),
                )
            )
            run = await self._await_terminal(submitted.run_id)
            trial = await self._update(trial.evaluate(datetime.now(UTC)))
            outcomes = []
            for definition in case.evaluators:
                outcome = await self._evaluators.evaluate(
                    EvaluationContext(
                        task.id, run.id, task.workspace_path, (), trial.id
                    ),
                    definition,
                )
                outcomes.append(outcome)
            if run.status is RunStatus.COMPLETED:
                verdict = (
                    ResultVerdict.ERROR
                    if any(item.verdict is ResultVerdict.ERROR for item in outcomes)
                    else ResultVerdict.FAILED
                    if any(item.verdict is ResultVerdict.FAILED for item in outcomes)
                    else ResultVerdict.PASSED
                )
                terminal = trial.complete(datetime.now(UTC))
            else:
                verdict = (
                    ResultVerdict.ERROR
                    if run.status is RunStatus.INTERRUPTED
                    else ResultVerdict.FAILED
                )
                terminal = trial.fail(
                    run.outcome_code or run.status.value, datetime.now(UTC)
                )
            result = EvalResult(
                trial.id,
                verdict,
                tuple(
                    {
                        "kind": definition.kind,
                        "verdict": outcome.verdict.value,
                        "code": outcome.code,
                        "evidence_artifact_id": str(outcome.evidence_artifact_id)
                        if outcome.evidence_artifact_id
                        else None,
                    }
                    for definition, outcome in zip(
                        case.evaluators, outcomes, strict=True
                    )
                ),
                None,
                datetime.now(UTC),
            )
            report = await self._reporter.create(
                terminal, result, self._suite.suite_digest
            )
            result = replace(result, report_artifact_id=report.artifact_id)
            async with self._container.unit_of_work() as uow:
                await uow.evals.add_result(result)
                await uow.evals.update_trial(terminal)
                await uow.commit()
            return result
        except asyncio.CancelledError:
            if trial.run_id is not None:
                await self._container.supervisor.cancel(trial.run_id)
            await self._update(trial.interrupt(datetime.now(UTC)))
            raise
        except Exception as error:
            if trial.run_id is not None:
                await self._container.supervisor.cancel(trial.run_id)
            failed = trial.fail(type(error).__name__, datetime.now(UTC))
            result = EvalResult(
                trial.id,
                ResultVerdict.ERROR,
                ({"code": type(error).__name__, "detail": str(error)[:1000]},),
                None,
                datetime.now(UTC),
            )
            async with self._container.unit_of_work() as uow:
                await uow.evals.add_result(result)
                await uow.evals.update_trial(failed)
                await uow.commit()
            return result

    async def _ensure_definitions(self, case: EvalCaseDefinition) -> tuple[UUID, UUID]:
        now = datetime.now(UTC)
        async with self._container.unit_of_work() as uow:
            suite = await uow.evals.get_suite(
                self._suite.partition.value,
                self._suite.suite_id,
                self._suite.suite_digest,
            )
            if suite is None:
                suite = EvalSuite(
                    new_id(),
                    self._suite.partition.value,
                    self._suite.suite_id,
                    self._suite.suite_digest,
                    {
                        "version": 1,
                        "cases": [item.case_id for item in self._suite.cases],
                    },
                    now,
                )
                await uow.evals.add_suite(suite)
            stored_case = await uow.evals.get_case(
                suite.id, case.case_id, case.case_digest
            )
            if stored_case is None:
                stored_case = EvalCase(
                    new_id(),
                    suite.id,
                    case.case_id,
                    case.case_digest,
                    {
                        "version": 1,
                        "fixture": case.fixture_ref,
                        "fixture_revision": case.fixture_revision,
                        "prompt": case.prompt,
                        "model": case.model,
                        "configuration": configuration_snapshot(case),
                        "evaluators": [
                            {
                                "kind": item.kind,
                                "options": item.options,
                                "content_digest": item.content_digest,
                            }
                            for item in case.evaluators
                        ],
                    },
                    now,
                )
                await uow.evals.add_case(stored_case)
            await uow.commit()
        return suite.id, stored_case.id

    async def _update(self, trial: EvalTrial) -> EvalTrial:
        async with self._container.unit_of_work() as uow:
            await uow.evals.update_trial(trial)
            await uow.commit()
        return trial

    async def _await_terminal(self, run_id: UUID) -> Run:
        while True:
            async with self._container.unit_of_work() as uow:
                run = await uow.runs.get(run_id)
                pending = await uow.approvals.list_pending_for_run(run_id)
            if run is None:
                raise RuntimeError("Eval Run disappeared")
            if run.status in (
                RunStatus.COMPLETED,
                RunStatus.FAILED,
                RunStatus.INTERRUPTED,
                RunStatus.CANCELLED,
            ):
                return run
            for approval in pending:
                if self._approval_prompter is None:
                    raise RuntimeError("human Approval prompter is required")
                choice = await self._approval_prompter(approval)
                if choice.decision not in (
                    ApprovalStatus.APPROVED,
                    ApprovalStatus.DENIED,
                ):
                    raise ValueError("Approval choice must be approved or denied")
                if not choice.decision_key:
                    raise ValueError("Approval decision key is required")
                await self._container.approval_service.decide(
                    approval.id,
                    choice.decision,
                    approval.spec_digest,
                    choice.reason,
                    choice.decision_key,
                )
            await asyncio.sleep(0.05)
