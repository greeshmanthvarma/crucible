"""Durable, sequential repair coordination. External effects use narrow ports."""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.repair import (
    TERMINAL_STATUSES,
    RepairAttempt,
    RepairCheckpoint,
    RepairFix,
    RepairRun,
    RepairStage,
    RepairStatus,
)
from crucible.repair.cases import ApprovedCase, seal_case
from crucible.repair.events import append_repair_event
from crucible.repair.identity import canonical_json, digest
from crucible.repair.incident_context import IncidentContext
from crucible.repair.roles import (
    CandidateProposal,
    DiagnosisResult,
    EvalReview,
    FixItem,
    Role,
    RoleExecutor,
    RoleInfrastructureError,
    RoleLimitError,
    RoleOutputError,
    parse_role_result,
)
from crucible.repair.trials import (
    TrialEvidence,
    compare_cases,
    private_artifact,
)


class RepairNeedsReview(ValueError):
    pass


class RepairSourceDrift(ValueError):
    pass


class RepairInfrastructureError(RuntimeError):
    pass


class RepairUnsupported(RuntimeError):
    pass


@dataclass(frozen=True)
class TrialBatch:
    custom: dict[str, tuple[TrialEvidence, ...]]
    development: dict[str, tuple[TrialEvidence, ...]]
    held_out: dict[str, tuple[TrialEvidence, ...]]
    regressions: dict[str, tuple[TrialEvidence, ...]] = field(default_factory=dict)
    regression_cases: dict[str, ApprovedCase] = field(default_factory=dict)


class EvaluationPort(Protocol):
    async def identity(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
    ) -> str: ...
    async def baseline(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
    ) -> TrialBatch: ...
    async def candidate(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
        generation: int,
    ) -> TrialBatch: ...
    async def stop(self, repair_id: UUID) -> None: ...


class RecoveryPort(Protocol):
    async def verify(self, repair: RepairRun) -> None: ...
    async def prepare(self, repair: RepairRun) -> None: ...
    async def snapshot(
        self, repair: RepairRun, declared_paths: tuple[str, ...] | None = None
    ) -> dict[str, Any]: ...
    async def restore(self, repair: RepairRun, snapshot: dict[str, Any]) -> None: ...


class AcceptancePort(Protocol):
    async def accept(
        self,
        repair: RepairRun,
        fix: RepairFix,
        case: ApprovedCase,
        snapshot: dict[str, Any],
        evidence_ids: tuple[UUID, ...],
    ) -> str: ...


class PublicationPort(Protocol):
    async def publish(self, repair_id: UUID) -> object: ...


class RepairController:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        roles: RoleExecutor,
        incident: IncidentContext,
        artifacts: ArtifactService,
        *,
        evaluation: EvaluationPort | None = None,
        recovery: RecoveryPort | None = None,
        acceptance: AcceptancePort | None = None,
        notifier: EventNotifier | None = None,
        publication: PublicationPort | None = None,
        execution_id: str | None = None,
    ) -> None:
        self.factory, self.clock, self.roles, self.incident, self.artifacts = (
            unit_of_work,
            clock,
            roles,
            incident,
            artifacts,
        )
        (
            self.evaluation,
            self.recovery,
            self.acceptance,
            self.notifier,
            self.execution_id,
        ) = evaluation, recovery, acceptance, notifier, execution_id
        self.publication = publication
        self.active: dict[UUID, asyncio.Task[None]] = {}
        self.locks: dict[UUID, asyncio.Lock] = {}

    async def submit(self, repair_id: UUID) -> None:
        if repair_id not in self.active:
            task = asyncio.create_task(self.execute(repair_id))
            self.active[repair_id] = task
            task.add_done_callback(
                lambda completed: self._finished(repair_id, completed)
            )

    def _finished(self, repair_id: UUID, task: asyncio.Task[None]) -> None:
        self.active.pop(repair_id, None)
        if not task.cancelled():
            task.exception()

    async def stop(self, repair_id: UUID) -> None:
        execution = self.active.get(repair_id)
        if execution is not None and execution is not asyncio.current_task():
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        else:
            await self.roles.stop(repair_id)
            if self.evaluation:
                await self.evaluation.stop(repair_id)

    async def _get(self, repair_id: UUID) -> RepairRun:
        async with self.factory() as uow:
            run = await uow.repairs.get(repair_id)
        if run is None:
            raise ValueError("Repair not found")
        return run

    async def _checkpoint(
        self,
        repair: RepairRun,
        state: dict[str, Any],
        *,
        stage: RepairStage | None = None,
        status: RepairStatus = RepairStatus.RUNNING,
        code: str | None = None,
        detail: str | None = None,
        release: bool = False,
        new_fixes: tuple[RepairFix, ...] = (),
        updated_fixes: tuple[RepairFix, ...] = (),
        clear_current_fix: bool = False,
    ) -> RepairRun:
        async with self.factory() as uow:
            await uow.repairs.lock()
            current = await uow.repairs.get(repair.id)
            if current is None:
                raise ValueError("Repair not found")
            if current.status in TERMINAL_STATUSES:
                return current
            # A stop request wins over any concurrent successful stage transition.
            if (
                current.status is RepairStatus.STOPPED
                and status is RepairStatus.RUNNING
            ):
                raise asyncio.CancelledError()
            updated = replace(
                current,
                stage=stage or current.stage,
                current_fix_id=None if clear_current_fix else current.current_fix_id,
                status=status,
                updated_at=self.clock.now(),
                outcome_code=code,
                outcome_detail=detail,
            )
            await uow.repairs.update(updated)
            for fix in new_fixes:
                await uow.repairs.add_fix(fix)
            for fix in updated_fixes:
                await uow.repairs.update_fix(fix)
            attempts = await uow.repairs.list_attempts(repair.id)
            await uow.repairs.add_checkpoint(
                RepairCheckpoint(
                    uuid4(),
                    repair.id,
                    updated.stage,
                    updated.current_fix_id,
                    None,
                    digest(
                        {
                            "identity": repair.initial_identity_json,
                            "accepted": updated.last_accepted_commit,
                        }
                    ),
                    updated.last_accepted_commit,
                    {str(a.id): a.attempt_index for a in attempts},
                    {"controller_state": state},
                    self.clock.now(),
                )
            )
            await append_repair_event(uow, self.clock, updated)
            if release:
                await uow.repairs.release_global(repair.id)
            await uow.commit()
        if self.notifier:
            await self.notifier.notify(repair.source_task_id)
        return updated

    async def _load_state(self, repair_id: UUID) -> dict[str, Any]:
        async with self.factory() as uow:
            checkpoints = await uow.repairs.list_checkpoints(repair_id)
        # Storage order is persisted append order for equal timestamps.
        return next(
            (
                dict(cast(dict[str, Any], c.result_refs_json["controller_state"]))
                for c in reversed(checkpoints)
                if "controller_state" in c.result_refs_json
            ),
            {},
        )

    async def _json(self, artifact_id: str | UUID) -> Any:
        _, raw = await self.artifacts.read(UUID(str(artifact_id)))
        return json.loads(raw)

    async def _role(
        self,
        repair: RepairRun,
        role: Role,
        fix_id: UUID | None,
        index: int,
        inputs: tuple[UUID, ...],
    ) -> tuple[Any, UUID]:
        input_fingerprint = digest(
            {
                "role": role,
                "fix_id": fix_id,
                "attempt_index": index,
                "inputs": inputs,
                "accepted": repair.last_accepted_commit,
                "identity": repair.initial_identity_json,
                "role_configuration": self.roles.configuration_fingerprint,
            }
        )
        if role != "proposal":
            async with self.factory() as uow:
                checkpoints = await uow.repairs.list_checkpoints(repair.id)
                attempts = {
                    str(a.id): a for a in await uow.repairs.list_attempts(repair.id)
                }
            for checkpoint in reversed(checkpoints):
                if checkpoint.input_fingerprint != input_fingerprint:
                    continue
                prior = attempts.get(
                    str(checkpoint.result_refs_json.get("role_attempt"))
                )
                if prior and prior.status == "completed" and prior.output_artifact_id:
                    _, raw = await self.artifacts.read(prior.output_artifact_id)
                    return parse_role_result(
                        role, raw.decode()
                    ), prior.output_artifact_id
        attempt = RepairAttempt(
            uuid4(),
            repair.id,
            fix_id,
            role,
            index,
            None,
            "pending",
            False,
            self.clock.now(),
        )
        async with self.factory() as uow:
            await uow.repairs.add_attempt(attempt)
            await uow.repairs.add_checkpoint(
                RepairCheckpoint(
                    uuid4(),
                    repair.id,
                    repair.stage,
                    fix_id,
                    None,
                    input_fingerprint,
                    repair.last_accepted_commit,
                    {},
                    {
                        "role_attempt": str(attempt.id),
                        "input_artifacts": [str(value) for value in inputs],
                    },
                    self.clock.now(),
                )
            )
            await append_repair_event(uow, self.clock, repair)
            await uow.commit()
        try:
            result = await self.roles.execute(
                repair.id, role, fix_id, attempt.id, inputs
            )
            artifact = await private_artifact(
                self.artifacts, repair.source_task_id, result.model_dump_json().encode()
            )
        except asyncio.CancelledError:
            await self._finish_attempt(attempt, "interrupted")
            raise
        except RoleInfrastructureError:
            await self._finish_attempt(attempt, "infrastructure_error")
            raise
        except RoleOutputError:
            await self._finish_attempt(attempt, "invalid_output")
            raise
        except ValueError:
            await self._finish_attempt(attempt, "invalid_output")
            raise RoleOutputError("Role result was invalid") from None
        await self._finish_attempt(attempt, "completed", artifact)
        return result, artifact

    async def _finish_attempt(
        self, attempt: RepairAttempt, status: str, artifact: UUID | None = None
    ) -> None:
        async with self.factory() as uow:
            current = next(
                a
                for a in await uow.repairs.list_attempts(attempt.repair_id)
                if a.id == attempt.id
            )
            await uow.repairs.update_attempt(
                replace(
                    current,
                    status=status,
                    completed_at=self.clock.now(),
                    output_artifact_id=artifact,
                )
            )
            repair = await uow.repairs.get(attempt.repair_id)
            if repair:
                await append_repair_event(uow, self.clock, repair)
            await uow.commit()
        if repair and self.notifier:
            await self.notifier.notify(repair.source_task_id)

    async def _visible_input(self, repair: RepairRun, value: Any) -> UUID:
        return await private_artifact(
            self.artifacts,
            repair.source_task_id,
            canonical_json(self.incident.safe(value)),
        )

    async def execute(self, repair_id: UUID) -> None:
        async with self.locks.setdefault(repair_id, asyncio.Lock()):
            repair = await self._get(repair_id)
            async with self.factory() as uow:
                owner = await uow.repairs.owner()
            if (
                repair.status not in {RepairStatus.QUEUED, RepairStatus.RUNNING}
                or owner.repair_id != repair_id
                or self.execution_id is not None
                and owner.owner_execution_id != self.execution_id
            ):
                return
            state = await self._load_state(repair_id)
            if not state:
                state["baseline_batch_cache_hits"] = 0
            try:
                repair = await self._recover_acceptance(repair, state, "reconcile")
                configuration = self.roles.configuration_fingerprint
                if (
                    state.get("role_configuration")
                    and state["role_configuration"] != configuration
                ):
                    raise RepairSourceDrift("Frozen role configuration changed")
                state["role_configuration"] = configuration
                if self.recovery:
                    await self.recovery.verify(repair)
                repair = await self._recover_acceptance(repair, state, "resume_pending")
                repair = await self._checkpoint(repair, state)
                await self._execute(repair, state)
            except asyncio.CancelledError:
                await self.roles.stop(repair_id)
                if self.evaluation:
                    await self.evaluation.stop(repair_id)
                current = await self._get(repair_id)
                try:
                    await self._interrupt_candidate(current, state)
                except RepairNeedsReview:
                    await self._checkpoint(
                        current,
                        state,
                        status=RepairStatus.STOPPED,
                        code="needs_review",
                        detail="Unaccepted workspace needs review before resuming.",
                    )
                    raise asyncio.CancelledError()
                current = await self._get(repair_id)
                status = (
                    RepairStatus.STOPPED
                    if current.status is RepairStatus.STOPPED
                    else RepairStatus.INTERRUPTED
                )
                await self._checkpoint(
                    current,
                    state,
                    status=status,
                    code="interrupted",
                    detail="Repair stopped; resume explicitly.",
                )
                raise
            except RepairUnsupported:
                current = await self._get(repair_id)
                try:
                    await self._interrupt_candidate(current, state)
                except RepairNeedsReview:
                    await self._checkpoint(
                        current,
                        state,
                        status=RepairStatus.STOPPED,
                        code="needs_review",
                        detail="Unaccepted workspace needs review.",
                        release=True,
                    )
                    return
                async with self.factory() as uow:
                    fixes = await uow.repairs.list_fixes(repair_id)
                if any(fix.status == "accepted" for fix in fixes):
                    state["unresolved_outcome"] = "unsupported"
                    await self._publish(current, state)
                    return
                await self._checkpoint(
                    current,
                    state,
                    status=RepairStatus.COMPLETED,
                    code="unsupported",
                    detail="Essential replay evidence is unsupported or unavailable.",
                    release=True,
                )
            except RepairNeedsReview:
                await self._checkpoint(
                    repair,
                    state,
                    status=RepairStatus.STOPPED,
                    code="needs_review",
                    detail="Unexpected worktree edits require review.",
                    release=True,
                )
            except RepairSourceDrift:
                await self._checkpoint(
                    repair,
                    state,
                    status=RepairStatus.INVALIDATED,
                    code="source_drift",
                    detail="Repair inputs changed; start a new repair.",
                    release=True,
                )
            except (
                RoleInfrastructureError,
                RepairInfrastructureError,
                OSError,
                TimeoutError,
            ):
                await self._checkpoint(
                    repair,
                    state,
                    status=RepairStatus.RETRYABLE_ERROR,
                    code="infrastructure_error",
                    detail="Repair infrastructure failed; resume explicitly to retry.",
                    release=True,
                )

            except Exception:
                await self._checkpoint(
                    repair,
                    state,
                    status=RepairStatus.RETRYABLE_ERROR,
                    code="infrastructure_error",
                    detail="Repair infrastructure failed; resume explicitly to retry.",
                    release=True,
                )

    async def _diagnose(self, repair: RepairRun, state: dict[str, Any]) -> bool:
        async with self.factory() as uow:
            attempts = await uow.repairs.list_attempts(repair.id)
        charged = [
            a for a in attempts if a.role in {"diagnosis", "review"} and a.charged
        ]
        chain = int(
            state.get(
                "diagnosis_chain",
                max((a.attempt_index for a in charged), default=0) + 1,
            )
        )
        last = charged[-1] if charged else None
        if (
            last
            and last.attempt_index >= chain
            and last.status in {"invalid_output", "interrupted"}
        ):
            chain = last.attempt_index + 1
        if state.get("pending_diagnosis") and not last:
            chain = int(state["diagnosis_chain"])
        while chain <= 3:
            state["diagnosis_chain"] = chain
            repair = await self._checkpoint(repair, state, stage=RepairStage.DIAGNOSIS)
            feedback = await self._visible_input(
                repair, {"feedback": state.get("diagnosis_feedback", [])}
            )
            try:
                if state.get("pending_diagnosis"):
                    diagnosis_id = UUID(state["pending_diagnosis"])
                    diagnosis = DiagnosisResult.model_validate(
                        await self._json(diagnosis_id)
                    )
                else:
                    diagnosis, diagnosis_id = await self._role(
                        repair, "diagnosis", None, chain, (feedback,)
                    )
                    state["pending_diagnosis"] = str(diagnosis_id)
                    state["diagnosis_chain"] = chain
                    await self._checkpoint(repair, state)
                if not isinstance(diagnosis, DiagnosisResult):
                    raise RoleOutputError("Unexpected role")
                if diagnosis.decision != "repairable":
                    await self._finish_diagnosis(
                        repair,
                        state,
                        status=RepairStatus.COMPLETED,
                        code=diagnosis.decision,
                        detail=str(self.incident.safe("; ".join(diagnosis.reasons)))[
                            :1000
                        ],
                    )
                    return False
                prepared = []
                for item in diagnosis.fix_items:
                    if item.decision != "repairable":
                        continue
                    assert item.proposed_case is not None
                    await self.incident.authorize_case(repair.id, item.proposed_case)
                    item = await self._prepare_case(repair, item)
                    repair = await self._checkpoint(
                        repair, state, stage=RepairStage.EVAL_REVIEW
                    )
                    review_input = await self._visible_input(
                        repair,
                        {
                            "diagnosis": diagnosis.model_dump(mode="json"),
                            "fix": item.model_dump(mode="json"),
                        },
                    )
                    review, review_id = await self._role(
                        repair, "review", None, chain, (review_input,)
                    )
                    if not isinstance(review, EvalReview):
                        raise RoleOutputError("Unexpected role")
                    for evidence_id in review.evidence_ids:
                        await self.incident.read_evidence(repair.id, evidence_id)
                    if review.decision == "reject":
                        state["diagnosis_feedback"] = list(review.reasons)
                        raise RoleOutputError("Reviewer rejected the proposed case")
                    assert item.proposed_case is not None
                    approved = seal_case(item.proposed_case, review_id)
                    approved_id = await private_artifact(
                        self.artifacts,
                        repair.source_task_id,
                        approved.model_dump_json().encode(),
                    )
                    item_id = await private_artifact(
                        self.artifacts,
                        repair.source_task_id,
                        item.model_dump_json().encode(),
                    )
                    prepared.append(
                        {
                            "approved": str(approved_id),
                            "item": str(item_id),
                            "diagnosis": str(diagnosis_id),
                        }
                    )
                if not prepared:
                    await self._finish_diagnosis(
                        repair,
                        state,
                        status=RepairStatus.COMPLETED,
                        code="unsupported"
                        if any(
                            item.decision == "unsupported"
                            for item in diagnosis.fix_items
                        )
                        else "no_change_needed",
                        detail=str(self.incident.safe("; ".join(diagnosis.reasons)))[
                            :1000
                        ],
                    )
                    return False
                async with self.factory() as uow:
                    previous_fixes = await uow.repairs.list_fixes(repair.id)
                next_ordinal = (
                    max((fix.ordinal for fix in previous_fixes), default=-1) + 1
                )
                new_fixes = []
                for ordinal, record in enumerate(prepared, start=next_ordinal):
                    case = ApprovedCase.model_validate(
                        await self._json(record["approved"])
                    )
                    fix = RepairFix(
                        uuid4(),
                        repair.id,
                        ordinal,
                        case.case_digest,
                        case.case_digest,
                        "approved",
                    )
                    new_fixes.append(fix)
                    record["fix_id"] = str(fix.id)
                state.update(fixes=prepared, fix_index=0)
                state.pop("pending_diagnosis", None)
                await self._checkpoint(repair, state, new_fixes=tuple(new_fixes))
                return True
            except (RepairNeedsReview, RepairSourceDrift):
                raise
            except (RoleOutputError, ValueError) as error:
                if isinstance(error, RoleLimitError):
                    state["last_role_limit"] = error.code
                state.pop("pending_diagnosis", None)
                state.setdefault("diagnosis_feedback", []).append(
                    str(self.incident.safe(str(error)))[:1000]
                )
                chain += 1
                state["diagnosis_chain"] = chain
                await self._checkpoint(repair, state)
        await self._finish_diagnosis(
            repair,
            state,
            status=RepairStatus.EXHAUSTED,
            code="diagnosis_exhausted",
            detail="Diagnosis/review allowance exhausted."
            + (
                f" Last role limit: {state['last_role_limit']}."
                if state.get("last_role_limit")
                else ""
            ),
        )
        return False

    async def _finish_diagnosis(
        self,
        repair: RepairRun,
        state: dict[str, Any],
        *,
        status: RepairStatus,
        code: str,
        detail: str,
    ) -> None:
        async with self.factory() as uow:
            fixes = await uow.repairs.list_fixes(repair.id)
        if any(fix.status == "accepted" for fix in fixes):
            state["unresolved_outcome"] = code
            if status is RepairStatus.EXHAUSTED:
                state["terminal_status"] = "exhausted"
            try:
                await self._publish(repair, state)
            except (RepairNeedsReview, RepairSourceDrift):
                raise
            except ValueError as error:
                raise RepairInfrastructureError(
                    "Publication must be retried"
                ) from error
        else:
            await self._checkpoint(
                repair, state, status=status, code=code, detail=detail, release=True
            )

    async def _failed_reproduction(
        self,
        repair: RepairRun,
        state: dict[str, Any],
        fix: RepairFix,
        baseline: TrialBatch,
        case: ApprovedCase,
        item: FixItem,
    ) -> None:
        evidence_id = await private_artifact(
            self.artifacts, repair.source_task_id, canonical_json(baseline)
        )
        observations = []
        for rows in baseline.custom.values():
            for row in rows:
                observed = (
                    await self._json(row.observation_artifact_id)
                    if row.observation_artifact_id
                    else None
                )
                observations.append(
                    {
                        "verdict": row.verdict,
                        "assertions": row.assertion_results,
                        "observation": {
                            "status": observed.get("status"),
                            "error_code": observed.get("error_code"),
                            "detail": str(
                                self.incident.safe(observed.get("detail") or "")
                            )[:2048],
                            "output_excerpt": canonical_json(
                                self.incident.safe(observed)
                            ).decode()[:8000],
                        }
                        if isinstance(observed, dict)
                        else None,
                    }
                )
        next_state = dict(state)
        # Only custom reproduction observations return to diagnosis. Fixed-suite
        # and private regression traces stay in the host-only evidence artifact.
        next_state["diagnosis_feedback"] = [
            {
                "reason": "Custom reproduction did not match the recorded incident.",
                "observations": observations,
                "failed_case": case.case.model_dump(mode="json"),
                "fixture_files": item.fixture_files,
            }
        ]
        next_state["diagnosis_chain"] = int(state.get("diagnosis_chain", 1)) + 1
        next_state["failed_reproductions"] = [
            *state.get("failed_reproductions", []),
            str(evidence_id),
        ]
        for name in (
            "fixes",
            "fix_index",
            "pending_diagnosis",
            "baseline",
            "baseline_parent",
            "baseline_fingerprint",
            "proposal",
            "proposal_feedback",
            "reuse_proposal_index",
            "candidate_snapshot",
            "validation_evidence",
        ):
            next_state.pop(name, None)
        next_state["candidate_dirty"] = False
        async with self.factory() as uow:
            fixes = await uow.repairs.list_fixes(repair.id)
        updates = tuple(
            replace(
                item, status="not_reproduced" if item.id == fix.id else "superseded"
            )
            for item in fixes
            if item.status == "approved"
        )
        repair = await self._checkpoint(
            repair,
            next_state,
            stage=RepairStage.DIAGNOSIS,
            updated_fixes=updates,
            clear_current_fix=True,
        )
        state.clear()
        state.update(next_state)
        await self._execute(repair, state)

    async def _prepare_case(self, repair: RepairRun, item: FixItem) -> FixItem:
        from crucible.repair.cases import relative_path

        files = item.fixture_files
        if (
            len(files) > 32
            or sum(len((v or "").encode()) for v in files.values()) > 8 * 1024 * 1024
        ):
            raise RoleOutputError("Fixture exceeds capture limits")
        for name, value in files.items():
            relative_path(name)
            if value is not None and len(value.encode()) > 1024 * 1024:
                raise RoleOutputError("Fixture file is oversized")
        case = item.proposed_case
        if case is None:
            raise RoleOutputError("Proposed case is missing")
        if case.kind == "tool" and case.tool_name in {"write_file", "apply_patch"}:
            captured = None
            for evidence_id in case.evidence_ids:
                capture = await self.incident.read_evidence(repair.id, evidence_id)
                if capture.get("kind") == "preedit" and capture.get("complete"):
                    captured = {
                        entry["path"]: entry.get("content") if entry["exists"] else None
                        for entry in capture.get("files", [])
                    }
                    break
            if captured is None or files != captured:
                raise RoleOutputError(
                    "Exact pre-edit evidence is required for mutation replay"
                )
        fixture_digest = digest(
            {
                "schema_version": 1,
                "files": {
                    key: value for key, value in files.items() if value is not None
                },
            }
        )
        settings = await self.incident.replay_settings(repair.id, case)
        if self.evaluation is not None and getattr(self.evaluation, "model", None):
            settings["model"] = str(getattr(self.evaluation, "model"))
        return item.model_copy(
            update={
                "proposed_case": case.model_copy(
                    update={
                        "fixture_manifest_digest": fixture_digest,
                        "settings": settings,
                    }
                )
            }
        )

    async def _execute(self, repair: RepairRun, state: dict[str, Any]) -> None:
        if state.get("publication_only"):
            await self._publish(repair, state)
            return
        if "fixes" not in state:
            if not await self._diagnose(repair, state):
                return
        if self.evaluation is None:
            raise RepairInfrastructureError("Evaluation is not configured")
        for ordinal in range(state.get("fix_index", 0), len(state["fixes"])):
            record = state["fixes"][ordinal]
            fix_id = UUID(record["fix_id"])
            async with self.factory() as uow:
                current_repair = await uow.repairs.get(repair.id)
                if current_repair is None:
                    raise ValueError("Repair is missing")
                repair = current_repair
                fixes = await uow.repairs.list_fixes(repair.id)
                fix = next(f for f in fixes if f.id == fix_id)
                repair = replace(repair, current_fix_id=fix.id)
                await uow.repairs.update(repair)
                await uow.commit()
            if fix.status in {"accepted", "skipped"}:
                continue
            case = ApprovedCase.model_validate(await self._json(record["approved"]))
            item = FixItem.model_validate(await self._json(record["item"]))
            repair = await self._checkpoint(
                repair, state, stage=RepairStage.BASELINE_REPRODUCTION
            )
            if state.get("candidate_dirty"):
                await self._interrupt_candidate(repair, state)
            if self.recovery:
                await self.recovery.prepare(repair)
            baseline_identity = await self.evaluation.identity(
                repair, case, item.fixture_files
            )
            if (
                state.get("baseline")
                and state.get("baseline_parent") == repair.last_accepted_commit
                and state.get("baseline_fingerprint") == baseline_identity
            ):
                from pydantic import TypeAdapter

                baseline = TypeAdapter(TrialBatch).validate_python(
                    await self._json(state["baseline"])
                )
                state["baseline_batch_cache_hits"] = int(
                    state.get("baseline_batch_cache_hits", 0)
                ) + sum(
                    len(rows)
                    for suite in (
                        baseline.custom,
                        baseline.development,
                        baseline.held_out,
                        baseline.regressions,
                    )
                    for rows in suite.values()
                )
            else:
                baseline = await self.evaluation.baseline(
                    repair, case, item.fixture_files
                )
                self._check_infrastructure(baseline)
                if not any(
                    row.verdict == "pass"
                    for rows in baseline.custom.values()
                    for row in rows
                ):
                    await self._failed_reproduction(
                        repair, state, fix, baseline, case, item
                    )
                    return
                state["baseline"] = str(
                    await private_artifact(
                        self.artifacts, repair.source_task_id, canonical_json(baseline)
                    )
                )
                state["baseline_parent"] = repair.last_accepted_commit
                state["baseline_fingerprint"] = baseline_identity
                await self._checkpoint(repair, state)
            async with self.factory() as uow:
                attempts = await uow.repairs.list_attempts(repair.id)
            proposal_attempts = [
                a
                for a in attempts
                if a.role == "proposal" and a.fix_id == fix.id and a.charged
            ]
            index = max((a.attempt_index for a in proposal_attempts), default=0) + 1
            if state.pop("reuse_proposal_index", False) and proposal_attempts:
                index -= 1
            while index <= 3:
                if self.recovery:
                    await self.recovery.prepare(repair)
                repair = await self._checkpoint(
                    repair, state, stage=RepairStage.PROPOSAL
                )
                state["candidate_generation"] = (
                    int(state.get("candidate_generation", 0)) + 1
                )
                state["candidate_dirty"] = True
                state["candidate_snapshot"] = None
                await self._checkpoint(repair, state)
                visible = await self._visible_input(
                    repair,
                    {
                        "fix_id": str(fix.id),
                        "mechanism": item.mechanism,
                        "case": case.model_dump(mode="json"),
                        "feedback": state.get("proposal_feedback", []),
                    },
                )
                async with self.factory() as uow:
                    await uow.repairs.update_fix(
                        replace(fix, attempts_used=max(fix.attempts_used, index))
                    )
                    await uow.commit()
                try:
                    proposal, proposal_id = await self._role(
                        repair, "proposal", fix.id, index, (visible,)
                    )
                    if (
                        not isinstance(proposal, CandidateProposal)
                        or proposal.fix_id != fix.id
                    ):
                        raise RoleOutputError("Proposal fix identity differs")
                    if self.recovery is None:
                        raise RepairInfrastructureError(
                            "Workspace recovery is unavailable"
                        )
                    snapshot = await self.recovery.snapshot(
                        repair, proposal.declared_paths
                    )
                    state["candidate_snapshot"] = snapshot
                    state["proposal"] = str(proposal_id)
                    repair = await self._checkpoint(
                        repair, state, stage=RepairStage.CANDIDATE_VALIDATION
                    )
                    candidate = await self.evaluation.candidate(
                        repair,
                        case,
                        item.fixture_files,
                        int(state["candidate_generation"]),
                    )
                    self._check_infrastructure(candidate)
                    if (
                        await self.recovery.snapshot(repair, proposal.declared_paths)
                        != snapshot
                    ):
                        raise RepairNeedsReview("Candidate changed during evaluation")
                    gates = [
                        compare_cases(
                            baseline.custom,
                            candidate.custom,
                            custom_cases={case.case.case_id: case},
                        ),
                        compare_cases(baseline.development, candidate.development),
                        compare_cases(baseline.held_out, candidate.held_out),
                    ]
                    if baseline.regressions or candidate.regressions:
                        gates.append(
                            compare_cases(
                                baseline.regressions,
                                candidate.regressions,
                                custom_cases=baseline.regression_cases,
                            )
                        )
                    if all(gate.eligible for gate in gates):
                        evidence_id = await private_artifact(
                            self.artifacts,
                            repair.source_task_id,
                            canonical_json(
                                {"baseline": baseline, "candidate": candidate}
                            ),
                        )
                        state.update(
                            validation_evidence=str(evidence_id),
                            candidate_snapshot=snapshot,
                        )
                        repair = await self._checkpoint(
                            repair, state, stage=RepairStage.COMMIT_ACCEPTANCE
                        )
                        if self.acceptance is None:
                            raise RepairInfrastructureError(
                                "Acceptance adapter is not installed"
                            )
                        commit = await self.acceptance.accept(
                            repair,
                            fix,
                            case,
                            snapshot,
                            (UUID(state["baseline"]), evidence_id),
                        )
                        async with self.factory() as uow:
                            current = await uow.repairs.get(repair.id)
                            if current is None:
                                raise ValueError("Repair is missing")
                            repair = replace(current, last_accepted_commit=commit)
                            await uow.repairs.update(repair)
                            await uow.repairs.update_fix(
                                replace(
                                    fix,
                                    status="accepted",
                                    accepted_commit=commit,
                                    attempts_used=index,
                                )
                            )
                            await uow.commit()
                        state.update(fix_index=ordinal + 1, candidate_dirty=False)
                        for name in (
                            "baseline",
                            "baseline_parent",
                            "baseline_fingerprint",
                            "proposal_feedback",
                            "candidate_snapshot",
                            "validation_evidence",
                        ):
                            state.pop(name, None)
                        await self._checkpoint(repair, state)
                        break
                    state["proposal_feedback"] = [
                        gates[0].public_feedback(),
                        gates[1].public_feedback(),
                        gates[2].public_feedback(held_out=True),
                        *[gate.public_feedback() for gate in gates[3:]],
                    ]
                except RoleOutputError:
                    state["proposal_feedback"] = [
                        "Return a valid proposal using editable paths only."
                    ]
                except (
                    RoleInfrastructureError,
                    RepairInfrastructureError,
                    OSError,
                    TimeoutError,
                ):
                    state["reuse_proposal_index"] = True
                    await self._checkpoint(repair, state)
                    raise
                await self._interrupt_candidate(repair, state)
                index += 1
            else:
                async with self.factory() as uow:
                    fixes = await uow.repairs.list_fixes(repair.id)
                if any(item.status == "accepted" for item in fixes):
                    state["terminal_status"] = "exhausted"
                    await self._publish(repair, state)
                else:
                    await self._checkpoint(
                        repair,
                        state,
                        status=RepairStatus.EXHAUSTED,
                        code="proposal_exhausted",
                        detail="Proposal allowance exhausted.",
                        release=True,
                    )
                return
        await self._publish(repair, state)

    async def _publish(self, repair: RepairRun, state: dict[str, Any]) -> None:
        if self.recovery:
            await self.recovery.verify(repair)
        state["publication_only"] = True
        repair = await self._checkpoint(repair, state, stage=RepairStage.PUBLICATION)
        if self.publication is None:
            await self._checkpoint(
                repair,
                state,
                status=RepairStatus.RETRYABLE_ERROR,
                code="publication_pending",
                detail="Accepted fixes await the publication adapter.",
                release=True,
            )
            return
        await self.publication.publish(repair.id)
        status = (
            RepairStatus.EXHAUSTED
            if state.get("terminal_status") == "exhausted"
            else RepairStatus.COMPLETED
        )
        await self._checkpoint(
            repair,
            state,
            status=status,
            code="fixes_accepted",
            detail="Accepted fixes are ready for review.",
            release=True,
        )

    @staticmethod
    def _check_infrastructure(batch: TrialBatch) -> None:
        rows = [
            row
            for suite in (
                batch.custom,
                batch.development,
                batch.held_out,
                batch.regressions,
            )
            for group in suite.values()
            for row in group
        ]
        if any(row.verdict == "unsupported" for row in rows):
            raise RepairUnsupported("Replay evidence is unsupported")
        if not rows or any(row.verdict == "error" for row in rows):
            raise RepairInfrastructureError(
                "Evaluation did not produce behavioral evidence"
            )

    async def _recover_acceptance(
        self, repair: RepairRun, state: dict[str, Any], method: str
    ) -> RepairRun:
        recover = getattr(self.acceptance, method, None)
        if recover is not None and await recover(repair):
            repair = await self._get(repair.id)
            state["candidate_dirty"] = False
            for name in (
                "candidate_snapshot",
                "validation_evidence",
                "baseline",
                "baseline_parent",
                "baseline_fingerprint",
                "proposal_feedback",
            ):
                state.pop(name, None)
        return repair

    async def _interrupt_candidate(
        self, repair: RepairRun, state: dict[str, Any]
    ) -> None:
        if not state.get("candidate_dirty"):
            return
        if self.acceptance and hasattr(self.acceptance, "interrupt"):
            if await self.acceptance.interrupt(repair):
                state.update(candidate_dirty=False, candidate_snapshot=None)
                await self._checkpoint(
                    await self._get(repair.id),
                    state,
                    status=(await self._get(repair.id)).status,
                )
                return
        if self.recovery is None:
            raise RepairNeedsReview("Unaccepted workspace requires review")
        snapshot = state.get("candidate_snapshot")
        if snapshot is None:
            snapshot = await self.recovery.snapshot(repair)
        await self.recovery.restore(repair, snapshot)
        state.update(candidate_dirty=False, candidate_snapshot=None)
        await self._checkpoint(
            repair, state, status=(await self._get(repair.id)).status
        )
