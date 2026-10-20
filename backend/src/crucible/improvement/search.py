"""Bounded search scheduling; execution adapters retain all original evidence."""

import asyncio
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

from crucible.improvement.checks import ProbePort
from crucible.improvement.contracts import Contract, ExperimentPaths
from crucible.improvement.evidence_views import DevelopmentViewBuilder
from crucible.improvement.promotion import PromotionService
from crucible.improvement.roles import (
    DevelopmentView,
    MiningResult,
    RoleRunner,
    SelectionDecision,
)
from crucible.improvement.selection import EvaluationSet, selection_counts
from crucible.improvement.store import ExperimentStore, RoundRecord
from crucible.improvement.trials import IsolatedTrialRunner
from crucible.improvement.worktree import OwnedWorktree

StopReason = Literal[
    "round_limit",
    "no_actionable_weakness",
    "insufficient_evidence",
    "out_of_scope",
    "all_development_pass",
    "operator_stopped",
    "infrastructure_error",
    "budget_limit",
]


class SearchOutcome(Contract):
    reason: StopReason
    accepted_commit: str
    completed_round_ids: tuple[UUID, ...]
    ready_for_final: bool


class SearchExecution(Protocol):
    async def baseline(self, experiment_id: UUID) -> EvaluationSet: ...
    async def mine(self, round: RoundRecord) -> MiningResult: ...
    async def candidate(self, round: RoundRecord) -> SelectionDecision: ...
    async def promote(self, round: RoundRecord) -> None: ...
    async def restore(self, round: RoundRecord) -> None: ...


class SearchController:
    def __init__(self, store: ExperimentStore, execution: SearchExecution) -> None:
        self.store, self.execution = store, execution

    async def _outcome(self, id: UUID, reason: StopReason) -> SearchOutcome:
        run = await self.store.get(id)
        outcome = SearchOutcome(
            reason=reason,
            accepted_commit=run.last_accepted_commit,
            completed_round_ids=tuple(
                r.id
                for r in await self.store.rounds(id)
                if r.status in ("decided", "interrupted")
            ),
            ready_for_final=reason
            not in ("operator_stopped", "infrastructure_error", "budget_limit"),
        )
        await self.store.record_event(
            id, "search_outcome", outcome.model_dump(mode="json")
        )
        return outcome

    async def resume_search(self, experiment_id: UUID) -> SearchOutcome:
        # Lifecycle recovery reconciles only; this call explicitly resumes work.
        return await self.run(experiment_id)

    async def run(self, experiment_id: UUID) -> SearchOutcome:
        from crucible.improvement.store import BudgetDenied

        id = experiment_id
        await self.store.require_search_open(id)
        previous = await self.store.events(id, "search_outcome")
        if previous and previous[-1].get("ready_for_final"):
            from crucible.repair.identity import canonical_json

            return SearchOutcome.model_validate_json(canonical_json(previous[-1]))
        try:
            rounds = await self.store.rounds(id)
            if rounds and rounds[-1].status == "decided":
                last = rounds[-1]
                assert last.decision_ref is not None
                decision = SelectionDecision.model_validate_json(last.decision_ref)
                if decision.verdict == "promote":
                    await self.execution.promote(last)
                elif decision.verdict == "reject":
                    await self.execution.restore(last)
                else:
                    # A ready run has passed explicit lifecycle recovery.
                    # Preserve the inconclusive decision and its consumed ordinal.
                    await self.execution.restore(last)
            while True:
                await self.store.require_search_open(id)
                rounds = await self.store.rounds(id)
                active = (
                    rounds[-1]
                    if rounds and rounds[-1].status not in ("decided", "interrupted")
                    else None
                )
                if active and active.decision_digest:
                    # Reconcile promotion before any new effect.
                    await self.execution.promote(active)
                    active = None
                if active:
                    roles = await self.store.roles(active.id)
                    if any(r.execution.kind == "proposal" for r in roles):
                        # Recovery must never grant a second proposal opportunity.
                        await self.execution.restore(active)
                        await self.store.interrupt_round(active.id)
                        continue
                if active is None and len(rounds) >= 3:
                    return await self._outcome(id, "round_limit")
                evaluation = await self.execution.baseline(id)
                counts = selection_counts(evaluation)
                if counts is None:
                    await self.store.fail_run(id, "incomplete_selection_baseline")
                    return await self._outcome(id, "infrastructure_error")
                if all(
                    counts[c.case_id] == 3
                    for c in evaluation.required_cases
                    if c.split == "development"
                ):
                    return await self._outcome(id, "all_development_pass")
                if active is None:
                    run = await self.store.require_search_open(id)
                    active = await self.store.begin_round(
                        id, await self.store.accepted_snapshot_id(id), run.revision
                    )
                mining = await self.execution.mine(active)
                if mining.disposition != "actionable":
                    return await self._outcome(id, mining.disposition)
                decision = await self.execution.candidate(active)
                run = await self.store.require_search_open(id)
                await self.store.record_decision(active.id, decision, run.revision)
                active = await self.store.round(active.id)
                if decision.verdict == "inconclusive":
                    await self.store.fail_run(id, "candidate_infrastructure")
                    return await self._outcome(id, "infrastructure_error")
                if decision.verdict == "promote":
                    await self.execution.promote(active)
                else:
                    await self.execution.restore(active)
        except asyncio.CancelledError:
            await self.store.fail_run(id, "operator_interrupted", interrupted=True)
            await self._outcome(id, "operator_stopped")
            raise
        except BudgetDenied:
            await self.store.fail_run(id, "search_budget_limit")
            return await self._outcome(id, "budget_limit")
        except Exception:
            stopped = (await self.store.get(id)).status in ("stopping", "interrupted")
            await self.store.fail_run(
                id,
                "operator_stopped" if stopped else "search_infrastructure",
                interrupted=stopped,
            )
            return await self._outcome(
                id, "operator_stopped" if stopped else "infrastructure_error"
            )


class LocalSearchExecution:
    """Composition of the existing isolated roles, checks, trials and promotion."""

    def __init__(
        self,
        store: ExperimentStore,
        paths: ExperimentPaths,
        suite_root: Path,
        roles: RoleRunner,
        trials: IsolatedTrialRunner,
        views: DevelopmentViewBuilder,
        promotions: PromotionService,
        worktree: OwnedWorktree,
        probes: ProbePort,
        view_root: Path,
    ) -> None:
        self.store, self.paths, self.suite_root = store, paths, suite_root
        self.roles, self.trials, self.views = roles, trials, views
        self.promotions, self.worktree, self.probes = promotions, worktree, probes
        self._views: dict[UUID, DevelopmentView] = {}
        self.view_root = view_root

    async def baseline(self, experiment_id: UUID) -> EvaluationSet:
        from crucible.improvement.trials import build_evaluation_set

        sid = await self.store.accepted_snapshot_id(experiment_id)
        # Verify existing evidence before scheduling; ambiguous generations fail closed.
        before = await build_evaluation_set(self.store, self.paths, experiment_id, sid)
        if selection_counts(before) is None:
            await self.trials.run_selection_baseline(experiment_id, sid)
        return await build_evaluation_set(self.store, self.paths, experiment_id, sid)

    async def mine(self, round: RoundRecord) -> MiningResult:
        import hashlib
        import json
        from uuid import uuid4

        from crucible.improvement.roles import parse_mining_output
        from crucible.improvement.trials import owned_artifact
        from crucible.improvement.worker_supervisor import read_regular_artifact

        view = await self.views.build_development_view(
            round.experiment_id,
            round.baseline_snapshot_id,
            self.view_root / str(uuid4()),
        )
        self._views[round.id] = view
        records = await self.store.roles(round.id)
        mining = [
            r.execution
            for r in records
            if r.execution.kind in ("mining", "format_correction")
        ]
        execution = mining[-1] if mining else await self.roles.mine(round.id, view)
        if mining and execution.status == "error":
            execution = await self.roles.recover_mining(execution, view)
        if execution.status == "invalid" and execution.kind == "mining":
            execution = await self.roles.mine(
                round.id, view, correction_of=execution.role_id
            )
        if execution.status == "invalid":
            raise RuntimeError("mining malformed after its single correction")
        if execution.status != "complete" or execution.output_ref is None:
            from crucible.improvement.store import BudgetDenied

            if execution.termination == "budget_exhausted":
                raise BudgetDenied("role budget exhausted")
            raise RuntimeError(
                "mining requires explicit recovery of original execution"
            )
        raw_input = read_regular_artifact(
            owned_artifact(self.paths.root, execution.input_ref), 1_048_576
        )
        raw = read_regular_artifact(
            owned_artifact(self.paths.root, execution.output_ref), 1_048_576
        )
        if (
            hashlib.sha256(raw_input).hexdigest() != execution.input_digest
            or hashlib.sha256(raw).hexdigest() != execution.output_digest
            or json.loads(raw_input)["view_index_digest"] != view.index_digest
            or execution.accepted_snapshot_id != round.baseline_snapshot_id
            or execution.source_digest != view.source_digest
        ):
            raise ValueError("mining checkpoint drift")
        return parse_mining_output(raw.decode(), view.evidence_refs)

    async def candidate(self, round: RoundRecord) -> SelectionDecision:
        import hashlib
        from uuid import uuid4

        from crucible.improvement.checks import check_candidate
        from crucible.improvement.roles import parse_mining_output
        from crucible.improvement.selection import decide_candidate
        from crucible.improvement.source import ScopeRejected
        from crucible.improvement.trials import build_evaluation_set, owned_artifact
        from crucible.improvement.worker_supervisor import read_regular_artifact
        from crucible.repair.identity import digest

        id = round.experiment_id
        baseline = await build_evaluation_set(
            self.store, self.paths, id, round.baseline_snapshot_id
        )
        view = self._views[round.id]
        mining_execution = [
            r.execution
            for r in await self.store.roles(round.id)
            if r.execution.kind in ("mining", "format_correction")
            and r.execution.status == "complete"
        ][-1]
        assert mining_execution.output_ref is not None
        mining = parse_mining_output(
            read_regular_artifact(
                owned_artifact(self.paths.root, mining_execution.output_ref), 1_048_576
            ).decode(),
            view.evidence_refs,
        )
        execution = await self.roles.propose(round.id, mining, view)
        if execution.status not in ("complete", "invalid"):
            from crucible.improvement.store import BudgetDenied

            if execution.termination == "budget_exhausted":
                raise BudgetDenied("proposal budget exhausted")
            raise RuntimeError("proposal execution incomplete")
        try:
            candidate = await self.roles.collect_proposal(execution, self.worktree)
        except ScopeRejected as error:
            reason = error.reason
            if reason not in (
                "no_change",
                "invalid_proposal",
                "protected proposal changes",
            ):
                raise
            return SelectionDecision(
                schema_version=1,
                verdict="reject",
                reason="no_change"
                if reason == "no_change"
                else "invalid_proposal"
                if reason == "invalid_proposal"
                else "scope_rejected",
                baseline_source_digest=baseline.source_digest,
                baseline_export_digest=baseline.export_digest,
                candidate_source_digest=baseline.source_digest,
                candidate_export_digest=baseline.export_digest,
                trial_ids=tuple(t.original_trial_id for t in baseline.trials),
                counts=(),
                boundary_result_digest=digest(
                    {"proposal": str(execution.role_id), "reason": reason}
                ),
                policy_digest=baseline.config_policy_digest,
            )
        run = await self.store.require_search_open(id)
        _, export = await self.trials.load_snapshot(id, candidate.snapshot_id)
        checks_root = self.paths.root / "checks" / str(uuid4())
        checks = await check_candidate(
            candidate,
            run.manifest,
            self.suite_root,
            checks_root,
            harness_export=export,
            probes=self.probes,
        )
        result_file = checks_root / "result.json"
        await self.store.record_event(
            id,
            "candidate_checks",
            {
                "path": str(result_file.relative_to(self.paths.root)),
                "sha256": hashlib.sha256(result_file.read_bytes()).hexdigest(),
            },
        )
        if checks.verdict == "pass" and checks.cleanup_confirmed:
            await self.trials.run_selection_baseline(id, candidate.snapshot_id)
        evaluation = await build_evaluation_set(
            self.store, self.paths, id, candidate.snapshot_id
        )
        return decide_candidate(baseline, evaluation, checks)

    async def promote(self, round: RoundRecord) -> None:
        assert round.decision_digest is not None
        await self.promotions.promote(round.id, round.decision_digest)
        from dataclasses import replace

        run = await self.store.get(round.experiment_id)
        self.worktree = replace(self.worktree, accepted_commit=run.last_accepted_commit)

    async def restore(self, round: RoundRecord) -> None:
        from uuid import uuid4

        baseline = await self.promotions.accepted_snapshot(round.experiment_id)
        if (await self.worktree.inspect()).source_digest == baseline.source_digest:
            return
        await self.worktree.archive_interrupted(uuid4())
        await self.worktree.restore_last_accepted(baseline.source_digest)
