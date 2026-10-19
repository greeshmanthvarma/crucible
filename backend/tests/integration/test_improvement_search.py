"""Bounded controller scheduling with scripted external execution ports."""

from uuid import uuid4

import pytest

from crucible.improvement.contracts import HarnessSnapshot
from crucible.improvement.roles import MiningResult, SelectionDecision
from crucible.improvement.search import SearchController
from crucible.improvement.selection import EvaluationSet, ScoredTrial
from crucible.improvement.store import ExperimentStore
from tests.unit.test_improvement_search_store import search_manifest

pytest_plugins = ["tests.integration.test_improvement_promotion"]


@pytest.fixture
async def search_setup(tmp_path):
    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    manifest = search_manifest()
    run = await store.create(manifest, "repo", "key")
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=manifest.initial_commit,
        source_digest=manifest.initial_source_digest,
        export_digest="b" * 64,
        patch_digest="c" * 64,
        kind="baseline",
    )
    await store.register_snapshot(run.id, snapshot, "base/snapshot.json")
    port = ScriptedSearch(store, manifest, snapshot)
    yield store, run.id, port
    await store.close()


async def test_no_actionable_mining_stops_without_proposal(search_setup):
    store, id, port = search_setup
    port.disposition = "no_actionable_weakness"
    result = await SearchController(store, port).run(id)
    assert result.reason == "no_actionable_weakness"
    assert result.ready_for_final
    assert port.proposals == 0
    assert len(await store.rounds(id)) == 1
    # Explicit repeated search does not run the role again.
    assert await SearchController(store, port).run(id) == result
    assert port.mining_calls == 1


@pytest.mark.parametrize(
    "reason", ["no_change", "invalid_proposal", "scope_rejected", "case_regression"]
)
async def test_two_rejects_still_execute_round_three(search_setup, reason):
    store, id, port = search_setup
    port.rejection = reason
    result = await SearchController(store, port).run(id)
    assert result.reason == "round_limit"
    assert len(result.completed_round_ids) == 3
    assert port.proposals == port.restores == 3
    assert port.mining_calls == 3
    assert len(await store.decisions(id)) == 3


async def test_incomplete_baseline_stops_before_mining(search_setup):
    store, id, port = search_setup
    port.incomplete = True
    result = await SearchController(store, port).run(id)
    assert result.reason == "infrastructure_error"
    assert not result.ready_for_final
    assert port.proposals == port.mining_calls == 0
    assert not await store.rounds(id)


async def test_all_development_pass_stops_before_mining(search_setup):
    store, id, port = search_setup
    port.all_pass = True
    result = await SearchController(store, port).run(id)
    assert result.reason == "all_development_pass"
    assert port.mining_calls == 0


async def test_infrastructure_error_is_never_a_rejection(search_setup):
    store, id, port = search_setup
    port.rejection = "infrastructure_error"
    result = await SearchController(store, port).run(id)
    assert result.reason == "infrastructure_error"
    assert not result.ready_for_final
    assert port.restores == 0
    assert port.proposals == 1
    assert (await store.decisions(id))[0].verdict == "inconclusive"


class ScriptedSearch:
    def __init__(self, store, manifest, snapshot):
        self.store, self.manifest, self.snapshot = store, manifest, snapshot
        self.disposition = "actionable"
        self.rejection = "no_change"
        self.proposals = self.restores = self.mining_calls = 0
        self.incomplete = self.all_pass = False
        self.interrupt = False

    async def baseline(self, experiment_id):
        return EvaluationSet(
            source_digest=self.snapshot.source_digest,
            export_digest=self.snapshot.export_digest,
            config_policy_digest="d" * 64,
            required_cases=tuple(
                c for c in self.manifest.cases if c.split != "final-test"
            ),
            trials=tuple(
                ScoredTrial(
                    case_id=c.case_id,
                    split=c.split,
                    repeat_index=i,
                    original_trial_id=uuid4(),
                    verdict="pass" if self.all_pass else "fail",
                    evidence_digest="e" * 64,
                )
                for c in self.manifest.cases
                if c.split != "final-test"
                for i in range(2 if self.incomplete else 3)
            ),
        )

    async def mine(self, round):
        self.mining_calls += 1
        if self.disposition == "actionable":
            # The controller accepts a parsed port result; role schema/ref validation
            # belongs to the concrete isolated RoleRunner, tested independently.
            from crucible.improvement.contracts import EDITABLE_PATHS
            from crucible.improvement.roles import EvidenceRef, Weakness

            return MiningResult(
                schema_version=1,
                disposition="actionable",
                summary="Observed development failure.",
                weaknesses=(
                    Weakness(
                        id="observed",
                        observed_behavior="Failure observed",
                        hypothesized_mechanism="Context loss",
                        evidence=(
                            EvidenceRef(
                                trial_id=uuid4(), artifact="trace", sha256="a" * 64
                            ),
                        ),
                        editable_paths=(EDITABLE_PATHS[0],),
                        confidence="low",
                        risks=(),
                        limitations="Scripted external port",
                    ),
                ),
            )
        return MiningResult(
            schema_version=1,
            disposition=self.disposition,
            weaknesses=(),
            summary="No actionable issue.",
        )

    async def candidate(self, round):
        self.proposals += 1
        if self.interrupt:
            import asyncio

            from crucible.improvement.roles import RoleExecution

            execution = RoleExecution(
                role_id=uuid4(),
                job_id=uuid4(),
                experiment_id=round.experiment_id,
                round_id=round.id,
                kind="proposal",
                accepted_snapshot_id=round.baseline_snapshot_id,
                source_digest=self.snapshot.source_digest,
                input_digest="a" * 64,
                input_ref="input.json",
                status="interrupted",
            )
            await self.store.save_role(
                execution, 0, (await self.store.get(round.experiment_id)).revision
            )
            raise asyncio.CancelledError
        return SelectionDecision(
            schema_version=1,
            verdict="inconclusive"
            if self.rejection == "infrastructure_error"
            else "reject",
            reason=self.rejection,
            baseline_source_digest=self.snapshot.source_digest,
            baseline_export_digest=self.snapshot.export_digest,
            candidate_source_digest=self.snapshot.source_digest,
            candidate_export_digest=self.snapshot.export_digest,
            trial_ids=(),
            counts=(),
            boundary_result_digest="f" * 64,
            policy_digest="d" * 64,
        )

    async def promote(self, round):
        raise AssertionError("unexpected promotion")

    async def restore(self, round):
        self.restores += 1


@pytest.fixture
async def local_search(promotion_setup, tmp_path):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.search import LocalSearchExecution
    from crucible.improvement.trials import IsolatedTrialRunner

    promotions, store, paths, owned, round, baseline, *_ = promotion_setup
    roles = MiningPort(store, paths.root)
    execution = LocalSearchExecution(
        store,
        paths,
        promotions.protected_suite_root,
        roles,
        IsolatedTrialRunner(store, paths, None, None, promotions.protected_suite_root),
        DevelopmentViewBuilder(store, paths.root),
        promotions,
        owned,
        None,
        tmp_path / "exports",
    )
    await execution.restore(round)
    return execution, roles, store, round, baseline


async def test_local_mining_exports_outside_private_state(local_search):
    execution, roles, store, round, _ = local_search
    result = await execution.mine(round)
    assert result.disposition == "no_actionable_weakness"
    assert roles.calls == 1
    assert len(await store.roles(round.id)) == 1
    assert await execution.mine(round) == result
    assert roles.calls == 1


async def test_local_restore_is_idempotent(local_search):
    execution, _, _, round, baseline = local_search
    await execution.restore(round)
    await execution.restore(round)
    assert (await execution.worktree.inspect()).source_digest == baseline.source_digest


async def test_local_baseline_error_never_schedules_implicit_retry(local_search):
    execution, _, store, round, baseline = local_search
    manifest = (await store.get(round.experiment_id)).manifest
    case = next(c for c in manifest.cases if c.split == "development")
    # A duplicate generation with no explicit recovery is ambiguous and must stop
    # before the execution port could schedule a fresh worker.
    await store.create_trial(
        round.experiment_id, baseline.snapshot_id, case.case_id, 0, "a" * 64
    )
    with pytest.raises(ValueError, match="ambiguous trial generations"):
        await execution.baseline(round.experiment_id)


async def test_malformed_mining_after_one_correction_is_failure(local_search):
    execution, roles, store, round, _ = local_search
    roles.invalid = True
    with pytest.raises(RuntimeError, match="malformed"):
        await execution.mine(round)
    assert roles.calls == 2
    assert [r.execution.status for r in await store.roles(round.id)] == [
        "invalid",
        "invalid",
    ]


class MiningPort:
    def __init__(self, store, root):
        self.store, self.root = store, root
        self.calls = 0
        self.invalid = self.error = False

    async def mine(self, round_id, view, correction_of=None):
        import hashlib

        from crucible.benchmarks.evidence import atomic_json
        from crucible.improvement.roles import RoleExecution

        self.calls += 1
        round = await self.store.round(round_id)
        role_id = uuid4()
        directory = self.root / "roles" / str(role_id)
        directory.mkdir(parents=True)
        atomic_json(directory / "input.json", {"view_index_digest": view.index_digest})
        raw = (
            b"{"
            if self.invalid
            else MiningResult(
                schema_version=1,
                disposition="no_actionable_weakness",
                weaknesses=(),
                summary="No actionable issue.",
            )
            .model_dump_json()
            .encode()
        )
        (directory / "output.json").write_bytes(raw)
        execution = RoleExecution(
            role_id=role_id,
            job_id=uuid4(),
            experiment_id=round.experiment_id,
            round_id=round_id,
            kind="format_correction" if correction_of else "mining",
            accepted_snapshot_id=round.baseline_snapshot_id,
            source_digest=view.source_digest,
            input_digest=hashlib.sha256(
                (directory / "input.json").read_bytes()
            ).hexdigest(),
            input_ref=str((directory / "input.json").relative_to(self.root)),
            output_digest=hashlib.sha256(raw).hexdigest(),
            output_ref=str((directory / "output.json").relative_to(self.root)),
            status="invalid" if self.invalid else "complete",
        )
        if self.error:
            execution = execution.model_copy(update={"status": "error"})
        await self.store.save_role(
            execution,
            1 if correction_of else 0,
            (await self.store.get(round.experiment_id)).revision,
        )
        if self.error:
            await self.store.fail_run(round.experiment_id, "role_infrastructure_error")
        return execution


async def recover(store, id):
    record = await store.request_stop(id)
    owner = await store.claim_recovery(id, record.revision, None, 123, None)
    await store.recover_ready(id, owner)


async def test_interrupted_proposal_consumes_round_on_explicit_resume(search_setup):
    import asyncio

    store, id, port = search_setup
    port.interrupt = True
    with pytest.raises(asyncio.CancelledError):
        await SearchController(store, port).run(id)
    assert port.proposals == 1
    assert (await store.get(id)).status == "interrupted"
    await recover(store, id)
    port.interrupt = False
    result = await SearchController(store, port).resume_search(id)
    assert result.reason == "round_limit"
    assert len(result.completed_round_ids) == 3
    assert port.proposals == 3
    assert (await store.rounds(id))[0].status == "interrupted"


async def test_trial_retry_requires_explicit_resume_and_preserves_original(
    search_setup,
):
    store, id, port = search_setup
    case = next(c for c in port.manifest.cases if c.split == "development")
    sid, key = port.snapshot.snapshot_id, "a" * 64
    original = await store.create_trial(id, sid, case.case_id, 0, key)
    await store.trial_state(original, "error", error="infrastructure")
    with pytest.raises(ValueError, match="explicit compatible recovery"):
        await store.require_trial_retry(id, sid, case.case_id, 0, key)
    await recover(store, id)
    await store.require_trial_retry(id, sid, case.case_id, 0, key)
    replacement = await store.create_trial(id, sid, case.case_id, 0, key)
    assert [r["id"] for r in await store.selected_trials(id)] == [str(replacement)]
    assert len(await store.trials(id)) == 2
    assert (await store.trials(id))[0]["status"] == "error"
    with pytest.raises(ValueError, match="explicit compatible recovery"):
        await store.require_trial_retry(id, sid, case.case_id, 0, key)


async def test_accepted_candidate_becomes_baseline_without_new_trials(
    promotion_setup, tmp_path
):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.search import LocalSearchExecution
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.trials import IsolatedTrialRunner, build_evaluation_set
    from crucible.repair.identity import digest

    promotions, store, paths, owned, round, base, candidate, checks = promotion_setup
    before = await build_evaluation_set(
        store, paths, round.experiment_id, base.snapshot_id
    )
    after = await build_evaluation_set(
        store, paths, round.experiment_id, candidate.snapshot_id
    )
    decision = decide_candidate(before, after, checks)
    await store.record_decision(
        round.id, decision, (await store.get(round.experiment_id)).revision
    )
    execution = LocalSearchExecution(
        store,
        paths,
        promotions.protected_suite_root,
        MiningPort(store, paths.root),
        IsolatedTrialRunner(store, paths, None, None, promotions.protected_suite_root),
        DevelopmentViewBuilder(store, paths.root),
        promotions,
        owned,
        None,
        tmp_path / "exports",
    )
    await execution.promote(await store.round(round.id))
    assert await execution.baseline(round.experiment_id) == after
    assert (
        await store.accepted_snapshot_id(round.experiment_id) == candidate.snapshot_id
    )
    assert (await store.promotions(round.experiment_id))[0].decision_digest == digest(
        decision
    )
    assert len(await store.trials(round.experiment_id)) == 12


async def test_inconclusive_candidate_can_resume_remaining_rounds(search_setup):
    store, id, port = search_setup
    port.rejection = "infrastructure_error"
    assert (
        await SearchController(store, port).run(id)
    ).reason == "infrastructure_error"
    await recover(store, id)
    port.rejection = "no_change"
    result = await SearchController(store, port).run(id)
    assert result.reason == "round_limit"
    assert port.proposals == 3
    assert (await store.decisions(id))[0].verdict == "inconclusive"


async def test_role_fail_closed_stop_flag_is_infrastructure(local_search):
    execution, roles, store, round, _ = local_search
    roles.error = True
    outcome = await SearchController(store, execution).run(round.experiment_id)
    assert outcome.reason == "infrastructure_error"
    assert not outcome.ready_for_final
    assert (await store.get(round.experiment_id)).status == "error"
