"""Sealing and post-search measurement; all worker/grader ports are scripted."""

from dataclasses import asdict
from uuid import uuid4

import pytest

from crucible.improvement.final import FinalEvaluator
from crucible.improvement.store import RevisionConflict
from crucible.repair.identity import digest

pytest_plugins = [
    "tests.integration.test_improvement_search",
    "tests.integration.test_improvement_trials",
]


async def test_seal_is_immutable_and_blocks_search(search_setup):
    store, id, port = search_setup
    from crucible.improvement.search import SearchController

    port.disposition = "no_actionable_weakness"
    outcome = await SearchController(store, port).run(id)
    seal = await store.seal_search(id, outcome.accepted_commit, outcome.reason)
    assert await store.seal_search(id, outcome.accepted_commit, outcome.reason) == seal
    assert seal.initial_snapshot_id == seal.selected_final_snapshot_id
    assert seal.selected_final_snapshot_id == port.snapshot.snapshot_id
    with pytest.raises(RevisionConflict):
        await store.require_search_open(id)
    with pytest.raises(RevisionConflict):
        await store.seal_search(id, "f" * 40, outcome.reason)
    with pytest.raises(RevisionConflict):
        await store.begin_round(
            id, port.snapshot.snapshot_id, (await store.get(id)).revision
        )
    assert digest(asdict(seal))


async def test_final_dispatch_requires_seal_before_any_external_effect(search_setup):
    store, id, port = search_setup
    with pytest.raises(RevisionConflict):
        await store.authorize_final(
            id, port.snapshot.snapshot_id, "final-test", 0, "a" * 64
        )
    with pytest.raises(RevisionConflict):
        await store.create_trial(
            id,
            port.snapshot.snapshot_id,
            "final-test",
            0,
            "a" * 64,
            final_authorization="b" * 64,
        )


async def test_final_authority_is_pinned_to_seal_and_two_snapshots(search_setup):
    store, id, port = search_setup
    from crucible.improvement.search import SearchController

    port.disposition = "no_actionable_weakness"
    outcome = await SearchController(store, port).run(id)
    seal = await store.seal_search(id, outcome.accepted_commit, outcome.reason)
    sha = digest(asdict(seal))
    await store.authorize_final(id, port.snapshot.snapshot_id, "final-test", 0, sha)
    for sid, case, repeat, key in [
        (uuid4(), "final-test", 0, sha),
        (port.snapshot.snapshot_id, "development", 0, sha),
        (port.snapshot.snapshot_id, "final-test", 3, sha),
        (port.snapshot.snapshot_id, "final-test", 0, "f" * 64),
    ]:
        with pytest.raises(RevisionConflict):
            await store.authorize_final(id, sid, case, repeat, key)


async def test_final_evaluation_denied_before_seal(search_setup, tmp_path):
    store, id, _ = search_setup
    evaluator = FinalEvaluator(store, None, None)
    with pytest.raises(RevisionConflict):
        await evaluator.evaluate_final(id)


@pytest.fixture
async def final_setup(trial_setup, tmp_path):
    from dataclasses import replace

    from crucible.benchmarks.manifests import case_digest
    from crucible.benchmarks.swebench import evaluation_identity
    from crucible.improvement.store import ExperimentStore
    from crucible.improvement.trials import IsolatedTrialRunner

    old_runner, _, original, snapshot, worker, old_grader = trial_setup
    store = await ExperimentStore.open(tmp_path / "v2.sqlite3")
    manifest = original.model_copy(
        update={"schema_version": 2, "worker_protocol_version": 2}
    )
    await store.create(manifest, "repository", "v2")
    await store.register_snapshot(
        manifest.experiment_id, snapshot, "snapshots/base/snapshot.json"
    )

    class Grader:
        async def grade(self, case, patch, trial_id, output_dir):
            result = await old_grader.grade(case, patch, trial_id, output_dir)
            return replace(
                result,
                evaluation_id=evaluation_identity(
                    trial_id,
                    case_digest(case),
                    patch.sha256,
                    manifest.grader.driver_digest,
                ),
            )

    runner = IsolatedTrialRunner(
        store, old_runner.paths, worker, Grader(), old_runner.protected_suite_root
    )
    # A real runner supplies every selection trial before sealing.
    await runner.run_selection_baseline(manifest.experiment_id, snapshot.snapshot_id)
    await store.record_event(
        manifest.experiment_id,
        "search_outcome",
        {
            "reason": "no_actionable_weakness",
            "ready_for_final": True,
            "accepted_commit": manifest.initial_commit,
        },
    )
    seal = await store.seal_search(
        manifest.experiment_id, manifest.initial_commit, "no_actionable_weakness"
    )
    yield (
        FinalEvaluator(store, runner.paths, runner),
        runner,
        store,
        manifest,
        snapshot,
        seal,
        worker,
        old_grader,
    )
    await store.close()


async def test_same_source_final_uses_three_fresh_trials_and_shared_original_ids(
    final_setup,
):
    evaluator, runner, store, manifest, snapshot, seal, worker, grader = final_setup
    assert worker.calls == 6
    measurement = await evaluator.evaluate_final(manifest.experiment_id)
    assert measurement.complete
    assert worker.calls == grader.calls == 9
    hidden = next(c for c in measurement.cases if c.split == "final-test")
    assert hidden.initial_trials == hidden.final_trials
    assert len({t.original_trial_id for t in hidden.initial_trials}) == 3
    assert hidden.initial_passes == hidden.final_passes == 0
    assert await store.events(manifest.experiment_id, "final_same_source")
    assert await evaluator.evaluate_final(manifest.experiment_id) == measurement
    assert worker.calls == 9
    assert (
        await store.search_seal(manifest.experiment_id)
    ).selected_final_commit == seal.selected_final_commit
    with pytest.raises(RevisionConflict):
        await runner.run(manifest.experiment_id, snapshot.snapshot_id, "development", 0)
    with pytest.raises(ValueError, match="final-test"):
        runner.resolve_case(manifest, "final-test")


async def test_final_infrastructure_is_inconclusive_and_requires_explicit_resume(
    final_setup,
    tmp_path,
):
    evaluator, _, store, manifest, _, seal, worker, grader = final_setup
    worker.cleanup = False
    result = await evaluator.evaluate_final(manifest.experiment_id)
    assert not result.complete
    assert any(e.reason == "error" for e in result.errors)
    assert grader.calls == 6
    assert worker.calls == 7
    assert await store.search_seal(manifest.experiment_id) == seal
    with pytest.raises(ValueError):
        await evaluator.evaluate_final(manifest.experiment_id)
    assert worker.calls == 7
    from tests.integration.test_improvement_search import recover

    await recover(store, manifest.experiment_id)
    worker.cleanup = True
    with pytest.raises(ValueError, match="explicit compatible authorization"):
        await evaluator.evaluate_final(manifest.experiment_id)
    assert worker.calls == 7
    await recover(store, manifest.experiment_id)
    from uuid import UUID

    failed = tuple(
        UUID(str(row["id"]))
        for row in await store.selected_trials(manifest.experiment_id)
        if row["status"] in ("error", "interrupted")
    )
    await store.authorize_trial_retries(manifest.experiment_id, failed)
    result = await evaluator.evaluate_final(manifest.experiment_id)
    assert result.complete
    assert worker.calls == 10  # original failed attempt plus exactly three valid trials
    assert len(await store.trials(manifest.experiment_id)) == 10
    assert await store.events(manifest.experiment_id, "trial_superseded")
    import json

    from crucible.improvement.reporting import ReportExporter

    await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, tmp_path / "retry-report"
    )
    report = json.loads((tmp_path / "retry-report/metrics.json").read_text())
    assert len(report["trial_retry_links"]) == 1
    assert report["recovery"]["cleanup_confirmed"] is True


async def test_final_regression_is_reported_without_reopening_selection(
    promotion_setup,
):
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.trials import build_evaluation_set
    from tests.integration.test_improvement_promotion import record_trials

    promotions, store, paths, _, round, baseline, candidate, checks = promotion_setup
    id = round.experiment_id
    before = await build_evaluation_set(store, paths, id, baseline.snapshot_id)
    after = await build_evaluation_set(store, paths, id, candidate.snapshot_id)
    decision = decide_candidate(before, after, checks)
    await store.record_decision(round.id, decision, (await store.get(id)).revision)
    promotion = await promotions.promote(round.id, digest(decision))
    await store.record_event(
        id,
        "search_outcome",
        {
            "ready_for_final": True,
            "reason": "round_limit",
            "accepted_commit": promotion.commit_sha,
        },
    )
    seal = await store.seal_search(id, promotion.commit_sha, "round_limit")
    sha = digest(asdict(seal))
    manifest = (await store.get(id)).manifest
    await record_trials(
        store, paths.root, manifest, baseline, 3, final_authorization=sha
    )
    await record_trials(
        store, paths.root, manifest, candidate, 0, final_authorization=sha
    )
    measurement = await FinalEvaluator(store, paths, None).measurement(id)
    assert measurement.complete
    hidden = next(c for c in measurement.cases if c.split == "final-test")
    assert (hidden.initial_passes, hidden.final_passes) == (3, 0)
    assert (await store.get(id)).last_accepted_commit == promotion.commit_sha
    with pytest.raises(RevisionConflict):
        await promotions.promote(round.id, digest(decision))
    from tests.integration.test_improvement_search import recover

    await recover(store, id)
    with pytest.raises(RevisionConflict):
        await store.require_search_open(id)
