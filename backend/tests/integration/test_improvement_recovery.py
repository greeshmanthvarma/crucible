import pytest

from crucible.improvement.contracts import UsageAmounts
from crucible.improvement.lifecycle import ExperimentService, RecoveryRequired
from crucible.improvement.store import ExperimentStore
from tests.unit.test_improvement_manifest import make_manifest


async def test_uncertain_usage_blocks_resume(lifecycle_setup):
    service, config, _, _ = lifecycle_setup
    run = await service.start(config, "key", True)
    usage = await service.store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
    await service.store.mark_uncertain(usage)
    await service.store.fail_run(run.id, "transport")
    with pytest.raises(RecoveryRequired, match="usage"):
        await service.resume(run.id)
    assert (await service.store.get(run.id)).status != "ready"
    assert await service.store.unsettled_usage(run.id)
    assert service.docker.calls == []


async def test_recover_acknowledges_without_dispatch_or_retry(lifecycle_setup):
    service, config, _, _ = lifecycle_setup
    run = await service.start(config, "key", True)
    usage = await service.store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
    await service.store.mark_uncertain(usage)
    await service.store.fail_run(run.id, "transport")
    result = await service.recover(run.id, (usage,))
    assert result["status"] == "ready"
    assert result["cleanup_confirmed"]
    assert result["usage"]["conservative"]["tokens"] == 50
    assert await service.recover(run.id, (usage,)) == result
    assert await service.store.events(run.id, "trial_retry_authorized") == ()
    assert service.docker.calls == []


async def test_recovery_completion_cannot_clear_new_execution(tmp_path):
    from crucible.improvement.store import RevisionConflict

    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        stopped = await store.request_stop(run.id)
        first = await store.claim_recovery(run.id, stopped.revision, None, 123, "old")
        with pytest.raises(RevisionConflict):
            await store.claim_recovery(run.id, stopped.revision, None, 124, "other")
        await store.recover_ready(run.id, first)
        second = await store.claim_execution(run.id, "baseline")
        with pytest.raises(RevisionConflict):
            await store.recover_ready(run.id, first)
        with pytest.raises(RevisionConflict):
            await store.finish_close(run.id, first)
        assert await store.execution_owner(run.id) == str(second)
        assert (await store.get(run.id)).status == "running"
    finally:
        await store.close()


@pytest.fixture
async def lifecycle_setup(tmp_path, trial_setup, harness, monkeypatch):
    import hashlib
    import json
    from uuid import uuid4

    from crucible.improvement.checks import CHECK_IDS, suite_digest
    from crucible.improvement.contracts import EDITABLE_PATHS
    from crucible.improvement.lifecycle import StartConfig
    from crucible.improvement.source import _digest, inspect_source

    runner, _, original, *_ = trial_setup
    repository, commit = harness
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "suite.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                test_ids=CHECK_IDS,
                timeout_seconds=5,
                output_bytes=65536,
            )
        )
    )
    inspection = inspect_source(repository, commit)
    manifest = original.model_copy(
        update=dict(
            experiment_id=uuid4(),
            initial_commit=commit,
            initial_source_digest=inspection.source_digest,
            controller_digest=_digest(
                {n: v for n, v in inspection.files.items() if n not in EDITABLE_PATHS}
            ),
            dependency_lock_digest=hashlib.sha256(
                (repository / "backend/uv.lock").read_bytes()
            ).hexdigest(),
            boundary_suite_digest=suite_digest(suite),
        )
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(manifest.model_dump_json())
    config = StartConfig(
        schema_version=1,
        manifest_file=manifest_path,
        repository=repository,
        registry_root=runner.paths.registry_root,
        protected_suite_root=suite,
    )
    config_path = tmp_path / "config.json"
    config_path.write_text(config.model_dump_json())

    class Admission:
        async def check(self, config, manifest):
            pass

    class Docker:
        calls = []

        async def invoke(self, *args):
            self.calls.append(args)
            return ""

    root = tmp_path / "lifecycle"
    store = await ExperimentStore.open(root / "experiments.sqlite3")
    service = ExperimentService(store, root, admission=Admission(), docker=Docker())

    async def process_start(pid):
        return "scripted process identity"

    monkeypatch.setattr(service, "_process_start", process_start)
    yield service, config_path, manifest, repository
    await store.close()


async def test_start_stop_resume_close_without_dispatch(lifecycle_setup):
    from tests.unit.test_improvement_source import git

    service, config, manifest, repository = lifecycle_setup
    run = await service.start(config, "key", True)
    assert (await service.start(config, "key", True)).id == run.id
    assert git(repository, "rev-parse", "HEAD") == manifest.initial_commit
    assert len(await service.store.snapshots(run.id)) == 1
    assert (await service.stop(run.id))["status"] == "interrupted"
    assert (await service.resume(run.id))["status"] == "ready"
    assert service.docker.calls == []
    assert (await service.close(run.id))["status"] == "closed"
    assert not (repository / ".git/crucible-improvement-owner.json").exists()
    assert (service.root / "worktrees" / str(run.id)).exists()


async def test_crash_before_global_lock_is_recoverable(lifecycle_setup, monkeypatch):
    service, config, manifest, repository = lifecycle_setup
    original = service._repository_lock

    async def crash(*args, **kwargs):
        raise RuntimeError("simulated admission crash")

    monkeypatch.setattr(service, "_repository_lock", crash)
    with pytest.raises(RuntimeError, match="simulated"):
        await service.start(config, "key", True)
    assert service._config_path(manifest.experiment_id).exists()
    assert (await service.store.get(manifest.experiment_id)).status == "error"
    assert not (repository / ".git/crucible-improvement-owner.json").exists()
    monkeypatch.setattr(service, "_repository_lock", original)
    assert (await service.close(manifest.experiment_id))["status"] == "closed"


async def test_conflicting_idempotency_never_acquires_global_lock(lifecycle_setup):
    from crucible.improvement.manifest import ManifestDrift

    service, config, manifest, repository = lifecycle_setup
    await service.store.create(make_manifest(), "other", "key")
    with pytest.raises(ManifestDrift):
        await service.start(config, "key", True)
    assert not (repository / ".git/crucible-improvement-owner.json").exists()


pytest_plugins = [
    "tests.integration.test_improvement_trials",
    "tests.unit.test_improvement_source",
]


async def test_rejected_cross_root_start_cannot_evaluate(lifecycle_setup, monkeypatch):
    from crucible.improvement.store import RevisionConflict

    service, config, manifest, _ = lifecycle_setup
    await service.start(config, "first", True)
    other_root = service.root.parent / "other-root"
    other_store = await ExperimentStore.open(other_root / "experiments.sqlite3")
    other = ExperimentService(
        other_store, other_root, admission=service.admission, docker=service.docker
    )
    monkeypatch.setattr(other, "_process_start", service._process_start)
    try:
        with pytest.raises(RevisionConflict, match="another state root"):
            await other.start(config, "second", True)
        assert (await other.store.get(manifest.experiment_id)).status == "error"
        with pytest.raises(RevisionConflict):
            await other.evaluate_candidate(manifest.experiment_id)
        assert not (other_root / "worktrees").exists()
        assert service.docker.calls == []
        assert (await other.close(manifest.experiment_id))["status"] == "closed"
        assert (
            service._load_config(manifest.experiment_id).repository
            / ".git/crucible-improvement-owner.json"
        ).exists()
    finally:
        await other_store.close()


async def test_unexpected_worktree_edits_are_preserved(lifecycle_setup):
    from crucible.improvement.contracts import EDITABLE_PATHS

    service, config, manifest, _ = lifecycle_setup
    await service.start(config, "key", True)
    target = (
        service.root / "worktrees" / str(manifest.experiment_id) / EDITABLE_PATHS[0]
    )
    target.write_text("unexpected human change\n")
    await service.stop(manifest.experiment_id)
    with pytest.raises(ValueError, match="unexpected|ownership|checkpoint"):
        await service.resume(manifest.experiment_id)
    assert target.read_text() == "unexpected human change\n"
    assert (await service.store.get(manifest.experiment_id)).status != "ready"


async def test_crash_after_container_create_reconciles_without_duplicate_trial(
    lifecycle_setup,
):
    import json
    from uuid import UUID

    service, config, manifest, _ = lifecycle_setup
    await service.start(config, "key", True)
    labels = {"experiment": str(manifest.experiment_id), "job": "owned-job"}
    effect = await service.store.begin_effect(
        manifest.experiment_id, None, "worker", {"name": "owned-worker"}, labels
    )

    class Docker:
        running = True
        removed = False
        calls = []

        async def invoke(self, *args):
            self.calls.append(args)
            if args[0] == "ps":
                return "" if self.removed else "owned-worker"
            if args[0] == "rm":
                self.removed = True
                return ""
            if args[0] == "kill":
                self.running = False
                return ""
            assert args == ("inspect", "owned-worker")
            return json.dumps(
                [{"Config": {"Labels": labels}, "State": {"Running": self.running}}]
            )

    service.docker = Docker()
    assert (await service.stop(manifest.experiment_id))["status"] == "interrupted"
    assert (await service.resume(manifest.experiment_id))["status"] == "ready"
    assert [call for call in service.docker.calls if call[0] == "kill"] == [
        ("kill", "owned-worker")
    ]
    assert await service.store.trials(manifest.experiment_id) == ()
    assert (
        next(
            e
            for e in await service.store.effects(manifest.experiment_id)
            if UUID(str(e["id"])) == effect
        )["status"]
        == "stopped"
    )


async def test_result_written_before_ledger_commit_is_not_reusable(
    trial_setup, monkeypatch
):
    from crucible.improvement.trials import reusable_trial

    runner, store, manifest, snapshot, worker, grader = trial_setup
    original = store.trial_state

    async def checkpoint(trial, status, **kwargs):
        if status == "complete":
            raise RuntimeError("simulated result checkpoint crash")
        await original(trial, status, **kwargs)

    monkeypatch.setattr(store, "trial_state", checkpoint)
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.status == "error"
    rows = await store.trials(manifest.experiment_id)
    assert len(rows) == 1 and rows[0]["status"] == "error"
    assert list(runner.paths.root.rglob("result.json"))
    assert (
        await reusable_trial(
            store, manifest.experiment_id, str(rows[0]["key"]), root=runner.paths.root
        )
        is None
    )
    assert worker.calls == grader.calls == 1


async def test_closed_start_remains_idempotent(lifecycle_setup):
    service, config, manifest, repository = lifecycle_setup
    await service.start(config, "key", True)
    await service.close(manifest.experiment_id)
    assert (await service.start(config, "key", True)).status == "closed"
    assert not (repository / ".git/crucible-improvement-owner.json").exists()


@pytest.mark.parametrize("crash_at", ["before_git", "after_git"])
async def test_resume_reconciles_promotion_before_worktree_inspection(
    lifecycle_setup, monkeypatch, crash_at
):
    import hashlib

    import crucible.improvement.promotion as promotion_module
    from crucible.benchmarks.evidence import atomic_json
    from crucible.improvement.checks import CHECK_IDS, CandidateCheckResult
    from crucible.improvement.contracts import (
        EDITABLE_PATHS,
        ExperimentManifest,
        ExperimentPaths,
        HarnessSnapshot,
    )
    from crucible.improvement.lifecycle import StartConfig
    from crucible.improvement.promotion import PromotionService
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.source import stage_harness
    from crucible.improvement.trials import build_evaluation_set, owned_artifact
    from crucible.improvement.worktree import ExperimentWorktree
    from crucible.repair.identity import digest
    from tests.integration.test_improvement_promotion import record_trials
    from tests.unit.test_improvement_source import git

    service, config_path, _, repository = lifecycle_setup
    config = StartConfig.model_validate_json(config_path.read_bytes())
    manifest = ExperimentManifest.model_validate_json(
        config.manifest_file.read_bytes()
    ).model_copy(update={"schema_version": 2, "worker_protocol_version": 2})
    config.manifest_file.write_text(manifest.model_dump_json())
    run = await service.start(config_path, "key", True)
    paths = ExperimentPaths(
        root=service.root, registry_root=config.registry_root, repository=repository
    )
    baseline_id = await service.store.accepted_snapshot_id(run.id)
    row = await service.store.snapshot(run.id, baseline_id)
    baseline = HarnessSnapshot.model_validate_json(
        owned_artifact(service.root, str(row["artifact_ref"])).read_bytes()
    )
    owned = await ExperimentWorktree(service.root).create(
        repository, run.id, run.initial_commit
    )
    round = await service.store.begin_round(
        run.id, baseline_id, (await service.store.get(run.id)).revision
    )
    (owned.path / EDITABLE_PATHS[0]).write_text("value = 2\n")
    await owned.checkpoint_owned_edits()
    candidate = stage_harness(
        owned.path, run.initial_commit, service.root / "candidate", EDITABLE_PATHS
    )
    await service.store.register_snapshot(run.id, candidate, "candidate/snapshot.json")
    await record_trials(service.store, service.root, run.manifest, baseline, 0)
    await record_trials(service.store, service.root, run.manifest, candidate, 1)
    check_root = service.root / "promotion-checks"
    check_root.mkdir()
    atomic_json(
        check_root / "checks.log.json",
        [{"test_id": id, "verdict": "pass"} for id in CHECK_IDS],
    )
    checks = CandidateCheckResult(
        candidate.source_digest,
        candidate.export_digest,
        run.manifest.boundary_suite_digest,
        digest(
            [
                run.manifest.worker_image,
                run.manifest.worker_platform,
                run.manifest.dependency_lock_digest,
                run.manifest.controller_digest,
            ]
        ),
        "pass",
        "checks_passed",
        hashlib.sha256((check_root / "checks.log.json").read_bytes()).hexdigest(),
        True,
    )
    atomic_json(check_root / "result.json", checks)
    await service.store.record_event(
        run.id,
        "candidate_checks",
        {
            "path": "promotion-checks/result.json",
            "sha256": hashlib.sha256(
                (check_root / "result.json").read_bytes()
            ).hexdigest(),
        },
    )
    before = await build_evaluation_set(service.store, paths, run.id, baseline_id)
    after = await build_evaluation_set(
        service.store, paths, run.id, candidate.snapshot_id
    )
    decision = decide_candidate(before, after, checks)
    await service.store.record_decision(
        round.id, decision, (await service.store.get(run.id)).revision
    )
    promotions = PromotionService(service.store, paths, config.protected_suite_root)
    complete, git_call = service.store.complete_promotion, promotion_module.git_bytes

    async def crash_after(*args, **kwargs):
        raise RuntimeError("commit ack lost")

    def crash_before(root, *args, **kwargs):
        if "commit-tree" in args:
            raise RuntimeError("before commit")
        return git_call(root, *args, **kwargs)

    if crash_at == "after_git":
        monkeypatch.setattr(service.store, "complete_promotion", crash_after)
    else:
        monkeypatch.setattr(promotion_module, "git_bytes", crash_before)
    with pytest.raises(RuntimeError):
        await promotions.promote(round.id, digest(decision))
    monkeypatch.setattr(service.store, "complete_promotion", complete)
    monkeypatch.setattr(promotion_module, "git_bytes", git_call)
    await service.store.fail_run(run.id, "simulated_crash")
    assert (await service.resume(run.id))["status"] == "ready"
    if crash_at == "before_git":
        assert git(owned.path, "rev-parse", "HEAD") == run.initial_commit
    promoted = await promotions.promote(round.id, digest(decision))
    assert promoted.state == "complete"
    assert (
        await build_evaluation_set(service.store, paths, run.id, candidate.snapshot_id)
        == after
    )
    assert (await service.store.snapshot(run.id, candidate.snapshot_id))[
        "status"
    ] == "verified"
    assert len(await service.store.trials(run.id)) == 12
    assert git(repository, "rev-parse", "HEAD") == run.initial_commit


async def test_acknowledgment_retains_overage_and_is_idempotent(tmp_path):
    from crucible.improvement.store import RevisionConflict

    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        reservation = await store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
        await store.settle_usage(reservation, UsageAmounts(tokens=70))
        stopped = await store.request_stop(run.id)
        owner = await store.claim_recovery(run.id, stopped.revision, None, 123, "test")
        await store.acknowledge_usage(run.id, owner, (reservation,))
        await store.close()
        store = await ExperimentStore.open(tmp_path / "state.sqlite3")
        import asyncio

        await asyncio.gather(
            store.acknowledge_usage(run.id, owner, (reservation,)),
            store.acknowledge_usage(run.id, owner, (reservation,)),
        )
        rows = await store.usage_records(run.id)
        assert rows[0]["status"] == "conservative"
        assert UsageAmounts.model_validate_json(rows[0]["settled_json"]).tokens == 70
        assert len(await store.events(run.id, "usage_acknowledged")) == 1
        summary = await store.usage_summary(run.id)
        assert summary["confirmed"]["tokens"] == 70
        assert summary["conservative"]["tokens"] == 0
        await store.recover_ready(run.id, owner)
        assert not await store.unsettled_usage(run.id)
        assert await store.events(run.id, "explicit_resume") == ()
        with pytest.raises(RevisionConflict):
            await store.acknowledge_usage(run.id, owner, (reservation,))
    finally:
        await store.close()


async def test_acknowledgment_cannot_ignore_owned_effect(tmp_path):
    from crucible.improvement.store import RevisionConflict

    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        reservation = await store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
        await store.begin_effect(run.id, None, "worker", {"name": "survivor"}, {})
        await store.mark_uncertain(reservation)
        stopped = await store.request_stop(run.id)
        owner = await store.claim_recovery(run.id, stopped.revision, None, 123, "test")
        with pytest.raises(RevisionConflict, match="effects"):
            await store.acknowledge_usage(run.id, owner, (reservation,))
        assert await store.unsettled_usage(run.id)
    finally:
        await store.close()


async def test_partial_resource_usage_settles_known_time_without_inventing_output(
    tmp_path,
):
    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        reservation = await store.reserve_usage(
            run.id,
            None,
            UsageAmounts(worker_wall_seconds=50, active_seconds=40, output_bytes=100),
        )
        await store.record_partial_usage(
            reservation,
            UsageAmounts(worker_wall_seconds=7, active_seconds=3),
            ("output_bytes",),
        )
        summary = await store.usage_summary(run.id)
        assert summary["confirmed"]["worker_wall_seconds"] == 7
        assert summary["retained"]["worker_wall_seconds"] == 0
        assert summary["retained"]["output_bytes"] == 100
        stopped = await store.request_stop(run.id)
        owner = await store.claim_recovery(run.id, stopped.revision, None, 123, "test")
        await store.acknowledge_usage(run.id, owner, (reservation,))
        summary = await store.usage_summary(run.id)
        assert summary["confirmed"]["worker_wall_seconds"] == 7
        assert summary["conservative"]["worker_wall_seconds"] == 0
        assert summary["conservative"]["output_bytes"] == 100
    finally:
        await store.close()


async def test_stage_driver_never_advances_after_incomplete_baseline(
    lifecycle_setup, monkeypatch
):
    from contextlib import asynccontextmanager

    from crucible.improvement.trials import IsolatedTrialRunner

    service, config, _, _ = lifecycle_setup
    run = await service.start(config, "stage-test", True)
    calls = []

    @asynccontextmanager
    async def runtime(manifest):
        yield None

    async def incomplete(self, id, snapshot):
        return []

    async def search(id, **kwargs):
        calls.append(id)
        raise AssertionError("mining must not be dispatched")

    monkeypatch.setattr(service, "_runtime", runtime)
    monkeypatch.setattr(IsolatedTrialRunner, "run_selection_baseline", incomplete)
    monkeypatch.setattr(service, "search", search)
    result = await service.run_stages(run.id, "mining")
    assert result["complete"] is False
    assert result["status"] == "error"
    assert result["stage"] == "baseline"
    assert result["completed_stage"] is None
    assert calls == []
