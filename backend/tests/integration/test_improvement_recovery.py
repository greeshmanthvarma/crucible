import pytest

from crucible.improvement.contracts import UsageAmounts
from crucible.improvement.lifecycle import ExperimentService, RecoveryRequired
from crucible.improvement.store import ExperimentStore
from tests.unit.test_improvement_manifest import make_manifest


async def test_uncertain_usage_blocks_resume(tmp_path):
    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        usage = await store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
        await store.mark_uncertain(usage)
        service = ExperimentService(store, tmp_path)
        with pytest.raises(RecoveryRequired, match="usage"):
            await service.resume(run.id)
        assert (await store.get(run.id)).status == "error"
    finally:
        await store.close()


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
        calls = []

        async def invoke(self, *args):
            self.calls.append(args)
            if args[0] == "ps":
                return "owned-worker"
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
