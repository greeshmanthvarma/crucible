"""Real local Git/SQLite promotion recovery; no model or Docker execution."""

import hashlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.benchmarks.contracts import (
    GradeResult,
    SolutionPatch,
    SolverResult,
    TrialIdentity,
    TrialResult,
)
from crucible.benchmarks.evidence import atomic_json
from crucible.benchmarks.swebench import evaluation_identity
from crucible.improvement.checks import CHECK_IDS, CandidateCheckResult, suite_digest
from crucible.improvement.contracts import EDITABLE_PATHS, ExperimentPaths
from crucible.improvement.fingerprints import trial_key
from crucible.improvement.promotion import PromotionService
from crucible.improvement.source import stage_harness
from crucible.improvement.store import ExperimentStore
from crucible.improvement.worktree import ExperimentWorktree
from crucible.repair.identity import digest
from tests.unit.test_improvement_search_store import search_manifest
from tests.unit.test_improvement_source import git

pytest_plugins = ["tests.unit.test_improvement_source"]


@pytest.fixture
async def promotion_setup(harness, tmp_path):
    repository, commit = harness
    root = tmp_path / "state"
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "suite.json").write_text("protected fixture")
    owned = await ExperimentWorktree(root).create(
        repository, (experiment_id := uuid4()), commit
    )
    baseline = stage_harness(owned.path, commit, root / "base", EDITABLE_PATHS)
    manifest = search_manifest().model_copy(
        update={
            "experiment_id": experiment_id,
            "initial_commit": commit,
            "initial_source_digest": baseline.source_digest,
            "boundary_suite_digest": suite_digest(suite),
        }
    )
    store = await ExperimentStore.open(root / "experiments.sqlite3")
    run = await store.create(manifest, "repo", "key")
    await store.register_snapshot(run.id, baseline, "base/snapshot.json")
    round = await store.begin_round(run.id, baseline.snapshot_id, run.revision)
    (owned.path / EDITABLE_PATHS[0]).write_text("value = 2\n")
    await owned.checkpoint_owned_edits()
    candidate = stage_harness(owned.path, commit, root / "candidate", EDITABLE_PATHS)
    await store.register_snapshot(run.id, candidate, "candidate/snapshot.json")
    await record_trials(store, root, manifest, baseline, 0)
    await record_trials(store, root, manifest, candidate, 1)
    checks_dir = root / "checks"
    checks_dir.mkdir()
    log = checks_dir / "checks.log.json"
    atomic_json(log, [{"test_id": id, "verdict": "pass"} for id in CHECK_IDS])
    checks = CandidateCheckResult(
        candidate.source_digest,
        candidate.export_digest,
        manifest.boundary_suite_digest,
        digest(
            [
                manifest.worker_image,
                manifest.worker_platform,
                manifest.dependency_lock_digest,
                manifest.controller_digest,
            ]
        ),
        "pass",
        "checks_passed",
        hashlib.sha256(log.read_bytes()).hexdigest(),
        True,
    )
    atomic_json(checks_dir / "result.json", checks)
    await store.record_event(
        run.id,
        "candidate_checks",
        {
            "path": "checks/result.json",
            "sha256": hashlib.sha256(
                (checks_dir / "result.json").read_bytes()
            ).hexdigest(),
        },
    )
    paths = ExperimentPaths(
        root=root, registry_root=tmp_path / "registry", repository=repository
    )
    service = PromotionService(store, paths, suite)
    yield service, store, paths, owned, round, baseline, candidate, checks
    await store.close()


async def record_trials(store, root, manifest, snapshot, dev_count):
    run = await store.get(manifest.experiment_id)
    for case in manifest.cases:
        if case.split == "final-test":
            continue
        for repeat in range(3):
            trial = await store.create_trial(
                run.id,
                snapshot.snapshot_id,
                case.case_id,
                repeat,
                trial_key(manifest, snapshot, case, repeat),
            )
            directory = root / "trials" / str(trial)
            directory.mkdir(parents=True)
            (directory / "trace.json").write_text("[]")
            (directory / "solution.diff").write_text("patch")
            sha = hashlib.sha256(b"patch").hexdigest()
            passed = repeat < dev_count if case.split == "development" else repeat > 0
            result = TrialResult(
                TrialIdentity(
                    trial_id=trial,
                    case_digest=case.case_digest,
                    harness_commit=snapshot.parent_commit,
                    harness_tree_digest=snapshot.source_digest,
                    model_config_digest=digest(manifest.model),
                    limits_digest=digest(manifest.trial_limits),
                    repeat_index=repeat,
                ),
                SolverResult(
                    "completed",
                    uuid4(),
                    uuid4(),
                    directory / "workspace",
                    directory / "trace.json",
                    directory / "usage.json",
                ),
                SolutionPatch(directory / "solution.diff", sha, "a" * 40, 5),
                GradeResult(
                    "pass" if passed else "fail",
                    "official_report",
                    evaluation_identity(
                        trial, case.case_digest, sha, manifest.grader.driver_digest
                    ),
                    passed,
                    sha,
                    directory / "report.json",
                    directory / "grade.log",
                    manifest.grader.driver_commit,
                    1.0,
                ),
                "complete",
                None,
            )
            (directory / "report.json").write_text("protected scripted report")
            (directory / "grade.log").write_text("protected scripted log")
            artifacts = {
                name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                for name in ("trace.json", "solution.diff", "report.json", "grade.log")
            }
            path = directory / "result.json"
            atomic_json(path, {"result": result, "artifacts": artifacts})
            await store.trial_state(
                trial,
                "complete",
                artifact=json.dumps(
                    {
                        "path": str(path.relative_to(root)),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    }
                ),
                verdict=result.grade.verdict,
                termination="completed",
            )


async def test_crash_after_git_commit_advances_once(promotion_setup, monkeypatch):
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.trials import build_evaluation_set

    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    before = await build_evaluation_set(
        store, paths, round.experiment_id, baseline.snapshot_id
    )
    after = await build_evaluation_set(
        store, paths, round.experiment_id, candidate.snapshot_id
    )
    decision = decide_candidate(before, after, checks)
    assert decision.verdict == "promote"
    await store.record_decision(
        round.id, decision, (await store.get(round.experiment_id)).revision
    )
    complete = store.complete_promotion

    async def crash(*args, **kwargs):
        raise RuntimeError("after git commit")

    monkeypatch.setattr(store, "complete_promotion", crash)
    with pytest.raises(RuntimeError, match="after git commit"):
        await service.promote(round.id, digest(decision))
    head = git(owned.path, "rev-parse", "HEAD")
    assert head != baseline.parent_commit
    monkeypatch.setattr(store, "complete_promotion", complete)
    promotion = (await store.promotions(round.experiment_id))[0]
    first = await service.reconcile_promotion(promotion.id)
    second = await service.promote(round.id, digest(decision))
    assert first == second
    assert first.state == "complete"
    assert first.commit_sha == head
    assert (await store.get(round.experiment_id)).last_accepted_commit == head
    assert git(paths.repository, "rev-parse", "HEAD") == baseline.parent_commit
    assert len(await store.events(round.experiment_id, "promotion_complete")) == 1


async def test_accepted_candidate_is_next_round_baseline(promotion_setup, tmp_path):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.manifest import manifest_digest
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.trials import build_evaluation_set

    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    before = await build_evaluation_set(
        store, paths, round.experiment_id, baseline.snapshot_id
    )
    after = await build_evaluation_set(
        store, paths, round.experiment_id, candidate.snapshot_id
    )
    decision = decide_candidate(before, after, checks)
    await store.record_decision(
        round.id, decision, (await store.get(round.experiment_id)).revision
    )
    first = await service.promote(round.id, digest(decision))
    run = await store.get(round.experiment_id)
    assert await store.accepted_snapshot_id(run.id) == candidate.snapshot_id
    next_round = await store.begin_round(run.id, candidate.snapshot_id, run.revision)
    assert next_round.baseline_commit == first.commit_sha
    accepted = await ExperimentWorktree(paths.root).create(
        paths.repository, run.id, run.initial_commit, run.last_accepted_commit
    )
    assert (await accepted.inspect()).changed_paths == ()
    view = await DevelopmentViewBuilder(store, paths.root).build_development_view(
        run.id, candidate.snapshot_id, tmp_path / "next-view"
    )
    assert view.source_digest == candidate.source_digest
    reused = await build_evaluation_set(store, paths, run.id, candidate.snapshot_id)
    assert reused == after
    assert reused.config_policy_digest == manifest_digest(run.manifest)
    (accepted.path / EDITABLE_PATHS[0]).write_text("value = 3\n")
    await accepted.checkpoint_owned_edits()
    second_candidate = stage_harness(
        accepted.path, run.last_accepted_commit, paths.root / "second", EDITABLE_PATHS
    )
    await store.register_snapshot(run.id, second_candidate, "second/snapshot.json")
    await record_trials(store, paths.root, run.manifest, second_candidate, 2)
    second_checks = replace(
        checks,
        snapshot_digest=second_candidate.source_digest,
        export_digest=second_candidate.export_digest,
    )
    check_root = paths.root / "second-checks"
    check_root.mkdir()
    atomic_json(
        check_root / "checks.log.json",
        [{"test_id": id, "verdict": "pass"} for id in CHECK_IDS],
    )
    atomic_json(check_root / "result.json", second_checks)
    await store.record_event(
        run.id,
        "candidate_checks",
        {
            "path": "second-checks/result.json",
            "sha256": hashlib.sha256(
                (check_root / "result.json").read_bytes()
            ).hexdigest(),
        },
    )
    second_eval = await build_evaluation_set(
        store, paths, run.id, second_candidate.snapshot_id
    )
    second_decision = decide_candidate(reused, second_eval, second_checks)
    assert second_decision.verdict == "promote"
    await store.record_decision(
        next_round.id, second_decision, (await store.get(run.id)).revision
    )
    second = await service.promote(next_round.id, digest(second_decision))
    assert git(accepted.path, "rev-parse", "HEAD^") == first.commit_sha
    assert second.commit_sha == (await store.get(run.id)).last_accepted_commit
    assert len(await store.promotions(run.id)) == 2
    assert git(paths.repository, "rev-parse", "HEAD") == baseline.parent_commit


async def save_promotable_decision(setup):
    from crucible.improvement.selection import decide_candidate
    from crucible.improvement.trials import build_evaluation_set

    service, store, paths, owned, round, baseline, candidate, checks = setup
    before = await build_evaluation_set(
        store, paths, round.experiment_id, baseline.snapshot_id
    )
    after = await build_evaluation_set(
        store, paths, round.experiment_id, candidate.snapshot_id
    )
    decision = decide_candidate(before, after, checks)
    await store.record_decision(
        round.id, decision, (await store.get(round.experiment_id)).revision
    )
    return decision


@pytest.mark.parametrize(
    "mutation", ["decision", "bytes", "branch", "evidence", "index"]
)
async def test_promotion_refuses_drift(promotion_setup, mutation):
    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    decision = await save_promotable_decision(promotion_setup)
    decision_hash = digest(decision)
    if mutation == "decision":
        decision_hash = "f" * 64
    elif mutation == "bytes":
        (owned.path / EDITABLE_PATHS[0]).write_text("user edit")
    elif mutation == "branch":
        git(owned.path, "checkout", "-qb", "unexpected")
    elif mutation == "index":
        blob = git(owned.path, "rev-parse", "HEAD:" + EDITABLE_PATHS[0])
        git(
            owned.path,
            "update-index",
            "--cacheinfo",
            "100644",
            blob,
            "backend/src/crucible/protected.py",
        )
    else:
        (paths.root / "checks/checks.log.json").write_text("changed")
    with pytest.raises(ValueError):
        await service.promote(round.id, decision_hash)
    assert (
        await store.get(round.experiment_id)
    ).last_accepted_commit == baseline.parent_commit
    assert git(paths.repository, "rev-parse", "HEAD") == baseline.parent_commit


@pytest.mark.parametrize("when", ["before_git", "after_db"])
async def test_promotion_crash_checkpoints_are_idempotent(
    promotion_setup, monkeypatch, when
):
    import crucible.improvement.promotion as module

    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    decision = await save_promotable_decision(promotion_setup)
    git_call, complete = module.git_bytes, store.complete_promotion

    def crash_before(root, *args, **kwargs):
        if "commit-tree" in args:
            raise RuntimeError("before git")
        return git_call(root, *args, **kwargs)

    async def crash_after(*args, **kwargs):
        await complete(*args, **kwargs)
        raise RuntimeError("after db")

    if when == "before_git":
        monkeypatch.setattr(module, "git_bytes", crash_before)
    else:
        monkeypatch.setattr(store, "complete_promotion", crash_after)
    with pytest.raises(RuntimeError):
        await service.promote(round.id, digest(decision))
    monkeypatch.setattr(module, "git_bytes", git_call)
    monkeypatch.setattr(store, "complete_promotion", complete)
    result = await service.promote(round.id, digest(decision))
    assert result.state == "complete"
    assert (
        git(owned.path, "rev-list", "--count", baseline.parent_commit + "..HEAD") == "1"
    )
    assert len(await store.events(round.experiment_id, "promotion_complete")) == 1


async def test_extra_commit_after_crash_is_not_adopted(promotion_setup, monkeypatch):
    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    decision = await save_promotable_decision(promotion_setup)
    complete = store.complete_promotion

    async def crash(*args, **kwargs):
        raise RuntimeError("crash")

    monkeypatch.setattr(store, "complete_promotion", crash)
    with pytest.raises(RuntimeError):
        await service.promote(round.id, digest(decision))
    git(
        owned.path,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@localhost",
        "commit",
        "--allow-empty",
        "-qm",
        "unexpected",
    )
    monkeypatch.setattr(store, "complete_promotion", complete)
    promotion = (await store.promotions(round.experiment_id))[0]
    with pytest.raises(ValueError, match="ambiguous"):
        await service.reconcile_promotion(promotion.id)
    assert (
        await store.get(round.experiment_id)
    ).last_accepted_commit == baseline.parent_commit


async def test_duplicate_generations_are_not_silently_selected(promotion_setup):
    from crucible.improvement.trials import build_evaluation_set

    service, store, paths, owned, round, baseline, candidate, checks = promotion_setup
    manifest = (await store.get(round.experiment_id)).manifest
    case = next(c for c in manifest.cases if c.split == "development")
    await store.create_trial(
        round.experiment_id,
        candidate.snapshot_id,
        case.case_id,
        0,
        trial_key(manifest, candidate, case, 0),
    )
    with pytest.raises(ValueError, match="ambiguous trial generations"):
        await build_evaluation_set(
            store, paths, round.experiment_id, candidate.snapshot_id
        )
