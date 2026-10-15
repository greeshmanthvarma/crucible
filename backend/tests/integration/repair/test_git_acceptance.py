"""Real Git boundaries; all repositories and incidents are disposable fixtures."""

import subprocess
from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.domain.repair import RepairAcceptance
from crucible.repair.controller import RepairNeedsReview
from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.git import LocalRepairGit
from tests.integration.repair.test_admission import environment

EDITABLE = "backend/src/crucible/context/selection.py"


async def git_environment(database, tmp_path):
    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "start")
    repair = await service.get(result.repair_id)
    root = tmp_path / "repo"
    root.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-qb", f"repair/{repair.id}")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.test")
    path = root / EDITABLE
    path.parent.mkdir(parents=True)
    path.write_text("# original\n")
    git("add", ".")
    git("commit", "-qm", "baseline")
    repair = replace(
        repair,
        worktree_path=str(root),
        branch=f"repair/{repair.id}",
        last_accepted_commit=git("rev-parse", "HEAD"),
    )
    async with factory() as uow:
        await uow.repairs.update(repair)
        await uow.commit()
    return repair, factory, root, git


async def intent_for(repair, root, git):
    # The caller has already saved this evaluated identity as acceptance intent.
    source = await capture_source_identity(root)
    adapter = LocalRepairGit(root, root.parent)
    tree = await adapter.candidate_tree(repair)
    return RepairAcceptance(
        uuid4(),
        repair.id,
        uuid4(),
        repair.last_accepted_commit,
        tree,
        source.tree_digest,
        {"worktree_path": str(root), "branch": repair.branch},
        "intent",
    )


async def test_commit_reconciles_once_and_preserves_sequential_parent(
    database, tmp_path
):
    repair, _, root, git = await git_environment(database, tmp_path)
    adapter = LocalRepairGit(root, tmp_path)
    (root / EDITABLE).write_text("# accepted A\n")
    intent = await intent_for(repair, root, git)
    commit = await adapter.accept(intent)
    assert await adapter.reconcile_acceptance(intent) == commit
    assert await adapter.accept(intent) == commit
    assert git("rev-list", "--count", "HEAD") == "2"
    assert git("show", f"{commit}:{EDITABLE}") == "# accepted A"
    assert git("show", "-s", "--format=%P", commit) == repair.last_accepted_commit
    assert str(repair.id) in git("show", "-s", "--format=%B", commit)
    repair = replace(repair, last_accepted_commit=commit)
    (root / EDITABLE).write_text("# accepted C\n")
    next_intent = await intent_for(repair, root, git)
    next_commit = await adapter.accept(next_intent)
    assert git("show", "-s", "--format=%P", next_commit) == commit


@pytest.mark.parametrize("staged", [False, True])
async def test_unknown_edits_never_get_committed_or_overwritten(
    database, tmp_path, staged
):
    repair, _, root, git = await git_environment(database, tmp_path)
    (root / EDITABLE).write_text("# candidate\n")
    intent = await intent_for(repair, root, git)
    (root / "user.txt").write_text("USER_SENTINEL")
    if staged:
        git("add", "user.txt")
    with pytest.raises(RepairNeedsReview):
        await LocalRepairGit(root, tmp_path).accept(intent)
    assert (root / "user.txt").read_text() == "USER_SENTINEL"
    assert git("rev-parse", "HEAD") == repair.last_accepted_commit


async def test_changed_allowed_file_and_wrong_commit_marker_require_review(
    database, tmp_path
):
    repair, _, root, git = await git_environment(database, tmp_path)
    (root / EDITABLE).write_text("# candidate\n")
    intent = await intent_for(repair, root, git)
    (root / EDITABLE).write_text("# USER_EDIT\n")
    adapter = LocalRepairGit(root, tmp_path)
    with pytest.raises(RepairNeedsReview):
        await adapter.accept(intent)
    git("add", EDITABLE)
    git("commit", "-qm", "unrelated user commit")
    with pytest.raises(RepairNeedsReview):
        await adapter.reconcile_acceptance(intent)
    assert (root / EDITABLE).read_text() == "# USER_EDIT\n"


@pytest.mark.parametrize(
    "failure_point", ["intent", "intent_drift", "commit", "finalize"]
)
async def test_saved_acceptance_recovers_commit_before_database_without_model_retry(
    database, tmp_path, failure_point
):
    from crucible.repair.git import RepairAcceptanceService
    from crucible.repair.incident_suite import IncidentSuite
    from tests.integration.repair.test_cycle import cycle_environment
    from tests.integration.storage.test_tool_loop_storage import CLOCK

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path)

    class LostCommitResponse(LocalRepairGit):
        lost = False

        async def accept(self, intent):
            if not self.lost and failure_point in {"intent", "intent_drift"}:
                self.lost = True
                raise OSError("intent saved before commit")
            commit = await super().accept(intent)
            if not self.lost and failure_point == "commit":
                self.lost = True
                raise OSError("commit response lost")
            return commit

    incidents = IncidentSuite(
        controller.evaluation.data_dir / "repair-incidents",
        factory,
        controller.artifacts,
        CLOCK,
        fixtures={},
    )

    class LostFinalizationResponse(RepairAcceptanceService):
        lost = False

        async def accept(self, *args):
            commit = await super().accept(*args)
            if not self.lost and failure_point == "finalize":
                self.lost = True
                raise OSError("finalization response lost")
            return commit

    controller.acceptance = LostFinalizationResponse(
        factory,
        LostCommitResponse(root, tmp_path),
        controller.artifacts,
        CLOCK,
        incidents,
    )
    await controller.execute(repair.id)
    failed = await service.get(repair.id)
    assert failed.status == "retryable_error"
    count = len(provider.requests)
    async with factory() as uow:
        intents = await uow.repairs.list_acceptances(repair.id)
    assert len(intents) == 1
    assert intents[0].status == (
        "accepted" if failure_point == "finalize" else "intent"
    )
    observed = await controller.acceptance.git.reconcile_acceptance(intents[0])
    assert (observed is None) == failure_point.startswith("intent")
    if failure_point == "intent_drift":
        controller.roles.configuration = {"model": "changed-model"}
    await service.command(repair.source_task_id, "/repair resume", "resume")
    await controller.execute(repair.id)
    current = await service.get(repair.id)
    if failure_point == "intent_drift":
        assert current.status == "invalidated"
        assert await controller.acceptance.git.reconcile_acceptance(intents[0]) is None
        assert len(provider.requests) == count
        return
    assert (
        current.stage == "publication" and current.outcome_code == "publication_pending"
    )
    assert len(provider.requests) == count
    records = await incidents.list_accepted()
    assert (
        len(records) == 1 and records[0].accepted_commit == current.last_accepted_commit
    )
    async with factory() as uow:
        assert (await uow.repairs.list_acceptances(repair.id))[0].status == "accepted"
        assert (await uow.repairs.list_fixes(repair.id))[0].status == "accepted"
    assert (root / editable).read_text() == "# candidate one\n"


@pytest.mark.parametrize("change", ["delete", "new", "staged"])
async def test_full_change_scan_accepts_exact_allowed_changes(
    database, tmp_path, change
):
    repair, _, root, git = await git_environment(database, tmp_path)
    if change == "delete":
        (root / EDITABLE).unlink()
    elif change == "new":
        new = root / "backend/src/crucible/context/summary_behavior.py"
        new.write_text("# new allowed module\n")
    else:
        (root / EDITABLE).write_text("# staged allowed\n")
        git("add", EDITABLE)
    intent = await intent_for(repair, root, git)
    commit = await LocalRepairGit(root, tmp_path).accept(intent)
    assert git("status", "--porcelain") == ""
    assert git("show", "-s", "--format=%T", commit) == intent.candidate_tree_digest


@pytest.mark.parametrize("change", ["delete", "new", "edit"])
async def test_rejected_patch_restores_only_b_after_accepted_a(
    database, tmp_path, change
):
    from crucible.application.artifact_service import ArtifactService
    from crucible.artifacts.store import LocalArtifactStore
    from crucible.domain.repair import RepairAttempt, RepairCheckpoint
    from crucible.repair.recovery import ManagedRepairWorkspace
    from tests.integration.storage.test_tool_loop_storage import CLOCK

    repair, factory, root, git = await git_environment(database, tmp_path)
    (root / EDITABLE).write_text("# accepted A\n")
    intent = await intent_for(repair, root, git)
    commit = await LocalRepairGit(root, tmp_path).accept(intent)
    repair = replace(repair, last_accepted_commit=commit, current_fix_id=None)
    target = root / EDITABLE
    if change == "delete":
        target.unlink()
    elif change == "new":
        target = root / "backend/src/crucible/context/summary_behavior.py"
        target.write_text("# rejected B\n")
    else:
        target.write_text("# rejected B\n")
    source = await capture_source_identity(root)
    attempt_id = uuid4()
    async with factory() as uow:
        await uow.repairs.update(repair)
        await uow.repairs.add_attempt(
            RepairAttempt(
                attempt_id,
                repair.id,
                None,
                "proposal",
                1,
                None,
                "completed",
                True,
                CLOCK.now(),
            )
        )
        await uow.repairs.add_checkpoint(
            RepairCheckpoint(
                uuid4(),
                repair.id,
                "proposal",
                None,
                None,
                "test",
                commit,
                {},
                {
                    "owned_mutation": {
                        "attempt_id": str(attempt_id),
                        "source_digest": source.tree_digest,
                    }
                },
                CLOCK.now(),
            )
        )
        await uow.commit()
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )
    workspace = ManagedRepairWorkspace(factory, artifacts)
    saved = await workspace.snapshot(repair)
    await workspace.restore(repair, saved)
    assert (root / EDITABLE).read_text() == "# accepted A\n"
    assert git("rev-parse", "HEAD") == commit
    assert git("status", "--porcelain") == ""


async def test_private_incident_reference_recovers_before_database_finalize(
    database, tmp_path
):
    from crucible.repair.identity import canonical_json
    from crucible.repair.incident_suite import IncidentSuite
    from crucible.repair.trials import fixture_manifest, private_artifact
    from tests.integration.repair.test_trials import approved_case, environment
    from tests.integration.storage.test_tool_loop_storage import CLOCK

    env = await environment(database, tmp_path)
    case = approved_case(env)
    definition = await private_artifact(
        env["artifacts"],
        case.case.incident_ids["task"],
        case.model_dump_json().encode(),
    )
    fixture = await private_artifact(
        env["artifacts"],
        case.case.incident_ids["task"],
        canonical_json(fixture_manifest(env["fixture"])),
    )
    root = tmp_path / "incidents"
    root.mkdir(mode=0o700)
    (root / (case.case_digest + ".json")).write_bytes(
        canonical_json(
            {
                "case_digest": case.case_digest,
                "definition_artifact_id": definition,
                "fixture_manifest_artifact_id": fixture,
                "accepted_commit": "a" * 40,
            }
        )
    )
    registry = IncidentSuite(root, env["factory"], env["artifacts"], CLOCK, fixtures={})
    first = await registry.register(case, "a" * 40)
    assert first.definition_artifact_id == definition
    assert await registry.register(case, "a" * 40) == first
    assert len(await registry.list_accepted()) == 1


async def test_stop_after_owned_staging_restores_and_resume_reproposes(
    database, tmp_path
):
    import asyncio

    from crucible.repair.git import RepairAcceptanceService
    from crucible.repair.incident_suite import IncidentSuite
    from tests.integration.repair.test_cycle import cycle_environment
    from tests.integration.storage.test_tool_loop_storage import CLOCK

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path)
    entered = asyncio.Event()

    class PausedStaging(LocalRepairGit):
        pause = True

        async def _stage(self, root, paths, index=None):
            await super()._stage(root, paths, index)
            if index is None and self.pause:
                entered.set()
                await asyncio.Future()

    git = PausedStaging(root, tmp_path)
    incidents = IncidentSuite(
        controller.evaluation.data_dir / "repair-incidents",
        factory,
        controller.artifacts,
        CLOCK,
        fixtures={},
    )
    controller.acceptance = RepairAcceptanceService(
        factory, git, controller.artifacts, CLOCK, incidents
    )
    service.bind_scheduler(controller)
    await controller.submit(repair.id)
    await asyncio.wait_for(entered.wait(), 30)
    await service.stop(repair.id, "stop-staged")
    assert (root / editable).read_text() == "# original\n"
    async with factory() as uow:
        assert (await uow.repairs.list_acceptances(repair.id))[0].status == "abandoned"
    git.pause = False
    await service.command(repair.source_task_id, "/repair resume", "resume-staged")
    if execution := controller.active.get(repair.id):
        await execution
    assert (await service.get(repair.id)).stage == "publication"
    async with factory() as uow:
        assert (
            len(
                [
                    a
                    for a in await uow.repairs.list_acceptances(repair.id)
                    if a.status == "accepted"
                ]
            )
            == 1
        )
