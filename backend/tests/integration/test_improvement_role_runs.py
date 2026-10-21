"""Scripted outputs test role retry policy, not measured isolation or quality."""

import pytest

from crucible.improvement.roles import parse_mining_output

pytest_plugins = ["tests.unit.test_improvement_evidence_views"]


def test_valid_no_action_has_no_retry():
    result = parse_mining_output(
        '{"schema_version":1,"disposition":"no_actionable_weakness",'
        '"weaknesses":[],"summary":"No supported change."}',
        (),
    )
    assert result.disposition == "no_actionable_weakness"


def test_markdown_fences_are_invalid_output():
    with pytest.raises(ValueError):
        parse_mining_output('```json\n{"schema_version":1}\n```', ())


async def test_valid_no_action_cannot_request_correction(tmp_path, evidence_setup):
    import json
    from uuid import uuid4

    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.roles import RoleExecution, RoleRunner

    store, root, run, snapshot, ids = evidence_setup
    round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
    builder = DevelopmentViewBuilder(store, root)
    view = await builder.build_development_view(
        run.id, snapshot.snapshot_id, tmp_path / "view"
    )
    output = root / "no-action.json"
    output.write_text(
        json.dumps(
            dict(
                schema_version=1,
                disposition="no_actionable_weakness",
                weaknesses=[],
                summary="No action",
            )
        )
    )
    execution = RoleExecution(
        role_id=uuid4(),
        job_id=uuid4(),
        experiment_id=run.id,
        round_id=round.id,
        kind="mining",
        accepted_snapshot_id=snapshot.snapshot_id,
        source_digest=snapshot.source_digest,
        input_digest="a" * 64,
        input_ref="input.json",
        output_ref="no-action.json",
        output_digest="b" * 64,
        status="complete",
    )
    await store.save_role(execution, 0, (await store.get(run.id)).revision)
    runner = RoleRunner(store, root, None, builder)
    with pytest.raises(ValueError, match="invalid mining"):
        await runner.mine(round.id, view, correction_of=execution.role_id)


@pytest.mark.parametrize(
    "output,expected",
    [
        (
            '{"schema_version":1,"disposition":"no_actionable_weakness",'
            '"weaknesses":[],"summary":"No action"}',
            "complete",
        ),
        ("not json", "invalid"),
    ],
)
async def test_mining_checkpoints_original_output(
    evidence_setup, tmp_path, output, expected
):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.roles import RoleRunner
    from crucible.improvement.worker_protocol import (
        IsolatedRoleExecution,
        RoleObservation,
    )
    from crucible.repair.gateway import TrustedUsage

    store, root, run, snapshot, ids = evidence_setup
    round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
    views = DevelopmentViewBuilder(store, root)
    view = await views.build_development_view(
        run.id, snapshot.snapshot_id, tmp_path / "view"
    )
    jobs = []

    class Supervisor:
        async def execute(self, job, accepted, workspace, *, harness_export):
            jobs.append(job)
            state = tmp_path / str(job.job_id)
            state.mkdir()
            (state / "assistant.txt").write_text(output)
            (state / "trace.json").write_text("[]")
            observation = RoleObservation(
                schema_version=2,
                job_id=job.job_id,
                role_id=job.role_id,
                round_id=job.round_id,
                experiment_id=job.experiment_id,
                export_digest=job.export_digest,
                termination="completed",
                assistant_output_ref="assistant.txt",
                trace_ref="trace.json",
                detail_code=None,
            )
            return IsolatedRoleExecution(
                observation, workspace, state, TrustedUsage(), (), True, 1.0
            )

    runner = RoleRunner(store, root, Supervisor(), views)
    result = await runner.mine(round.id, view)
    assert result.status == expected
    assert (root / result.output_ref).read_text() == output
    assert len(jobs) == 1
    with pytest.raises(ValueError):
        await runner.mine(round.id, view)
    if expected == "complete":
        with pytest.raises(ValueError, match="invalid mining"):
            await runner.mine(round.id, view, correction_of=result.role_id)
    else:
        corrected = await runner.mine(round.id, view, correction_of=result.role_id)
        assert corrected.status == "invalid"
        assert len(jobs) == 2
        with pytest.raises(ValueError, match="correction"):
            await runner.mine(round.id, view, correction_of=result.role_id)


async def test_role_uses_ordinary_engine_with_read_only_registry(database, tmp_path):
    from crucible.application.worker_container import create_worker_engine
    from crucible.domain.run import RunStatus
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.improvement.worker import seed_task
    from tests.contract.test_improvement_roles import role_job

    job = role_job()
    job = job.model_copy(
        update={
            "model": job.model.model_copy(update={"input_limit": 20000}),
            "limits": job.limits.model_copy(update={"max_model_tokens": 20000}),
        }
    )
    job_path = tmp_path / "request.json"
    job_path.write_text(job.model_dump_json())
    requests = []

    class Gateway:
        async def stream(self, request):
            requests.append(request)
            yield TextDelta(
                '{"schema_version":1,"disposition":"no_actionable_weakness",'
                '"weaknesses":[],"summary":"No action"}'
            )
            yield ModelUsage(10, 10)
            yield ModelStop(ModelStopReason.COMPLETE)

    engine, uow = create_worker_engine(
        database,
        job,
        Gateway(),
        "test-only",
        state_root=tmp_path / "state",
        job_path=job_path,
    )
    run_id = await seed_task(
        job.job_id,
        "Analyze the supplied evidence.",
        job.harness_digest,
        job.command_image,
        uow,
    )
    assert await engine.execute(run_id)
    async with uow() as unit:
        run = await unit.runs.get(run_id)
    assert run.status == RunStatus.COMPLETED, run.outcome_code
    assert run.settings_snapshot.sandbox_image == job.command_image
    assert len(requests) == 1
    assert {tool.name for tool in requests[0].tools} == {
        "list_files",
        "read_file",
        "read_file_chunk",
        "search_files",
    }


async def test_partial_proposal_transfer_is_owned_but_user_edit_is_not(tmp_path):
    import hashlib
    from uuid import uuid4

    from crucible.benchmarks.evidence import atomic_json
    from crucible.improvement.contracts import EDITABLE_PATHS
    from crucible.improvement.source import ScopeRejected
    from crucible.improvement.worktree import ExperimentWorktree
    from tests.unit.test_improvement_source import git

    repository = tmp_path / "repo"
    repository.mkdir()
    git(repository, "init", "-q")
    path = repository / EDITABLE_PATHS[0]
    path.parent.mkdir(parents=True)
    path.write_text("before")
    git(repository, "add", "--", EDITABLE_PATHS[0])
    git(
        repository,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@localhost",
        "commit",
        "-qm",
        "initial",
    )
    commit = git(repository, "rev-parse", "HEAD")
    owned = await ExperimentWorktree(tmp_path / "state").create(
        repository, uuid4(), commit
    )
    original = await owned.inspect()
    journal = {
        "state": "intent",
        "before": {
            name: [mode, hashlib.sha256(data).hexdigest()]
            for name, (mode, data) in original.files.items()
        },
        "after": {EDITABLE_PATHS[0]: [0o100644, hashlib.sha256(b"after").hexdigest()]},
    }
    atomic_json(owned.root / "proposal-transfer.json", journal)
    target = owned.path / EDITABLE_PATHS[0]
    target.write_text("user edit")
    with pytest.raises(ScopeRejected, match="unexpected edit"):
        await owned.reconcile_proposal_transfer()
    assert target.read_text() == "user edit"
    target.write_text("after")
    await owned.reconcile_proposal_transfer()
    await owned.archive_interrupted(uuid4())
    await owned.restore_last_accepted(original.source_digest)
    assert target.read_text() == "before"


@pytest.mark.parametrize("after_replace", [False, True])
async def test_interrupted_proposal_file_replacement_is_recoverable(
    tmp_path, monkeypatch, after_replace
):
    import hashlib
    import json
    import os
    from uuid import uuid4

    from crucible.improvement.contracts import EDITABLE_PATHS
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.roles import RoleExecution, RoleRunner
    from crucible.improvement.source import inspect_source, stage_harness
    from crucible.improvement.store import ExperimentStore
    from crucible.improvement.worktree import ExperimentWorktree
    from tests.unit.test_improvement_search_store import search_manifest
    from tests.unit.test_improvement_source import git

    repository = tmp_path / "repo"
    repository.mkdir()
    git(repository, "init", "-q")
    source = repository / EDITABLE_PATHS[0]
    source.parent.mkdir(parents=True)
    source.write_text("before")
    git(repository, "add", "--", EDITABLE_PATHS[0])
    git(
        repository,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@localhost",
        "commit",
        "-qm",
        "initial",
    )
    commit = git(repository, "rev-parse", "HEAD")
    root = tmp_path / "state"
    manifest = search_manifest().model_copy(
        update={
            "initial_commit": commit,
            "initial_source_digest": inspect_source(repository, commit).source_digest,
        }
    )
    store = await ExperimentStore.open(root / "experiments.sqlite3")
    try:
        run = await store.create(manifest, "repo", "key")
        owned = await ExperimentWorktree(root).create(repository, run.id, commit)
        snapshot = stage_harness(owned.path, commit, root / "base", EDITABLE_PATHS)
        await store.register_snapshot(run.id, snapshot, "base/snapshot.json")
        round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
        role_id = uuid4()
        directory = root / "roles" / str(role_id)
        workspace = directory / "workspace"
        proposed = workspace / "source" / EDITABLE_PATHS[0]
        proposed.parent.mkdir(parents=True)
        proposed.write_text("after")
        original = {"source/" + EDITABLE_PATHS[0]: [0o100644, b"before".hex()]}
        (directory / "workspace-original.json").write_text(json.dumps(original))
        output = directory / "assistant.txt"
        output.write_text("{}")
        execution = RoleExecution(
            role_id=role_id,
            job_id=uuid4(),
            experiment_id=run.id,
            round_id=round.id,
            kind="proposal",
            accepted_snapshot_id=snapshot.snapshot_id,
            source_digest=snapshot.source_digest,
            input_digest="b" * 64,
            input_ref="input.json",
            output_digest=hashlib.sha256(output.read_bytes()).hexdigest(),
            output_ref=str(output.relative_to(root)),
            status="complete",
        )
        await store.save_role(execution, 0, (await store.get(run.id)).revision)
        target = owned.path / EDITABLE_PATHS[0]
        replace = os.replace

        def interrupt(source, destination):
            if destination == target:
                if after_replace:
                    replace(source, destination)
                raise RuntimeError("simulated crash")
            return replace(source, destination)

        monkeypatch.setattr(os, "replace", interrupt)
        runner = RoleRunner(store, root, None, DevelopmentViewBuilder(store, root))
        with pytest.raises(RuntimeError, match="simulated crash"):
            await runner.collect_proposal(execution, owned)
        assert target.read_bytes() == (b"after" if after_replace else b"before")
        monkeypatch.setattr(os, "replace", replace)
        await owned.reconcile_proposal_transfer()
        await owned.archive_interrupted(uuid4())
        await owned.restore_last_accepted(snapshot.source_digest)
        assert target.read_bytes() == b"before"
    finally:
        await store.close()


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "legacy",
        "source",
        "nested_git",
        "extra",
        "git",
        "job_kind",
        "usage",
        "dispatched",
        "provider",
    ],
)
async def test_predispatch_mining_failure_recovers_original_identity(
    evidence_setup, tmp_path, drift
):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder
    from crucible.improvement.roles import RoleRunner
    from crucible.improvement.worker_protocol import (
        IsolatedRoleExecution,
        RoleObservation,
    )
    from crucible.repair.gateway import TrustedUsage

    store, root, run, snapshot, _ = evidence_setup
    round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
    views = DevelopmentViewBuilder(store, root)
    view = await views.build_development_view(
        run.id, snapshot.snapshot_id, tmp_path / "view"
    )
    jobs = []

    class Supervisor:
        async def execute(self, job, accepted, workspace, *, harness_export):
            jobs.append(job)
            if len(jobs) == 1 and drift != "provider":
                raise OSError("image inspection unavailable before dispatch")
            state = tmp_path / "result"
            state.mkdir()
            (state / "assistant.txt").write_text(
                '{"schema_version":1,"disposition":"no_actionable_weakness",'
                '"weaknesses":[],"summary":"No action"}'
            )
            (state / "trace.json").write_text("[]")
            observation = RoleObservation(
                schema_version=2,
                job_id=job.job_id,
                role_id=job.role_id,
                round_id=job.round_id,
                experiment_id=job.experiment_id,
                export_digest=job.export_digest,
                termination="provider_error" if drift == "provider" else "completed",
                assistant_output_ref="assistant.txt",
                trace_ref="trace.json",
                detail_code=None,
            )
            return IsolatedRoleExecution(
                observation, workspace, state, TrustedUsage(), (), True, 1.0
            )

    runner = RoleRunner(store, root, Supervisor(), views)
    failed = await runner.mine(round.id, view)
    with pytest.raises(ValueError):
        await runner.recover_mining(failed, view)
    directory = root / "roles" / str(failed.role_id)
    if drift == "legacy":
        (directory / "job.json").unlink()
        (directory / "mining-git.json").unlink()
        (directory / "workspace" / ".git" / "private-canary").write_text(
            "private history"
        )
    elif drift == "source":
        source = next((directory / "workspace" / "source").rglob("*.py"))
        source.write_text("# unexpected edit")
    elif drift == "nested_git":
        nested = directory / "workspace" / "source" / ".git"
        nested.mkdir()
        (nested / "private-canary").write_text("private history")
    elif drift == "git":
        (directory / "workspace" / ".git" / "private-canary").write_text(
            "private history"
        )
    elif drift == "extra":
        (directory / "workspace" / "extra.txt").write_text("canary")
    elif drift == "job_kind":
        import json

        path = directory / "job.json"
        payload = json.loads(path.read_bytes())
        payload["job_kind"] = "proposal"
        path.write_text(json.dumps(payload))
    elif drift == "dispatched":
        (root / "jobs" / str(failed.job_id)).mkdir(parents=True)
    record = await store.request_stop(run.id)
    owner = await store.claim_recovery(run.id, record.revision, None, 123, None)
    await store.recover_ready(run.id, owner)
    if drift == "usage":
        from crucible.improvement.contracts import UsageAmounts

        reservation = await store.reserve_usage(
            run.id, None, UsageAmounts(tokens=1), kind="model", job_id=failed.job_id
        )
        await store.settle_usage(reservation, UsageAmounts(tokens=1))
    if drift and drift != "legacy":
        with pytest.raises(ValueError):
            await runner.recover_mining(failed, view)
        assert len(jobs) == 1
        assert (await store.roles(round.id))[0].execution == failed
        return
    from types import SimpleNamespace

    from crucible.improvement.search import LocalSearchExecution

    local = LocalSearchExecution(
        store,
        SimpleNamespace(root=root),
        tmp_path,
        runner,
        None,
        views,
        None,
        None,
        None,
        tmp_path / "recovered-view",
    )
    mining = await local.mine(round)
    assert mining.disposition == "no_actionable_weakness"
    result = (await store.roles(round.id))[0].execution
    assert result.status == "complete"
    assert result.role_id == failed.role_id
    assert jobs[0] == jobs[1]
    assert len(await store.roles(round.id)) == 1
    assert (await store.round(round.id)).mining_attempts == 1
    if drift == "legacy":
        assert not (directory / "workspace" / ".git" / "private-canary").exists()
        assert list(directory.glob("archived-git-*/private-canary"))
