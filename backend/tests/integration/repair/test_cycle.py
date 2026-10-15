from crucible.repair.controller import RepairController
from tests.integration.repair.test_roles import role_environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


async def test_no_change_completes_without_proposal_or_publication(database, tmp_path):
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(
        database,
        tmp_path,
        [
            {
                "decision": "no_change_needed",
                "reasons": ["No harness defect"],
                "fix_items": [],
            }
        ],
    )
    repair = (await service.list_for_task(task.id))[0]
    controller = RepairController(factory, CLOCK, roles, incident, artifacts)
    await controller.execute(repair.id)
    run = await service.get(repair.id)
    assert run.status == "completed" and run.outcome_code == "no_change_needed"
    assert run.outcome_detail == "No harness defect"
    assert len(provider.requests) == 1
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
        assert await uow.repairs.get_publication(repair.id) is None


async def cycle_environment(
    database, tmp_path, *, heldout_failure=False, acceptance=None
):
    import asyncio
    import json
    import shutil
    import subprocess
    from dataclasses import replace
    from uuid import uuid4

    from crucible.domain.repair import RepairStage
    from crucible.engine.gateway import CompleteToolCall, ModelStop, ModelStopReason
    from crucible.evals.evaluators import EvaluatorRegistry
    from crucible.evals.fixtures import FixturePreparer
    from crucible.repair.evaluation import RepairEvaluation
    from crucible.repair.gateway import ScopedModelGateway, TrustedUsage
    from crucible.repair.identity import RuntimeIdentity
    from crucible.repair.recovery import ManagedRepairWorkspace
    from crucible.repair.worker_protocol import WorkerObservation
    from crucible.repair.worker_supervisor import WorkerExecution

    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(database, tmp_path, [])
    repair = (await service.list_for_task(task.id))[0]
    root = tmp_path / "source"
    editable = "backend/src/crucible/context/selection.py"
    (root / editable).parent.mkdir(parents=True)
    (root / editable).write_text("# original\n")

    def git(*args, cwd=root):
        return subprocess.run(
            ["git", *map(str, args)],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-qb", "repair/test")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.test")
    git("add", ".")
    git("commit", "-qm", "baseline")
    parent = git("rev-parse", "HEAD")
    async with factory() as uow:
        repair = replace(repair, last_accepted_commit=parent, stage=RepairStage.INTAKE)
        await uow.repairs.update(repair)
        await uow.commit()
    case = {
        "schema_version": 1,
        "case_id": "custom",
        "incident_ids": {"task": str(task.id), "run": None},
        "kind": "tool",
        "replay_mode": "direct",
        "fixture_manifest_digest": "controller-fills",
        "settings": {},
        "expected_baseline_failure": {"status": "error", "error_code": "missing"},
        "candidate_assertions": [{"op": "output_text_contains", "value": "fixed"}],
        "expected_behavior": "A focused synthetic read should return the literal.",
        "evidence_ids": [],
        "tool_name": "read_file",
        "arguments": {"path": "note.txt"},
    }
    async with factory() as uow:
        case["incident_ids"]["run"] = str((await uow.runs.list_for_task(task.id))[0].id)
    diagnosis = {
        "decision": "repairable",
        "reasons": ["Synthetic fixture plumbing"],
        "fix_items": [
            {
                "mechanism": "literal formatting",
                "proposed_case": case,
                "fixture_files": {"note.txt": "fixture"},
            }
        ],
    }
    review = {
        "decision": "approve",
        "reasons": ["Grounded synthetic expectation"],
        "evidence_ids": [],
        "replay_assessment": "direct",
    }

    def proposal(request):
        prompt = next(
            json.loads(part.text_content)
            for message in request.messages
            if message.role == "user"
            for part in message.parts
            if part.text_content and '"result_schema"' in part.text_content
        )
        fix_id = prompt["inputs"][0]["fix_id"]
        return {
            "fix_id": fix_id,
            "hypothesis": "Return the literal",
            "mechanism": "formatting",
            "declared_paths": [editable],
            "regression_risks": [],
        }

    def write(text):
        return [
            CompleteToolCall(
                uuid4(), "write_file", {"path": editable, "content": text}
            ),
            ModelStop(ModelStopReason.TOOL_CALLS),
        ]

    provider.outputs = [
        diagnosis,
        review,
        write("# candidate one\n"),
        proposal,
        write("# candidate two\n"),
        proposal,
        write("# candidate three\n"),
        proposal,
    ]
    main = tmp_path / "main"
    for partition, name in (
        ("development", "development"),
        ("held-out", "heldout_private_case"),
    ):
        selected = main / "evals" / partition
        fixture = selected / "fixtures" / "repo"
        fixture.mkdir(parents=True)
        (fixture / "required.txt").write_text("present")
        git("init", "-q", cwd=fixture)
        git("config", "user.name", "Test", cwd=fixture)
        git("config", "user.email", "test@example.test", cwd=fixture)
        git("add", ".", cwd=fixture)
        git("commit", "-qm", "fixture", cwd=fixture)
        revision = git("rev-parse", "HEAD", cwd=fixture)
        (selected / "cases").mkdir()
        prompt = "HELDOUT_SENTINEL" if partition == "held-out" else "visible task"
        (selected / "cases" / f"{name}.toml").write_text(
            f'version=1\nid="{name}"\nfixture="fixtures/repo"\n'
            f'revision="{revision}"\nprompt="{prompt}"\nmodel="fake"\n'
            '[[evaluators]]\nkind="required_file"\npath="required.txt"\n'
            'contains="present"\n'
        )

    class Worker:
        jobs = []
        pause_candidate = False
        candidate_entered = asyncio.Event()
        cancelled = False

        async def run(self, job, source, fixture, credential):
            self.jobs.append(job)
            if job.purpose == "candidate" and self.pause_candidate:
                self.candidate_entered.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise
            snapshot = tmp_path / "snapshots" / str(job.job_id)
            shutil.copytree(fixture, snapshot, ignore=shutil.ignore_patterns(".git"))
            repository = job.input_manifest.get("kind") == "repository"
            if (
                repository
                and heldout_failure
                and job.purpose == "candidate"
                and job.case_id == "heldout_private_case"
            ):
                (snapshot / "required.txt").unlink()
            baseline = not repository and job.purpose == "baseline"
            observation = WorkerObservation(
                job_id=job.job_id,
                status="error" if baseline else "completed",
                error_code="missing" if baseline else None,
                tool_result={
                    "output_text": "fixed",
                    "data": {"run_status": "completed", "run_outcome_code": None},
                },
            )
            return WorkerExecution(
                observation,
                snapshot,
                "completed",
                TrustedUsage(0, 0, 0, "reported"),
                0.01,
            )

    worker = Worker()
    gateway = ScopedModelGateway(
        provider, clock=CLOCK, endpoint="http://localhost/test-only"
    )
    evaluation = RepairEvaluation(
        factory,
        CLOCK,
        artifacts,
        main_root=main,
        data_dir=tmp_path / "private",
        runtime_identity=RuntimeIdentity(parent, "source", "lock"),
        model="fake",
        worker_factory=lambda: worker,
        gateway=gateway,
        fixtures=FixturePreparer(tmp_path / "private"),
        evaluators=EvaluatorRegistry(
            data_dir=tmp_path / "private", artifacts=artifacts
        ),
        image="worker@sha256:" + "a" * 64,
        instruction_digest="instructions",
        tool_schema_digest="schema",
        prompt_digest="prompt",
    )
    recovery = ManagedRepairWorkspace(factory, artifacts)
    controller = RepairController(
        factory,
        CLOCK,
        roles,
        incident,
        artifacts,
        evaluation=evaluation,
        recovery=recovery,
        acceptance=acceptance,
    )
    return (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    )


async def test_reproduction_and_review_precede_proposal_and_acceptance_is_a_seam(
    database, tmp_path
):
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
    await controller.execute(repair.id)
    run = await service.get(repair.id)
    assert run.status == "retryable_error" and run.stage == "commit_acceptance"
    assert worker.jobs[0].purpose == "baseline" and worker.jobs[0].case_id == "custom"
    assert sum(job.purpose == "baseline" for job in worker.jobs) == 7
    assert sum(job.purpose == "candidate" for job in worker.jobs) == 7
    assert (root / editable).read_text() == "# candidate one\n"
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert [(a.role, a.attempt_index) for a in attempts] == [
            ("diagnosis", 1),
            ("review", 1),
            ("proposal", 1),
        ]
        assert await uow.repairs.get_publication(repair.id) is None


async def test_retry_feedback_never_contains_heldout_trace(database, tmp_path):
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
    ) = await cycle_environment(database, tmp_path, heldout_failure=True)
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "exhausted"
    assert (root / editable).read_text() == "# original\n"
    assert "heldout_private_case" not in repr(provider.requests)
    assert "HELDOUT_SENTINEL" not in repr(provider.requests)
    assert "Evaluation gate failed." in repr(provider.requests)
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert [a.attempt_index for a in attempts if a.role == "proposal"] == [1, 2, 3]


async def test_reviewer_rejection_uses_one_three_attempt_diagnosis_chain(
    database, tmp_path
):
    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    rejected = {
        **review,
        "decision": "reject",
        "reasons": ["Expectation is not grounded"],
    }
    provider.outputs = [diagnosis, rejected, diagnosis, rejected, diagnosis, rejected]
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).outcome_code == "diagnosis_exhausted"
    assert worker.jobs == []
    async with factory() as uow:
        assert [
            (a.role, a.attempt_index)
            for a in await uow.repairs.list_attempts(repair.id)
        ] == [
            ("diagnosis", 1),
            ("review", 1),
            ("diagnosis", 2),
            ("review", 2),
            ("diagnosis", 3),
            ("review", 3),
        ]


async def test_independent_no_change_item_is_skipped(database, tmp_path):
    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    diagnosis["fix_items"].insert(
        0,
        {
            "decision": "no_change_needed",
            "mechanism": "An unrelated issue needs no change",
        },
    )
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).stage == "commit_acceptance"
    async with factory() as uow:
        assert len(await uow.repairs.list_fixes(repair.id)) == 1


async def test_baseline_must_reproduce_before_any_proposal(database, tmp_path):
    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    diagnosis["fix_items"][0]["proposed_case"]["expected_baseline_failure"][
        "error_code"
    ] = "different_failure"
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).outcome_code == "not_reproduced"
    assert len(provider.requests) == 2
    assert len(worker.jobs) == 1 and worker.jobs[0].purpose == "baseline"


async def test_independent_fixes_follow_expected_accepted_history(database, tmp_path):
    import copy
    import subprocess

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    second = copy.deepcopy(diagnosis["fix_items"][0])
    second["mechanism"] = "independent synthetic mechanism"
    second["proposed_case"]["case_id"] = "second_custom"
    diagnosis["fix_items"].append(second)
    provider.outputs = [diagnosis, review, review, *provider.outputs[2:]]

    class Acceptance:
        parents = []
        accepted = []

        async def accept(self, run, fix, case, snapshot, evidence_ids):
            self.parents.append(run.last_accepted_commit)
            assert snapshot["source_digest"] and evidence_ids
            subprocess.run(["git", "add", "--", editable], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "-qm", f"synthetic fix {fix.ordinal}"],
                cwd=root,
                check=True,
            )
            sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=root, text=True
            ).strip()
            self.accepted.append(sha)
            return sha

    acceptance = Acceptance()
    controller.acceptance = acceptance
    await controller.execute(repair.id)
    current = await service.get(repair.id)
    assert (
        current.stage == "publication" and current.outcome_code == "publication_pending"
    )
    assert acceptance.parents == [repair.last_accepted_commit, acceptance.accepted[0]]
    assert current.last_accepted_commit == acceptance.accepted[1]
    async with factory() as uow:
        fixes = await uow.repairs.list_fixes(repair.id)
        assert [f.status for f in fixes] == ["accepted", "accepted"]
        assert [f.attempts_used for f in fixes] == [1, 1]
    assert len([job for job in worker.jobs if job.purpose == "baseline"]) == 14
