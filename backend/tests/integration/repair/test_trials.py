"""Durable controller plumbing with synthetic observations, never repair metrics."""

import asyncio
import json
import shutil
import subprocess
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import TypeAdapter
from sqlalchemy import select

from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.repair.cases import IncidentCase, seal_case
from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.gateway import TrustedUsage
from crucible.repair.identity import digest
from crucible.repair.trials import TrialEvidence, TrialService, compare_cases
from crucible.repair.worker_protocol import WorkerObservation
from crucible.repair.worker_supervisor import WorkerExecution
from crucible.storage import models
from crucible.storage.repair_trials import SqlAlchemyTrialStore
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 10, 14, tzinfo=UTC)


class Clock:
    def now(self):
        return NOW


async def environment(database, tmp_path):
    """Only test DB provenance is seeded; no production/user-facing repair runs."""
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    (source / "behavior.py").write_text("# deterministic source identity fixture\n")
    for args in (
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.com"),
        ("add", "behavior.py"),
        ("commit", "-qm", "fixture"),
    ):
        subprocess.run(["git", *args], cwd=source, check=True, capture_output=True)
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "note.txt").write_text("before\n")
    fixture_digest = digest({"schema_version": 1, "files": {"note.txt": "before\n"}})
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, _, _ = await seed_exchange(uow, "synthetic-repair-trial")
        await uow.commit()

    def factory():
        return SqlAlchemyUnitOfWork(database)

    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", Clock()), factory
    )
    return {
        "factory": factory,
        "artifacts": artifacts,
        "source_root": source,
        "source": await capture_source_identity(source),
        "fixture": fixture,
        "fixture_digest": fixture_digest,
        "task": task,
        "run": run,
    }


def approved_case(env, *, case_id="synthetic-write", mode="direct", assertions=None):
    payload = {
        "schema_version": 1,
        "case_id": case_id,
        "incident_ids": {"task": env["task"].id, "run": env["run"].id},
        "kind": "tool",
        "replay_mode": "direct",
        "fixture_manifest_digest": env["fixture_digest"],
        "settings": {"admitted_mutation_paths": ["note.txt"]},
        "expected_baseline_failure": {
            "status": "error",
            "error_code": "write_failed",
            "detail_contains": ["cannot write", "{workspace}/note.txt"],
        },
        "candidate_assertions": assertions
        or [
            {"op": "error_code_equals", "value": None},
            {"op": "file_text_equals", "path": "note.txt", "value": "after\n"},
            {"op": "unrelated_files_unchanged"},
        ],
        "expected_behavior": "Synthetic test expectation; not a natural repair case.",
        "evidence_ids": [],
        "tool_name": "write_file",
        "arguments": {"path": "note.txt", "content": "after\n"},
    }
    if mode == "fresh_call":
        payload.pop("tool_name")
        payload.pop("arguments")
        payload.update(
            kind="compaction",
            replay_mode=mode,
            source_artifact_ids=[],
            correlation_links={},
            request_artifact_id=uuid4(),
            raw_response_artifact_id=None,
        )
    return seal_case(TypeAdapter(IncidentCase).validate_python(payload), uuid4())


class Worker:
    """Test-only supervisor seam; never imports or executes candidate Python."""

    def __init__(
        self,
        root,
        *,
        content="after\n",
        status="completed",
        code=None,
        termination="completed",
        context_messages=None,
    ):
        self.root, self.content = root, content
        self.status, self.code, self.termination = status, code, termination
        self.context_messages = context_messages
        self.jobs = []
        self.running = False
        self.before_return = None
        self.entered = asyncio.Event()
        self.block = False

    async def run(self, job, source, fixture, credential):
        self.jobs.append(job)
        self.running = True
        self.entered.set()
        try:
            if self.block:
                await asyncio.Event().wait()
            snapshot = self.root / str(job.job_id)
            shutil.copytree(fixture, snapshot)
            if self.content is not None:
                (snapshot / "note.txt").write_text(self.content)
            observation = WorkerObservation(
                job_id=job.job_id,
                status=self.status,
                tool_result={
                    "output_text": "worker claims pass",
                    "data": {
                        "passed": True,
                        "claimed_tokens": 0,
                    },
                },
                error_code=self.code,
                detail=f"cannot write {snapshot}/note.txt" if self.code else None,
                context_messages=self.context_messages,
            )
            execution = WorkerExecution(
                observation,
                snapshot,
                self.termination,
                TrustedUsage(1, 11, 3, "reported"),
                0.25,
            )
            if self.before_return:
                self.before_return(execution)
            return execution
        finally:
            self.running = False


def service(env, worker, *, invocation_id=None, candidate_generation=0):
    return TrialService(
        env["factory"],
        worker,
        source_root=env["source_root"],
        fixtures={env["fixture_digest"]: env["fixture"]},
        artifacts=env["artifacts"],
        clock=Clock(),
        invocation_id=invocation_id or uuid4(),
        candidate_generation=candidate_generation,
    )


def configuration(case, source, *, revision="same"):
    return digest(
        {
            "version": 1,
            "source": source.model_dump(mode="json"),
            "case_digest": case.case_digest,
            "controlled_config": revision,
        }
    )


async def test_verifier_ignores_worker_pass_claim(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(tmp_path / "workers", content=None)
    result = await service(env, worker).run(
        case, env["source"], "candidate", 0, configuration(case, env["source"])
    )
    assert result.verdict == "fail"
    assert any(not assertion["passed"] for assertion in result.assertion_results)
    assert result.usage["input_tokens"] == 11
    assert result.usage["output_tokens"] == 3
    async with env["factory"]() as uow:
        saved = await uow.repair_trials.list_results(
            result.fingerprint, case.case.case_id, "candidate"
        )
        job = await uow.repair_trials.get_job(result.id)
    assert saved == (result,)
    assert job.status == "completed"
    assert job.completed_at is not None
    assert result.observation_artifact_id is not None
    metadata, raw = await env["artifacts"].read(result.observation_artifact_id)
    assert metadata.sensitivity == "private"
    assert json.loads(raw)["job_id"] == str(result.id)


async def test_baseline_matches_original_failure_but_candidate_same_error_fails(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(
        tmp_path / "workers", status="error", code="write_failed", content=None
    )
    trials = service(env, worker)
    fingerprint = configuration(case, env["source"])
    baseline = await trials.run(case, env["source"], "baseline", 0, fingerprint)
    candidate = await trials.run(case, env["source"], "candidate", 0, fingerprint)
    assert (
        baseline.verdict == "pass"
    )  # Baseline pass means intended failure reproduced.
    assert candidate.verdict == "fail"
    assert baseline.side == "baseline" and candidate.side == "candidate"


@pytest.mark.parametrize("code", [None, "different_failure"])
async def test_no_failure_or_wrong_failure_is_not_reproduction(
    database, tmp_path, code
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(
        tmp_path / "workers", status="error" if code else "completed", code=code
    )
    result = await service(env, worker).run(
        case, env["source"], "baseline", 0, configuration(case, env["source"])
    )
    assert result.verdict == "fail"


@pytest.mark.parametrize(
    "status,termination,expected",
    [
        ("unsupported", "completed", "unsupported"),
        ("completed", "timed_out", "error"),
        ("completed", "process_error", "error"),
        ("completed", "backend_error", "error"),
        ("completed", "cleanup_error", "error"),
    ],
)
async def test_infrastructure_and_missing_evidence_are_not_behavioral_failures(
    database, tmp_path, status, termination, expected
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(tmp_path / "workers", status=status, termination=termination)
    result = await service(env, worker).run(
        case, env["source"], "candidate", 0, configuration(case, env["source"])
    )
    assert result.verdict == expected
    assert not any(
        assertion.get("behavioral_failure") for assertion in result.assertion_results
    )


async def test_changed_source_aborts_before_worker_dispatch(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(tmp_path / "workers")
    (env["source_root"] / "new.py").write_text("# changed source")
    with pytest.raises(ValueError, match="[Ss]ource|identity|frozen"):
        await service(env, worker).run(
            case, env["source"], "candidate", 0, configuration(case, env["source"])
        )
    assert not worker.jobs


async def test_source_drift_during_evaluation_cannot_produce_pass(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(tmp_path / "workers")
    worker.before_return = lambda _: (env["source_root"] / "behavior.py").write_text(
        "# external edit during trial"
    )
    trials = service(env, worker)
    try:
        result = await trials.run(
            case, env["source"], "candidate", 0, configuration(case, env["source"])
        )
    except ValueError:
        pass
    else:
        assert result.verdict == "error"
    async with env["factory"]() as uow:
        saved = await uow.repair_trials.list_results(
            configuration(case, env["source"]), case.case.case_id, "candidate"
        )
    assert not any(result.verdict == "pass" for result in saved)
    assert (
        env["source_root"] / "behavior.py"
    ).read_text() == "# external edit during trial"


async def test_result_and_completed_job_are_committed_atomically(
    database, tmp_path, monkeypatch
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    original = SqlAlchemyTrialStore.update_job

    async def fail_completion(self, job):
        if job.status == "completed":
            raise RuntimeError("synthetic transaction failure")
        return await original(self, job)

    monkeypatch.setattr(SqlAlchemyTrialStore, "update_job", fail_completion)
    with pytest.raises(RuntimeError, match="synthetic transaction failure"):
        await service(env, Worker(tmp_path / "workers")).run(
            case, env["source"], "candidate", 0, configuration(case, env["source"])
        )
    async with env["factory"]() as uow:
        results = await uow.repair_trials.list_results(
            configuration(case, env["source"]), case.case.case_id, "candidate"
        )
    assert results == ()
    async with database.engine.connect() as connection:
        statuses = (
            (await connection.execute(select(models.repair_trial_jobs.c.status)))
            .scalars()
            .all()
        )
    assert "completed" not in statuses


def evidence(case_id, *, side, verdict, repeat_index, fingerprint="f" * 64):
    return TrialEvidence(
        id=uuid4(),
        fingerprint=fingerprint,
        case_id=case_id,
        repeat_index=repeat_index,
        side=side,
        source_digest="a" * 64,
        observation_artifact_id=None,
        verdict=verdict,
        assertion_results=(),
        usage={
            "requests": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "basis": "reported",
        },
        created_at=NOW,
    )


def samples(case_id, passes, *, side, total=3):
    return tuple(
        evidence(
            case_id,
            side=side,
            verdict="pass" if index < passes else "fail",
            repeat_index=index,
        )
        for index in range(total)
    )


def test_case_gain_cannot_offset_another_regression():
    baseline = {
        "protected-case": samples("protected-case", 2, side="baseline"),
        "other-case": samples("other-case", 0, side="baseline"),
    }
    candidate = {
        "protected-case": samples("protected-case", 1, side="candidate"),
        "other-case": samples("other-case", 3, side="candidate"),
    }
    gate = compare_cases(baseline, candidate)
    assert not gate.eligible
    assert gate.counts["protected-case"]["baseline_passes"] == 2
    assert gate.counts["protected-case"]["candidate_passes"] == 1


@pytest.mark.parametrize(
    "baseline_count,candidate_count,eligible",
    [(3, 3, True), (2, 2, True), (1, 2, True), (3, 2, False)],
)
def test_fixed_case_gate_preserves_each_observed_pass_count(
    baseline_count, candidate_count, eligible
):
    gate = compare_cases(
        {"case": samples("case", baseline_count, side="baseline")},
        {"case": samples("case", candidate_count, side="candidate")},
    )
    assert gate.eligible is eligible


@pytest.mark.parametrize(
    "baseline_count,candidate_count,eligible",
    [(0, 3, False), (1, 3, True), (2, 2, False)],
)
async def test_fresh_custom_requires_reproduction_and_three_candidate_passes(
    database, tmp_path, baseline_count, candidate_count, eligible
):
    env = await environment(database, tmp_path)
    case = approved_case(env, mode="fresh_call")
    name = case.case.case_id
    gate = compare_cases(
        {name: samples(name, baseline_count, side="baseline")},
        {name: samples(name, candidate_count, side="candidate")},
        custom_cases={name: case},
    )
    assert gate.eligible is eligible


@pytest.mark.parametrize("bad", ["error", "unsupported"])
def test_error_and_unsupported_do_not_count_as_behavioral_failures(bad):
    baseline = {"case": samples("case", 2, side="baseline")}
    candidate = {
        "case": (
            evidence("case", side="candidate", verdict=bad, repeat_index=0),
            *samples("case", 3, side="candidate")[1:],
        )
    }
    gate = compare_cases(baseline, candidate)
    assert not gate.eligible
    assert gate.counts["case"]["candidate_failures"] == 0
    assert gate.counts["case"][f"candidate_{bad}"] == 1


@pytest.mark.parametrize(
    "defect",
    ["missing_case", "extra_case", "two_trials", "duplicate_repeat", "wrong_side"],
)
def test_incomplete_or_mismatched_trial_sets_cannot_pass(defect):
    baseline = {"case": samples("case", 3, side="baseline")}
    candidate = {"case": samples("case", 3, side="candidate")}
    if defect == "missing_case":
        candidate = {}
    elif defect == "extra_case":
        candidate["invented"] = samples("invented", 3, side="candidate")
    elif defect == "two_trials":
        candidate["case"] = candidate["case"][:2]
    elif defect == "duplicate_repeat":
        candidate["case"] = (candidate["case"][0],) * 3
    else:
        candidate["case"] = samples("case", 3, side="baseline")
    assert not compare_cases(baseline, candidate).eligible


def test_held_out_public_feedback_does_not_disclose_case_or_verifier_details():
    case_id = "private-held-out-identifier"
    gate = compare_cases(
        {case_id: samples(case_id, 3, side="baseline")},
        {case_id: samples(case_id, 2, side="candidate")},
    )
    feedback = gate.public_feedback(held_out=True)
    assert "gate" in feedback.lower()
    assert case_id not in feedback
    assert "2/3" not in feedback


async def test_changed_fixture_is_rejected_before_worker_dispatch(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    worker = Worker(tmp_path / "workers")
    (env["fixture"] / "note.txt").write_text("changed controlled fixture")
    with pytest.raises(ValueError, match="[Ff]ixture|digest|frozen"):
        await service(env, worker).run(
            case, env["source"], "candidate", 0, configuration(case, env["source"])
        )
    assert not worker.jobs


def test_empty_gate_is_not_evidence_of_acceptance():
    assert not compare_cases({}, {}).eligible


async def test_repository_suite_uses_three_workers_and_protected_properties(
    database, tmp_path
):
    from types import SimpleNamespace

    from crucible.domain.repository import RepositorySettings
    from crucible.evals.evaluators import EvaluatorRegistry
    from crucible.evals.manifests import (
        EvalCaseDefinition,
        EvalPartition,
        EvalSuiteDefinition,
        EvaluatorDefinition,
    )

    env = await environment(database, tmp_path)
    worker = Worker(tmp_path / "workers")
    controlled = EvalCaseDefinition(
        "fixed-repository",
        env["fixture"],
        "a" * 40,
        "Synthetic repository test",
        RepositorySettings(),
        "deterministic-test-model",
        (EvaluatorDefinition("required_file", {"path": "note.txt"}),),
        "b" * 64,
        EvalPartition.DEVELOPMENT,
        "synthetic-fixture",
    )
    suite = EvalSuiteDefinition(
        "synthetic-suite", (controlled,), "c" * 64, EvalPartition.DEVELOPMENT
    )

    class Fixtures:
        def prepare(self, case, job_id):
            return SimpleNamespace(root=env["fixture"])

    async def credential(job):
        return None  # Deterministic supervisor seam; no model connection.

    trials = service(env, worker)
    trials.credential_factory = credential
    results = await trials.run_repository_suite(
        suite,
        env["source"],
        "candidate",
        {controlled.case_id: "d" * 64},
        fixtures=Fixtures(),
        evaluators=EvaluatorRegistry(data_dir=tmp_path),
        task_id=env["task"].id,
        run_id=env["run"].id,
    )
    assert len(results[controlled.case_id]) == 3
    assert all(row.verdict == "pass" for row in results[controlled.case_id])
    assert len(worker.jobs) == 3
    assert all("evaluators" not in job.input_manifest for job in worker.jobs)
    assert {job.repeat_index for job in worker.jobs} == {0, 1, 2}


async def test_supervisor_initial_snapshot_is_the_file_mode_reference(
    database, tmp_path
):
    from dataclasses import replace

    env = await environment(database, tmp_path)
    (env["fixture"] / "unrelated.txt").write_text("preserved")
    env["fixture_digest"] = digest(
        {
            "schema_version": 1,
            "files": {"note.txt": "before\n", "unrelated.txt": "preserved"},
        }
    )
    case = approved_case(env)
    worker = Worker(tmp_path / "workers")
    before = tmp_path / "prepared-before"
    shutil.copytree(env["fixture"], before)
    (before / "note.txt").chmod(0o666)
    (before / "unrelated.txt").chmod(0o666)
    (before / ".git").mkdir()
    (before / ".git" / "index").write_bytes(b"supervisor metadata")
    original_run = worker.run

    async def stopped(*args):
        execution = await original_run(*args)
        (execution.workspace_snapshot / "note.txt").chmod(0o666)
        (execution.workspace_snapshot / "unrelated.txt").chmod(0o666)
        (execution.workspace_snapshot / ".git").mkdir()
        (execution.workspace_snapshot / ".git" / "index").write_bytes(
            b"changed Git index"
        )
        return replace(execution, initial_workspace_snapshot=before)

    worker.run = stopped
    result = await service(env, worker).run(
        case, env["source"], "candidate", 0, configuration(case, env["source"])
    )
    assert result.verdict == "pass"
