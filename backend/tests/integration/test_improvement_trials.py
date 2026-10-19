"""Scripted-worker orchestration plumbing, not isolated benchmark performance."""

from pathlib import Path

import pytest

from crucible.improvement.trials import selection_schedule
from tests.unit.test_improvement_manifest import make_manifest


def test_selection_schedule_excludes_final_and_has_three_repeats():
    schedule = selection_schedule(make_manifest())
    assert schedule == tuple(
        (case, index)
        for case in ["development", "selection-validation"]
        for index in range(3)
    )


@pytest.fixture
async def trial_setup(tmp_path, case_fixture, monkeypatch):
    import hashlib
    import json
    import shutil
    from uuid import uuid4

    from crucible.benchmarks.admission import grader_policy_digest
    from crucible.benchmarks.contracts import GraderConfig, GradeResult
    from crucible.benchmarks.manifests import case_digest
    from crucible.improvement.contracts import (
        ExperimentPaths,
        FrozenCaseRef,
        HarnessSnapshot,
    )
    from crucible.improvement.source import _digest
    from crucible.improvement.store import ExperimentStore
    from crucible.improvement.trials import IsolatedTrialRunner
    from crucible.improvement.worker_protocol import (
        IsolatedExecution,
        WorkerObservation,
    )
    from crucible.repair.gateway import TrustedUsage

    source = tmp_path / "evaluator"
    shutil.copytree(case_fixture.fixture_path, source)
    monkeypatch.setattr(
        "crucible.benchmarks.swebench.PINNED_COMMIT", case_fixture.base_commit
    )
    config = GraderConfig(
        driver_python=Path("/usr/bin/python3"),
        driver_source=source,
        driver_commit=case_fixture.base_commit,
        driver_digest=case_fixture.fixture_digest,
        timeout_seconds=10,
        memory_bytes=1024,
        cpus=1,
        output_limit_bytes=65536,
        network="none",
    )
    registry = tmp_path / "registry"
    registry.mkdir()
    refs = []
    entries = {}
    for split in ("development", "selection-validation", "final-test"):
        case = case_fixture.model_copy(update={"case_id": split, "split": split})
        manifest_path = registry / (split + ".json")
        manifest_path.write_text(case.model_dump_json())
        admission_root = registry / (split + "-admission")
        admission_root.mkdir()
        reports = {}
        for label, resolved in [("empty", False), ("reference", True)]:
            report = admission_root / (label + ".json")
            report.write_text(
                json.dumps(
                    {
                        split: dict(
                            resolved=resolved,
                            patch_exists=True,
                            patch_is_None=False,
                            patch_successfully_applied=True,
                            infra_failure=False,
                            tests_status={
                                key: dict(
                                    success=["test"]
                                    if key == "FAIL_TO_PASS" and resolved
                                    else [],
                                    failure=["test"]
                                    if key == "FAIL_TO_PASS" and not resolved
                                    else [],
                                )
                                for key in [
                                    "FAIL_TO_PASS",
                                    "PASS_TO_PASS",
                                    "FAIL_TO_FAIL",
                                    "PASS_TO_FAIL",
                                ]
                            },
                        )
                    }
                )
            )
            reports[label] = report
        admission = admission_root / "admission.json"
        admission.write_text(
            json.dumps(
                dict(
                    schema_version=1,
                    platform=case.platform,
                    grader_policy_digest=grader_policy_digest(config),
                    report_digests={
                        k: hashlib.sha256(p.read_bytes()).hexdigest()
                        for k, p in reports.items()
                    },
                    result=dict(
                        status="admitted",
                        case_digest=case_digest(case),
                        driver_digest=config.driver_digest,
                        image_digest=case.image.split("@")[1],
                        empty_grade={"report_path": str(reports["empty"])},
                        reference_grade={"report_path": str(reports["reference"])},
                    ),
                )
            )
        )
        entries[split] = dict(
            case_manifest=str(manifest_path),
            admission_record=str(admission),
            retired=False,
        )
        refs.append(
            FrozenCaseRef(
                case_id=split,
                split=split,
                case_digest=case_digest(case),
                admission_digest=hashlib.sha256(admission.read_bytes()).hexdigest(),
            )
        )
    (registry / "registry.json").write_text(
        json.dumps(dict(schema_version=1, pending_slots=0, cases=entries))
    )
    manifest = make_manifest().model_copy(
        update={
            "cases": tuple(refs),
            "grader": config,
            "initial_commit": case_fixture.base_commit,
            "budget": make_manifest().budget.model_copy(
                update={"max_total_output_bytes": 64 * 1024 * 1024}
            ),
        }
    )
    state = tmp_path / "state"
    state.mkdir()
    store = await ExperimentStore.open(state / "experiments.sqlite3")
    await store.create(manifest, "repository", "key")
    snapshot_root = state / "snapshots/base"
    snapshot_root.mkdir(parents=True)
    (snapshot_root / "harness").mkdir()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=case_fixture.base_commit,
        source_digest="a" * 64,
        export_digest=_digest({}),
        patch_digest=None,
        kind="baseline",
    )
    (snapshot_root / "snapshot.json").write_text(snapshot.model_dump_json())
    await store.register_snapshot(
        manifest.experiment_id, snapshot, "snapshots/base/snapshot.json"
    )

    class Worker:
        calls = 0
        termination = "budget_exhausted"
        cleanup = True
        jobs = []

        async def execute(self, job, snapshot, workspace, *, harness_export):
            self.calls += 1
            self.jobs.append(job)
            (workspace / "app.py").write_text("answer = 42\n")
            output = tmp_path / ("worker-" + str(job.job_id))
            output.mkdir()
            (output / "trace.json").write_text("[]")
            (output / "usage.json").write_text("{}")
            observation = WorkerObservation(
                schema_version=1,
                job_id=job.job_id,
                trial_id=job.trial_id,
                export_digest=job.export_digest,
                termination=self.termination,
                trace_ref="trace.json",
                usage_ref="usage.json",
                detail_code="claims_pass",
            )
            return IsolatedExecution(
                observation, workspace, output, TrustedUsage(), (), self.cleanup, 1.0
            )

        async def stop(self, job_id):
            pass

    class Grader:
        calls = 0
        verdict = "fail"

        async def grade(self, case, patch, trial_id, output_dir):
            self.calls += 1
            output_dir.mkdir()
            report = output_dir / "report.json"
            report.write_text("{}")
            log = output_dir / "log"
            log.write_text("scripted grading plumbing")
            assert "answer = 42" in patch.path.read_text()
            return GradeResult(
                self.verdict,
                "scripted",
                str(trial_id),
                self.verdict == "pass",
                patch.sha256,
                report,
                log,
                config.driver_commit,
                0.1,
            )

    worker = Worker()
    grader = Grader()
    paths = ExperimentPaths(
        root=state, registry_root=registry, repository=case_fixture.fixture_path
    )
    runner = IsolatedTrialRunner(store, paths, worker, grader, tmp_path / "suite")
    yield runner, store, manifest, snapshot, worker, grader
    await store.close()


pytest_plugins = ["tests.unit.test_benchmark_manifests"]


async def test_worker_claimed_pass_is_ignored(trial_setup):
    runner, store, manifest, snapshot, worker, grader = trial_setup
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.status == "complete"
    assert result.grade.verdict == "fail"
    assert result.solver.termination == "budget_exhausted"
    assert "split" not in type(worker.jobs[0].solver_case).model_fields
    assert "grader" not in type(worker.jobs[0]).model_fields
    cached = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert cached.identity.trial_id == result.identity.trial_id
    assert worker.calls == grader.calls == 1


async def test_budget_end_can_pass_independent_grade(trial_setup):
    runner, store, manifest, snapshot, worker, grader = trial_setup
    grader.verdict = "pass"
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.grade.verdict == "pass"
    assert result.solver.termination == "budget_exhausted"


async def test_integrity_failure_never_grades(trial_setup):
    runner, store, manifest, snapshot, worker, grader = trial_setup
    worker.cleanup = False
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.status == "error"
    assert grader.calls == 0


async def test_final_test_never_dispatched(trial_setup):
    runner, store, manifest, snapshot, worker, grader = trial_setup
    with pytest.raises(ValueError, match="final-test"):
        await runner.run(manifest.experiment_id, snapshot.snapshot_id, "final-test", 0)
    assert worker.calls == grader.calls == 0


async def test_cleanup_failure_still_checkpoints_and_stops_experiment(trial_setup):
    runner, store, manifest, snapshot, worker, grader = trial_setup
    worker.cleanup = False

    async def broken_stop(job_id):
        raise ValueError("cleanup unavailable")

    worker.stop = broken_stop
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.status == "error"
    assert result.error_code == "cleanup_uncertain"
    assert (await store.get(manifest.experiment_id)).status == "error"
    assert (await store.trials(manifest.experiment_id))[0]["status"] == "error"


async def test_candidate_checks_precede_paid_trials(trial_setup):
    from uuid import uuid4

    runner, store, manifest, snapshot, worker, grader = trial_setup
    candidate = snapshot.model_copy(
        update={
            "snapshot_id": uuid4(),
            "kind": "candidate",
            "source_digest": "e" * 64,
            "patch_digest": "f" * 64,
        }
    )
    root = runner.paths.root / "snapshots/candidate"
    root.mkdir()
    (root / "harness").mkdir()
    (root / "snapshot.json").write_text(candidate.model_dump_json())
    await store.register_snapshot(
        manifest.experiment_id, candidate, "snapshots/candidate/snapshot.json"
    )
    with pytest.raises(ValueError):
        await runner.run(
            manifest.experiment_id, candidate.snapshot_id, "development", 0
        )
    assert worker.calls == grader.calls == 0


async def test_host_quota_termination_preserves_patch_for_grading(
    trial_setup, database, tmp_path
):
    """Trusted engine + wire adapter plumbing; candidate Python is never host-loaded."""
    from dataclasses import replace

    import httpx

    from crucible.engine.gateway import ModelError
    from crucible.engine.run_engine import RunEngine
    from crucible.improvement.worker_gateway import WorkerModelGateway
    from crucible.repair.gateway import WorkerCredential
    from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory

    runner, _, manifest, snapshot, worker, grader = trial_setup
    grader.verdict = "pass"
    original = worker.execute
    seen_errors = []

    async def execute(job, snapshot, workspace, *, harness_export):
        execution = await original(
            job, snapshot, workspace, harness_export=harness_export
        )
        submitted = await queued_run(database, tmp_path, "quota-engine")
        factory = uow_factory(database)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    content=(
                        b'{"schema_version":1,"kind":"error",'
                        b'"code":"improvement_budget_exhausted",'
                        b'"detail":"Model quota exhausted"}\n'
                    ),
                )
            )
        ) as client:
            gateway = WorkerModelGateway(
                WorkerCredential("token", "http://broker/internal/improvement/model"),
                job.job_id,
                client=client,
            )

            class CapturingGateway:
                async def stream(self, request):
                    async for event in gateway.stream(request):
                        if isinstance(event, ModelError):
                            seen_errors.append(event)
                        yield event

            await RunEngine(factory, FixedClock(), CapturingGateway()).execute(
                submitted.run_id
            )
        async with factory() as uow:
            terminal = await uow.runs.get(submitted.run_id)
        assert terminal.outcome_code == "budget_exhausted"
        assert not seen_errors
        return replace(
            execution,
            observation=execution.observation.model_copy(
                update={"termination": terminal.outcome_code}
            ),
        )

    worker.execute = execute
    result = await runner.run(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0
    )
    assert result.status == "complete"
    assert result.solver.termination == "budget_exhausted"
    assert result.grade.verdict == "pass"
    assert grader.calls == 1
