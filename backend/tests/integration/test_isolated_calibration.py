"""One-shot admission and host-authoritative grading, without paid calls."""

import json
from uuid import uuid4

import pytest

from crucible.improvement.calibration import (
    grading_allowed,
    validate_calibration_inputs,
)
from crucible.improvement.contracts import CalibrationManifest, HarnessSnapshot
from crucible.improvement.store import CalibrationStore
from tests.unit.test_isolated_calibration import calibration_payload

pytest_plugins = ["tests.unit.test_benchmark_manifests"]


def test_calibration_grades_budget_stop_not_infra_error():
    assert grading_allowed("completed", True)
    assert grading_allowed("budget_exhausted", True)
    assert not grading_allowed("infrastructure_error", True)
    assert not grading_allowed("completed", False)


async def test_calibration_identity_cannot_launch_twice(tmp_path):
    manifest = CalibrationManifest.model_validate_json(
        json.dumps(calibration_payload())
    )
    store = await CalibrationStore.open(tmp_path / "state.sqlite3")
    try:
        await store.create(manifest, "calibration", "key")
        snapshot = HarnessSnapshot(
            snapshot_id=uuid4(),
            parent_commit="a" * 40,
            source_digest="a" * 64,
            export_digest="b" * 64,
            patch_digest=None,
            kind="baseline",
        )
        await store.register_snapshot(manifest.experiment_id, snapshot, "snapshot.json")
        assert (
            await store.admit_trial(
                manifest.experiment_id, snapshot.snapshot_id, "case"
            )
            == manifest.trial_id
        )
        with pytest.raises(ValueError, match="already admitted"):
            await store.admit_trial(
                manifest.experiment_id, snapshot.snapshot_id, "case"
            )
    finally:
        await store.close()


def test_calibration_drift_blocks_dispatch(tmp_path):
    manifest = CalibrationManifest.model_validate_json(
        json.dumps(calibration_payload())
    )
    with pytest.raises((ValueError, FileNotFoundError)):
        validate_calibration_inputs(
            manifest, tmp_path, tmp_path / "output", tmp_path / "state"
        )


async def test_calibration_approval_eof_denies(monkeypatch):
    from types import SimpleNamespace

    from crucible.improvement import calibration

    async def closed():
        raise EOFError

    monkeypatch.setattr(calibration, "_operator_line", closed)
    assert not await calibration.interactive_approval(
        SimpleNamespace(job_id=uuid4()),
        SimpleNamespace(canonical_json=lambda: '{"executable":"echo"}'),
    )


@pytest.mark.parametrize(
    "termination,cleanup,gradable",
    [
        ("completed", True, True),
        ("budget_exhausted", True, True),
        ("infrastructure_error", True, False),
        ("completed", False, False),
    ],
)
async def test_calibration_composition(
    tmp_path, case_fixture, monkeypatch, termination, cleanup, gradable
):
    from contextlib import asynccontextmanager

    from crucible.benchmarks.contracts import GradeResult
    from crucible.improvement import calibration
    from crucible.improvement.worker_protocol import (
        IsolatedExecution,
        WorkerObservation,
    )
    from crucible.repair.gateway import TrustedUsage

    payload = calibration_payload()
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            dict(
                manifest=payload,
                repository=str(tmp_path),
                data_dir=str(tmp_path / "runtime"),
            )
        )
    )
    monkeypatch.setattr(
        calibration, "validate_calibration_inputs", lambda *args: case_fixture
    )
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit="a" * 40,
        source_digest="a" * 64,
        export_digest="b" * 64,
        patch_digest=None,
        kind="baseline",
    )

    def stage(repository, commit, destination, paths):
        (destination / "harness").mkdir(parents=True)
        (destination / "snapshot.json").write_text(snapshot.model_dump_json())
        return snapshot

    monkeypatch.setattr(calibration, "stage_harness", stage)
    seen = []

    class Worker:
        async def execute(self, job, snapshot, workspace, *, harness_export):
            seen.append(job)
            assert "grader_input" not in job.model_dump_json()
            assert harness_export.name == "harness"
            state = tmp_path / "worker-state"
            state.mkdir()
            (state / "trace.json").write_text("{}")
            (state / "usage.json").write_text("{}")
            return IsolatedExecution(
                WorkerObservation(
                    schema_version=2,
                    job_id=job.job_id,
                    trial_id=job.trial_id,
                    export_digest=job.export_digest,
                    termination=termination,
                    trace_ref="trace.json",
                    usage_ref="usage.json",
                    detail_code=None,
                ),
                workspace,
                state,
                TrustedUsage(0, 0, 0),
                (),
                cleanup,
                1,
            )

        async def stop(self, job_id):
            pass

    @asynccontextmanager
    async def runtime(*args):
        yield Worker()

    monkeypatch.setattr(calibration, "execution_runtime", runtime)
    grades = []

    async def grade(self, case, patch, trial_id, output):
        grades.append(patch)
        return GradeResult(
            "pass",
            "resolved",
            "id",
            True,
            patch.sha256,
            None,
            output / "log",
            "a" * 40,
            1,
        )

    monkeypatch.setattr(calibration.SwebenchGrader, "grade", grade)
    output = tmp_path / "output"
    if gradable:
        result = await calibration.run_isolated_calibration(config, output)
        assert result.status == "complete"
        assert result.grade.verdict == "pass"
        assert len(grades) == 1
    else:
        with pytest.raises(RuntimeError):
            await calibration.run_isolated_calibration(config, output)
        assert not grades
        assert json.loads((output / "result.json").read_text())["status"] == "error"
    assert len(seen) == 1
    with pytest.raises(FileExistsError):
        await calibration.run_isolated_calibration(config, tmp_path / "second-output")
    assert len(seen) == 1
