"""Recovery reconciles owned state without granting execution or new allowance."""

import json
from uuid import UUID

import pytest

from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner
from tests.integration import test_mining_diagnostic as fixtures

harness = fixtures.harness
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial
diagnostic_config = fixtures.diagnostic_config
prepared_diagnostic = fixtures.prepared_diagnostic


async def test_recover_is_execution_free(prepared_diagnostic, monkeypatch):
    from crucible.improvement.worker_supervisor import WorkerSupervisor
    from crucible.repair.gateway import ScopedModelGateway

    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(
        store, prepared.root, faults={0: "interrupted"}
    )
    failed = await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
        prepared.diagnostic_id
    )
    before = failed.slots[0].spent

    def forbidden(*args, **kwargs):
        raise AssertionError("recovery constructed an execution port")

    monkeypatch.setattr(WorkerSupervisor, "__init__", forbidden)
    monkeypatch.setattr(ScopedModelGateway, "__init__", forbidden)
    status = await MiningDiagnosticRunner(store, prepared.root, None).recover(
        prepared.diagnostic_id
    )
    assert status.status == "ready"
    assert status.next_action == "authorize-retry"
    assert status.slots[0].status == "interrupted"
    assert status.slots[0].spent == before
    assert len(worker.jobs) == 1


async def test_single_use_retry_targets_exact_failed_attempt(prepared_diagnostic):
    from crucible.improvement.store import RevisionConflict

    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(
        store, prepared.root, faults={0: "interrupted"}
    )
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    failed = await runner.continue_run(prepared.diagnostic_id)
    slot = failed.slots[0]
    await runner.recover(prepared.diagnostic_id)
    with pytest.raises((ValueError, RevisionConflict)):
        await runner.authorize_retry(
            prepared.diagnostic_id, slot.slot_id, UUID(int=999)
        )
    await runner.authorize_retry(
        prepared.diagnostic_id, slot.slot_id, slot.attempts[-1].attempt_id
    )
    worker.faults = {}
    done = await runner.continue_run(prepared.diagnostic_id)
    assert done.status == "complete"
    assert worker.jobs[1].limits.max_model_tokens == 49900
    assert worker.jobs[1].limits.max_steps == 9
    assert len(done.slots[0].attempts) == 2
    assert done.slots[0].spent.tokens == 200
    with pytest.raises((ValueError, RevisionConflict)):
        await runner.authorize_retry(
            prepared.diagnostic_id, slot.slot_id, slot.attempts[-1].attempt_id
        )


async def test_continue_reuses_only_validated_matching_reports(prepared_diagnostic):
    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(
        store, prepared.root, faults={1: "interrupted"}
    )
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    failed = await runner.continue_run(prepared.diagnostic_id)
    assert failed.slots[0].status == "complete"
    await runner.recover(prepared.diagnostic_id)
    slot = failed.slots[1]
    await runner.authorize_retry(
        prepared.diagnostic_id, slot.slot_id, slot.attempts[-1].attempt_id
    )
    worker.faults = {}
    completed = await runner.continue_run(prepared.diagnostic_id)
    assert completed.status == "complete"
    assert len(worker.jobs) == 5
    assert len(completed.slots[0].attempts) == 1
    assert len(completed.slots[1].attempts) == 2


@pytest.mark.parametrize(
    "artifact", ["assistant.txt", "inspection-receipts.json", "outcome.json"]
)
async def test_completed_report_artifact_drift_blocks_reuse(
    prepared_diagnostic, artifact
):
    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(
        store, prepared.root, faults={1: "interrupted"}
    )
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    await runner.continue_run(prepared.diagnostic_id)
    path = prepared.root / "attempts" / str(worker.jobs[0].job_id) / artifact
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="drift"):
        await runner.recover(prepared.diagnostic_id)
    assert len(worker.jobs) == 2


@pytest.mark.parametrize(
    "field", ["evidence", "source", "model", "prompt", "schema", "caps", "image"]
)
async def test_changed_frozen_input_requires_new_identity(prepared_diagnostic, field):
    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(store, prepared.root)
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    if field in ("source", "evidence"):
        path = (
            prepared.root
            / "frozen/package/source/backend/src/crucible/tools/filesystem.py"
            if field == "source"
            else next((prepared.root / "frozen/package/trials").rglob("overview.json"))
        )
        path.write_bytes(path.read_bytes() + b" ")
    else:
        path = prepared.root / "frozen/manifest.json"
        data = json.loads(path.read_bytes())
        if field == "model":
            data["model"]["model_id"] = "foreign/model"
        elif field == "caps":
            data["analyst_limits"]["max_model_tokens"] = 49999
        elif field == "image":
            data["worker_image"] = "foreign@sha256:" + "f" * 64
        else:
            data[
                {"prompt": "prompt_digest", "schema": "report_schema_digest"}[field]
            ] = "f" * 64
        path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        await runner.continue_run(prepared.diagnostic_id)
    assert worker.jobs == []


async def test_recovery_never_resets_unknown_allowance(prepared_diagnostic):
    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(
        store, prepared.root, faults={0: "unknown_observation"}
    )
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    failed = await runner.continue_run(prepared.diagnostic_id)
    recovered = await runner.recover(prepared.diagnostic_id)
    assert recovered.status == "blocked"
    assert recovered.slots[0].retained == failed.slots[0].retained
    assert recovered.slots[0].remaining == failed.slots[0].remaining
    assert len(worker.jobs) == 1


async def test_stop_prevents_remaining_launches(prepared_diagnostic):
    prepared, store = prepared_diagnostic

    class StoppingWorker(fixtures.ScriptedSupervisor):
        async def execute(self, *args, **kwargs):
            result = await super().execute(*args, **kwargs)
            await runner.stop(prepared.diagnostic_id)
            return result

    worker = StoppingWorker(store, prepared.root)
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    stopped = await runner.continue_run(prepared.diagnostic_id)
    assert stopped.status == "interrupted"
    assert len(worker.jobs) == 1
    assert stopped.slots[1].status == "pending"
    assert (await runner.continue_run(prepared.diagnostic_id)).status == "interrupted"


@pytest.mark.parametrize("foreign", [False, True])
async def test_recovery_reconciles_only_exact_owned_effects(
    prepared_diagnostic, foreign
):
    from crucible.improvement.worker_supervisor import labels

    prepared, store = prepared_diagnostic

    class RetainedWorker(fixtures.ScriptedSupervisor):
        async def execute(self, *args, **kwargs):
            result = await super().execute(*args, **kwargs)
            job = self.jobs[-1]
            effect = await store.begin_effect(
                job.experiment_id,
                None,
                "worker",
                {"name": "improvement-worker-" + str(job.job_id)},
                labels(job),
            )
            await store.effect_state(effect, "active", "container")
            return result

    worker = RetainedWorker(store, prepared.root, faults={0: "interrupted"})
    await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
        prepared.diagnostic_id
    )
    job = worker.jobs[0]

    class Docker:
        def __init__(self):
            self.present = True
            self.calls = []

        async def invoke(self, *args):
            self.calls.append(args)
            if args[0] == "ps":
                return "container" if self.present else ""
            if args[0] == "inspect":
                return json.dumps(
                    [
                        {
                            "Config": {"Labels": {} if foreign else labels(job)},
                            "State": {"Running": False},
                        }
                    ]
                )
            assert args == ("rm", "container")
            self.present = False
            return ""

    docker = Docker()
    recovered = await MiningDiagnosticRunner(
        store, prepared.root, None, docker=docker
    ).recover(prepared.diagnostic_id)
    assert recovered.status == ("blocked" if foreign else "ready")
    assert docker.present == foreign
    assert recovered.cleanup_confirmed != foreign
    assert not any(call[0] in ("start", "create") for call in docker.calls)


async def test_recover_collects_durable_worker_artifacts_after_controller_crash(
    prepared_diagnostic, monkeypatch
):
    import os
    from pathlib import Path

    from crucible.repair.identity import canonical_json

    prepared, store = prepared_diagnostic

    class DurableWorker(fixtures.ScriptedSupervisor):
        async def execute(self, *args, **kwargs):
            result = await super().execute(*args, **kwargs)
            (result.state_root.parent / "job-outcome.json").write_bytes(
                canonical_json(result.outcome)
            )
            return result

    class ControllerCrash(BaseException):
        pass

    replace_file = os.replace

    def crash_before_collection(source, destination):
        if (
            Path(destination).name == "outcome.json"
            and "attempts" in Path(destination).parts
        ):
            raise ControllerCrash()
        return replace_file(source, destination)

    worker = DurableWorker(store, prepared.root)
    monkeypatch.setattr(os, "replace", crash_before_collection)
    with pytest.raises(ControllerCrash):
        await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
            prepared.diagnostic_id
        )
    monkeypatch.setattr(os, "replace", replace_file)
    before = await store.diagnostic_status(prepared.diagnostic_id)
    assert before.slots[0].status == "running"
    assert before.slots[0].reserved.output_bytes == 4 * 1_048_576
    recovered = await MiningDiagnosticRunner(store, prepared.root, None).recover(
        prepared.diagnostic_id
    )
    assert recovered.status == "ready"
    assert recovered.slots[0].status == "interrupted"
    assert recovered.slots[0].spent.tokens == 100
    assert recovered.slots[0].reserved.output_bytes == 0
    assert recovered.slots[0].spent.output_bytes > 0
    assert not (prepared.root / "reports").exists()
    assert len(worker.jobs) == 1
