"""Public metrics distinguish unavailable usage from zero and omit raw payloads."""

import json

from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner
from crucible.improvement.mining_reporting import write_mining_report
from tests.integration import test_mining_diagnostic as fixtures

harness = fixtures.harness
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial
diagnostic_config = fixtures.diagnostic_config
prepared_diagnostic = fixtures.prepared_diagnostic


async def test_usage_unknown_is_not_zero(prepared_diagnostic, tmp_path):
    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(store, prepared.root, faults={0: "raised"})
    await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
        prepared.diagnostic_id
    )
    await write_mining_report(store, prepared.diagnostic_id, tmp_path / "report")
    metrics = json.loads((tmp_path / "report/metrics.json").read_bytes())
    attempt = metrics["slots"][0]["attempts"][0]
    assert attempt["reported_model"] is None
    assert attempt["tool_calls"] is None
    assert metrics["usage"]["reported_model"]["input_tokens"] is None
    assert metrics["usage"]["charged"]["retained"]["output_bytes"] == 4 * 1048576


async def test_report_is_deterministic_and_excludes_raw_payloads(
    prepared_diagnostic, tmp_path, monkeypatch
):
    import hashlib

    from crucible.repair.identity import canonical_json

    prepared, store = prepared_diagnostic
    secret = "REPORT_PRIVATE_PAYLOAD_aa9d39"
    monkeypatch.setenv("REPORT_TEST_API_KEY", secret)

    class SecretWorker(fixtures.ScriptedSupervisor):
        async def execute(self, *args, **kwargs):
            result = await super().execute(*args, **kwargs)
            path = result.state_root / "assistant.txt"
            data = json.loads(path.read_bytes())
            data["summary"] = secret
            path.write_bytes(canonical_json(data))
            (result.state_root / "trace.json").write_bytes(
                canonical_json({"private": secret})
            )
            return result

    worker = SecretWorker(store, prepared.root)
    await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
        prepared.diagnostic_id
    )
    first = await write_mining_report(store, prepared.diagnostic_id, tmp_path / "first")
    second = await write_mining_report(
        store, prepared.diagnostic_id, tmp_path / "second"
    )
    assert first == second
    for name, digest in first.items():
        raw = (tmp_path / "first" / name).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == digest
        assert secret.encode() not in raw
        assert b"# historical" not in raw
        assert str(prepared.root).encode() not in raw
        assert b"record_key" not in raw
    metrics = json.loads((tmp_path / "first/metrics.json").read_bytes())
    assert metrics["usage"]["charged"]["dollars"] is None
    assert metrics["usage"]["charged"]["price_basis"] is None
    assert metrics["selection"]["selected_repeat_count"] == 3
    assert metrics["selection"]["eligible_corpus_task_count"] is None


async def test_report_cannot_write_into_an_immutable_workspace(prepared_diagnostic):
    import pytest

    prepared, store = prepared_diagnostic
    worker = fixtures.ScriptedSupervisor(store, prepared.root)
    runner = MiningDiagnosticRunner(store, prepared.root, worker)
    await runner.continue_run(prepared.diagnostic_id)
    output = (
        prepared.root / "attempts" / str(worker.jobs[0].job_id) / "workspace/report"
    )
    with pytest.raises(ValueError, match="nonoverlapping"):
        await write_mining_report(store, prepared.diagnostic_id, output)
    assert not output.exists()
    assert (await runner.validate(prepared.diagnostic_id)).status == "complete"


async def test_conservative_charge_is_not_labeled_observed(
    prepared_diagnostic, tmp_path
):
    import os
    from dataclasses import replace
    from uuid import UUID

    from crucible.improvement.contracts import UsageAmounts
    from crucible.improvement.worker_protocol import JobFailure

    prepared, store = prepared_diagnostic

    class PartialWorker(fixtures.ScriptedSupervisor):
        async def execute(self, *args, **kwargs):
            result = await super().execute(*args, **kwargs)
            job = self.jobs[-1]
            reservation = await store.reserve_usage(
                job.experiment_id,
                None,
                UsageAmounts(tokens=1000, requests=1),
                job_id=job.job_id,
            )
            await store.record_partial_usage(
                reservation, UsageAmounts(requests=1), ("tokens",)
            )
            return replace(
                result,
                outcome=replace(
                    result.outcome,
                    termination="failed",
                    failure=JobFailure("quota", "controller"),
                    unresolved_reservations=(
                        *result.outcome.unresolved_reservations,
                        reservation,
                    ),
                ),
            )

    await MiningDiagnosticRunner(
        store, prepared.root, PartialWorker(store, prepared.root)
    ).continue_run(prepared.diagnostic_id)
    stopped = await store.request_stop(prepared.diagnostic_id)
    owner = await store.claim_recovery(
        prepared.diagnostic_id, stopped.revision, None, os.getpid(), None
    )
    pending = tuple(
        UUID(str(r["reservation_id"]))
        for r in await store.usage_records(prepared.diagnostic_id)
        if r["status"] in ("reserved", "uncertain")
    )
    # Explicit acknowledgment retains caps without inventing measurements.
    await store.acknowledge_usage(prepared.diagnostic_id, owner, pending)
    await store.release_execution(prepared.diagnostic_id, owner, stopped.stage)
    await write_mining_report(store, prepared.diagnostic_id, tmp_path / "report")
    metrics = json.loads((tmp_path / "report/metrics.json").read_bytes())
    reservations = metrics["slots"][0]["attempts"][0]["reservations"]
    charged = next(
        r
        for r in reservations
        if r["status"] == "conservative" and r["ledger_amounts"]["tokens"] == 1000
    )
    assert charged["ledger_basis"] == "conservative_charge"
    assert charged["observed"] is None
    assert metrics["usage"]["charged"]["conservative"]["tokens"] == 1000
