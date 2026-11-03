"""Durable diagnostic admission through the real SQLite reservation interface."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest

from crucible.improvement.contracts import MiningDiagnosticManifest, UsageAmounts
from crucible.improvement.mining_contracts import AnalystAssignment
from crucible.improvement.mining_store import MiningDiagnosticStore
from crucible.improvement.store import BudgetDenied, QuotaExhausted, RevisionConflict
from crucible.improvement.worker_protocol import RoleJob
from tests.contract.test_mining_diagnostic import diagnostic_payload, role_payload


@pytest.fixture
async def diagnostic_store(tmp_path):
    manifest = MiningDiagnosticManifest.model_validate_json(
        json.dumps(diagnostic_payload())
    )
    store = await MiningDiagnosticStore.open(tmp_path / "accounting.sqlite3")
    assignments = tuple(
        AnalystAssignment(
            slot_id=uuid4(),
            trial_id=trial,
            package_digest=manifest.package_digest,
            investigation_source_digest=manifest.investigation_source.source_digest,
        )
        for trial in manifest.selected_trial_ids
    )
    status = await store.create_diagnostic(
        manifest, "a" * 64, manifest.package_digest, assignments
    )
    try:
        yield store, manifest, status
    finally:
        await store.close()


def make_role(manifest, slot, **updates):
    payload = role_payload(manifest, slot.kind)
    payload.update(
        job_id=str(uuid4()),
        role_id=str(uuid4()),
        budget_slot_id=str(slot.slot_id),
        limits=slot.limits.model_dump(mode="json"),
        wall_deadline_seconds=slot.limits.wall_timeout_seconds,
    )
    payload.update(updates)
    return RoleJob.model_validate_json(json.dumps(payload))


@pytest.fixture
async def registered_requests(diagnostic_store):
    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    await store.register_attempt(status.slots[0].slot_id, job, job.input_digest)
    return [
        dict(
            id=status.diagnostic_id,
            trial_id=None,
            request=UsageAmounts(tokens=30000, requests=1),
            job_id=job.job_id,
        )
        for _ in range(2)
    ]


async def test_atomic_reservation_enforces_slot_and_group_caps(
    diagnostic_store, registered_requests
):
    store, _, status = diagnostic_store
    outcomes = await asyncio.gather(
        *(store.reserve_usage(**r) for r in registered_requests), return_exceptions=True
    )
    assert sum(isinstance(value, QuotaExhausted) for value in outcomes) == 1
    usage = await store.slot_usage(status.slots[0].slot_id)
    assert usage["remaining"].tokens == 20000
    assert (await store.usage_summary(status.diagnostic_id))["remaining"][
        "tokens"
    ] == 220000


async def test_generic_job_cannot_bypass_diagnostic_mapping(diagnostic_store):
    store, _, status = diagnostic_store
    for job_id in (None, uuid4()):
        with pytest.raises(BudgetDenied, match="registered"):
            await store.reserve_usage(
                status.diagnostic_id, None, UsageAmounts(tokens=1), job_id=job_id
            )


async def finish(store, attempt, status="invalid", calls=4):
    await store.finish_attempt(
        attempt,
        status,
        tool_call_count=calls,
        normal_completion=status in ("invalid", "complete", "incomplete"),
        cleanup_confirmed=True,
    )


async def test_correction_shares_original_allowance(diagnostic_store):
    store, manifest, status = diagnostic_store
    slot = status.slots[0]
    primary = make_role(manifest, slot)
    attempt = await store.register_attempt(slot.slot_id, primary, primary.input_digest)
    rid = await store.reserve_usage(
        status.diagnostic_id,
        None,
        UsageAmounts(
            tokens=20000, requests=3, active_seconds=100, worker_wall_seconds=150
        ),
        kind="worker",
        job_id=primary.job_id,
    )
    await store.settle_usage(
        rid,
        UsageAmounts(
            tokens=20000, requests=3, active_seconds=100, worker_wall_seconds=150
        ),
    )
    await finish(store, attempt)
    limits = dict(
        max_model_tokens=30000,
        max_steps=7,
        max_tool_calls=1,
        max_active_seconds=200,
        wall_timeout_seconds=450,
    )
    correction = make_role(
        manifest,
        slot,
        job_kind="format_correction",
        correction_of_job_id=str(primary.job_id),
        correction_target="trial_analysis",
        limits=limits,
        wall_deadline_seconds=450,
    )
    corrected = await store.register_attempt(
        slot.slot_id, correction, correction.input_digest
    )
    await store.reserve_usage(
        status.diagnostic_id, None, UsageAmounts(tokens=30000), job_id=correction.job_id
    )
    with pytest.raises(QuotaExhausted):
        await store.reserve_usage(
            status.diagnostic_id, None, UsageAmounts(tokens=1), job_id=correction.job_id
        )
    with pytest.raises(BudgetDenied):
        await store.reserve_usage(
            status.diagnostic_id, None, UsageAmounts(tokens=1), job_id=primary.job_id
        )
    assert corrected != attempt


async def test_retry_authorization_consumed_atomically_once(diagnostic_store):
    store, manifest, status = diagnostic_store
    slot = status.slots[0]
    primary = make_role(manifest, slot)
    attempt = await store.register_attempt(slot.slot_id, primary, primary.input_digest)
    await finish(store, attempt, "interrupted", 2)
    await recover(store, status.diagnostic_id)
    authorization = await store.authorize_retry(
        status.diagnostic_id, slot.slot_id, attempt
    )
    limits = slot.limits.model_dump(mode="json")
    limits["max_tool_calls"] = 18
    jobs = [make_role(manifest, slot, limits=limits) for _ in range(2)]
    outcomes = await asyncio.gather(
        *(
            store.register_attempt(slot.slot_id, j, j.input_digest, authorization)
            for j in jobs
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(value, RevisionConflict) for value in outcomes) == 1
    assert (
        len((await store.diagnostic_status(status.diagnostic_id)).slots[0].attempts)
        == 2
    )


async def test_uncertainty_blocks_all_slots(diagnostic_store):
    store, manifest, status = diagnostic_store
    jobs = [make_role(manifest, slot) for slot in status.slots[:2]]
    for slot, job in zip(status.slots, jobs):
        await store.register_attempt(slot.slot_id, job, job.input_digest)
    rid = await store.reserve_usage(
        status.diagnostic_id, None, UsageAmounts(tokens=1000), job_id=jobs[0].job_id
    )
    await store.mark_uncertain(rid)
    with pytest.raises(BudgetDenied, match="uncertain"):
        await store.reserve_usage(
            status.diagnostic_id, None, UsageAmounts(tokens=1), job_id=jobs[1].job_id
        )


async def test_store_reopen_preserves_caps_and_remaining(
    diagnostic_store, tmp_path, registered_requests
):
    store, manifest, status = diagnostic_store
    rid = await store.reserve_usage(**registered_requests[0])
    await store.settle_usage(rid, UsageAmounts(tokens=25000, requests=1))
    reopened = await MiningDiagnosticStore.open(tmp_path / "accounting.sqlite3")
    try:
        assert (await reopened.slot_usage(status.slots[0].slot_id))[
            "remaining"
        ].tokens == 25000
        with pytest.raises(QuotaExhausted):
            await reopened.reserve_usage(**registered_requests[0])
        with pytest.raises(BudgetDenied, match="admission"):
            job = make_role(
                manifest, status.slots[0], job_id=str(registered_requests[0]["job_id"])
            )
            reopened.take_job_admission(job, manifest)
    finally:
        await reopened.close()


async def recover(store, diagnostic_id, reservations=()):
    stopped = await store.request_stop(diagnostic_id)
    owner = await store.claim_recovery(
        diagnostic_id, stopped.revision, None, 123, "test"
    )
    if reservations:
        await store.acknowledge_usage(diagnostic_id, owner, reservations)
    await store.recover_ready(diagnostic_id, owner)


async def test_group_cap_after_reported_provider_overage(diagnostic_store, tmp_path):
    from uuid import uuid5

    from crucible.domain.clock import SystemClock
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        PreparedModelRequest,
    )
    from crucible.improvement.broker import ImprovementBroker, WorkspaceBinding
    from crucible.repair.gateway import ScopedModelGateway

    class Upstream:
        async def stream(self, request):
            yield ModelUsage(170000, 10000)
            yield ModelStop(ModelStopReason.COMPLETE)

    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    await store.register_attempt(status.slots[0].slot_id, job, job.input_digest)
    broker = ImprovementBroker(
        store,
        ScopedModelGateway(Upstream(), clock=SystemClock(), endpoint="unused"),
        None,
    )
    credential = broker.issue(
        job,
        manifest,
        WorkspaceBinding(tmp_path, manifest.worker_image, manifest.worker_platform),
    )
    request = PreparedModelRequest(
        uuid5(job.job_id, "run"), uuid4(), manifest.model.model_id, (), (), 100
    )
    assert [
        event async for event in broker.model(credential.token, job.job_id, request)
    ]
    reservations = tuple(
        UUID(str(r["reservation_id"]))
        for r in await store.usage_records(status.diagnostic_id)
    )
    await recover(store, status.diagnostic_id, reservations)
    jobs = [make_role(manifest, s) for s in status.slots[1:3]]
    for slot, new_job in zip(status.slots[1:], jobs):
        await store.register_attempt(slot.slot_id, new_job, new_job.input_digest)
    outcomes = await asyncio.gather(
        *(
            store.reserve_usage(
                status.diagnostic_id, None, UsageAmounts(tokens=40000), job_id=j.job_id
            )
            for j in jobs
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(value, QuotaExhausted) for value in outcomes) == 1
    assert (await store.usage_summary(status.diagnostic_id))["remaining"][
        "tokens"
    ] == 30000
    assert (await store.slot_usage(status.slots[0].slot_id))["remaining"].tokens == 0


async def test_retry_spend_is_never_reset(diagnostic_store):
    store, manifest, status = diagnostic_store
    slot = status.slots[0]
    job = make_role(manifest, slot)
    attempt = await store.register_attempt(slot.slot_id, job, job.input_digest)
    rid = await store.reserve_usage(
        status.diagnostic_id,
        None,
        UsageAmounts(tokens=30000, requests=1),
        job_id=job.job_id,
    )
    await store.settle_usage(rid, UsageAmounts(tokens=30000, requests=1))
    await finish(store, attempt, "interrupted", 2)
    await recover(store, status.diagnostic_id)
    authorization = await store.authorize_retry(
        status.diagnostic_id, slot.slot_id, attempt
    )
    fresh = make_role(manifest, slot)
    with pytest.raises(ValueError, match="remaining"):
        await store.register_attempt(
            slot.slot_id, fresh, fresh.input_digest, authorization
        )
    limits = slot.limits.model_dump(mode="json")
    limits.update(max_model_tokens=20000, max_tool_calls=18, max_steps=9)
    retry = make_role(manifest, slot, limits=limits)
    await store.register_attempt(slot.slot_id, retry, retry.input_digest, authorization)
    assert store.take_job_admission(retry, manifest).max_requests == 9
    await store.reserve_usage(
        status.diagnostic_id, None, UsageAmounts(tokens=20000), job_id=retry.job_id
    )
    with pytest.raises(QuotaExhausted):
        await store.reserve_usage(
            status.diagnostic_id, None, UsageAmounts(tokens=1), job_id=retry.job_id
        )


async def test_overage_is_retained_and_blocks_next_dispatch(
    diagnostic_store, registered_requests
):
    store, _, status = diagnostic_store
    rid = await store.reserve_usage(**registered_requests[0])
    await store.settle_usage(rid, UsageAmounts(tokens=60000, requests=1))
    with pytest.raises(BudgetDenied):
        await store.reserve_usage(**registered_requests[1])
    usage = await store.slot_usage(status.slots[0].slot_id)
    assert usage["retained"].tokens == 60000
    assert usage["remaining"].tokens == 0
    assert (await store.diagnostic_status(status.diagnostic_id)).status == "blocked"


@pytest.mark.parametrize("fault", ["tools", "cleanup"])
async def test_unknown_attempt_counts_or_cleanup_block_retry(diagnostic_store, fault):
    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    attempt = await store.register_attempt(
        status.slots[0].slot_id, job, job.input_digest
    )
    await store.finish_attempt(
        attempt,
        "interrupted",
        tool_call_count=None if fault == "tools" else 1,
        normal_completion=False,
        cleanup_confirmed=fault != "cleanup",
    )
    if fault == "cleanup":
        assert not (
            await store.diagnostic_status(status.diagnostic_id)
        ).cleanup_confirmed
    with pytest.raises(BudgetDenied):
        await store.authorize_retry(
            status.diagnostic_id, status.slots[0].slot_id, attempt
        )


async def test_retained_partial_charge_uses_only_unknown_dimension_caps(
    diagnostic_store,
):
    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    await store.register_attempt(status.slots[0].slot_id, job, job.input_digest)
    rid = await store.reserve_usage(
        status.diagnostic_id,
        None,
        UsageAmounts(tokens=1000, requests=1, output_bytes=1000),
        job_id=job.job_id,
    )
    await store.record_partial_usage(
        rid, UsageAmounts(requests=1, output_bytes=20), ("tokens",)
    )
    await recover(store, status.diagnostic_id, (rid,))
    usage = await store.slot_usage(status.slots[0].slot_id)
    assert usage["retained"] == UsageAmounts(tokens=1000, requests=1, output_bytes=20)
    assert usage["remaining"].tokens == 49000


async def test_existing_measured_ledger_is_refused_without_migration(tmp_path):
    from crucible.improvement.store import ExperimentStore
    from tests.unit.test_improvement_manifest import make_manifest

    path = tmp_path / "accounting.sqlite3"
    measured = await ExperimentStore.open(path)
    await measured.create(make_manifest(), "repository", "start")
    await measured.close()
    original = path.read_bytes()
    with pytest.raises(ValueError, match="dedicated diagnostic"):
        await MiningDiagnosticStore.open(path)
    assert path.read_bytes() == original


async def test_unknown_extension_version_is_refused(tmp_path, diagnostic_store):
    import sqlite3

    store, _, _ = diagnostic_store
    await store.close()
    path = tmp_path / "accounting.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE mining_metadata SET schema_version=99")
    with pytest.raises(ValueError, match="diagnostic schema version"):
        await MiningDiagnosticStore.open(path)


@pytest.mark.parametrize(
    "dimension,amount",
    [("active_seconds", 300), ("worker_wall_seconds", 600), ("requests", 10)],
)
async def test_worker_and_model_reservations_share_slot_dimensions(
    diagnostic_store, dimension, amount
):
    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    await store.register_attempt(status.slots[0].slot_id, job, job.input_digest)
    await store.reserve_usage(
        status.diagnostic_id,
        None,
        UsageAmounts(**{dimension: amount}),
        kind="worker",
        job_id=job.job_id,
    )
    with pytest.raises(QuotaExhausted):
        await store.reserve_usage(
            status.diagnostic_id,
            None,
            UsageAmounts(**{dimension: 1}),
            job_id=job.job_id,
        )


async def test_format_correction_is_single_use_and_requires_normal_invalid_output(
    diagnostic_store,
):
    store, manifest, status = diagnostic_store
    slot = status.slots[0]
    primary = make_role(manifest, slot)
    attempt = await store.register_attempt(slot.slot_id, primary, primary.input_digest)
    await finish(store, attempt)
    limits = slot.limits.model_dump(mode="json")
    limits["max_tool_calls"] = 1
    correction = make_role(
        manifest,
        slot,
        job_kind="format_correction",
        correction_target="trial_analysis",
        correction_of_job_id=str(primary.job_id),
        limits=limits,
    )
    corrected = await store.register_attempt(
        slot.slot_id, correction, correction.input_digest
    )
    await finish(store, corrected, calls=0)
    await recover(store, status.diagnostic_id)
    again = make_role(
        manifest,
        slot,
        job_kind="format_correction",
        correction_target="trial_analysis",
        correction_of_job_id=str(correction.job_id),
        limits=limits,
    )
    with pytest.raises(RevisionConflict, match="correction"):
        await store.register_attempt(slot.slot_id, again, again.input_digest)


@pytest.mark.parametrize("change", ["source", "input", "slot", "model", "export"])
async def test_changed_job_identity_cannot_register(diagnostic_store, change):
    store, manifest, status = diagnostic_store
    job = make_role(manifest, status.slots[0])
    # Register another slot first to bind the immutable executing export identity.
    first = make_role(manifest, status.slots[1])
    await store.register_attempt(status.slots[1].slot_id, first, first.input_digest)
    updates = {
        "source": {"harness_digest": "f" * 64},
        "input": {"input_digest": "f" * 64},
        "slot": {"budget_slot_id": uuid4()},
        "model": {
            "model": manifest.model.model_copy(update={"model_id": "foreign/model"})
        },
        "export": {"export_digest": "f" * 64},
    }
    job = job.model_copy(update=updates[change])
    with pytest.raises(ValueError):
        await store.register_attempt(status.slots[0].slot_id, job, "c" * 64)


async def test_execution_status_does_not_invent_source_export_hash(diagnostic_store):
    _, manifest, status = diagnostic_store
    assert status.executing_source.controller_digest == manifest.controller_digest
    assert "export_digest" not in status.executing_source.model_dump()
    assert status.investigation_source == manifest.investigation_source
