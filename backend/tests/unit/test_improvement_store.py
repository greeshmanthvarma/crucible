import asyncio

import pytest

from crucible.improvement.contracts import UsageAmounts
from crucible.improvement.store import BudgetDenied, ExperimentStore
from tests.unit.test_improvement_manifest import make_manifest


async def test_parallel_reservations_cannot_overspend(tmp_path):
    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        experiment = await store.create(make_manifest(), "repository", "start")
        outcomes = await asyncio.gather(
            store.reserve_usage(experiment.id, None, UsageAmounts(tokens=60)),
            store.reserve_usage(experiment.id, None, UsageAmounts(tokens=60)),
            return_exceptions=True,
        )
        assert sum(isinstance(x, BudgetDenied) for x in outcomes) == 1
    finally:
        await store.close()


async def test_restart_preserves_reservations_and_idempotency(tmp_path):
    path = tmp_path / "experiments.sqlite3"
    manifest = make_manifest()
    store = await ExperimentStore.open(path)
    first = await store.create(manifest, "repository", "start")
    await store.reserve_usage(first.id, None, UsageAmounts(tokens=80))
    await store.close()
    store = await ExperimentStore.open(path)
    try:
        repeated = await store.create(manifest, "repository", "start")
        assert repeated == first
        with pytest.raises(BudgetDenied):
            await store.reserve_usage(first.id, None, UsageAmounts(tokens=21))
    finally:
        await store.close()


async def test_stale_revision_and_stop_prevent_new_effects(tmp_path):
    from crucible.improvement.store import RevisionConflict

    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        await store.transition(run.id, 0, "running", "baseline", "baseline_started")
        with pytest.raises(RevisionConflict):
            await store.transition(run.id, 0, "ready", "baseline", "stale")
        await store.request_stop(run.id)
        with pytest.raises(BudgetDenied):
            await store.reserve_usage(run.id, None, UsageAmounts(requests=1))
    finally:
        await store.close()


async def test_changed_idempotent_manifest_and_repository_lock(tmp_path):
    from crucible.improvement.store import ManifestDrift, RevisionConflict

    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    manifest = make_manifest()
    try:
        await store.create(manifest, "repo", "key")
        changed = manifest.model_copy(update={"controller_digest": "c" * 64})
        with pytest.raises(ManifestDrift):
            await store.create(changed, "repo", "key")
        with pytest.raises(RevisionConflict):
            await store.create(make_manifest(), "repo", "different")
    finally:
        await store.close()


async def test_settlement_releases_only_known_unused_reservation(tmp_path):
    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        reservation = await store.reserve_usage(run.id, None, UsageAmounts(tokens=100))
        await store.settle_usage(reservation, UsageAmounts(tokens=40))
        await store.reserve_usage(run.id, None, UsageAmounts(tokens=60))
        with pytest.raises(BudgetDenied):
            await store.reserve_usage(run.id, None, UsageAmounts(tokens=1))
    finally:
        await store.close()


async def test_unknown_schema_rejected(tmp_path):
    import sqlite3

    path = tmp_path / "experiments.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA user_version=4")
    with pytest.raises(ValueError, match="schema"):
        await ExperimentStore.open(path)


async def test_independent_controllers_share_atomic_budget(tmp_path):
    path = tmp_path / "experiments.sqlite3"
    first = await ExperimentStore.open(path)
    second = await ExperimentStore.open(path)
    try:
        run = await first.create(make_manifest(), "repo", "key")
        outcomes = await asyncio.gather(
            first.reserve_usage(run.id, None, UsageAmounts(tokens=60)),
            second.reserve_usage(run.id, None, UsageAmounts(tokens=60)),
            return_exceptions=True,
        )
        assert sum(isinstance(x, BudgetDenied) for x in outcomes) == 1
    finally:
        await first.close()
        await second.close()


async def test_uncertain_usage_cannot_be_rewritten(tmp_path):
    from crucible.improvement.store import RevisionConflict

    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        run = await store.create(make_manifest(), "repo", "key")
        reservation = await store.reserve_usage(run.id, None, UsageAmounts(tokens=60))
        await store.settle_usage(reservation, UsageAmounts(tokens=80))
        with pytest.raises(RevisionConflict, match="uncertain"):
            await store.settle_usage(reservation, UsageAmounts(tokens=0))
    finally:
        await store.close()


async def test_v2_accounting_upgrade_preserves_every_original_row(tmp_path):
    import sqlite3

    path = tmp_path / "state.sqlite3"
    store = await ExperimentStore.open(path)
    run = await store.create(make_manifest(), "repo", "key")
    reservation = await store.reserve_usage(run.id, None, UsageAmounts(tokens=50))
    await store.mark_uncertain(reservation)
    await store.close()
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA user_version=2")
        before = conn.execute("SELECT * FROM experiment_usage").fetchall()
        events = conn.execute("SELECT * FROM experiment_events").fetchall()
    store = await ExperimentStore.open(path)
    await store.close()
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (3,)
        assert conn.execute("SELECT * FROM experiment_usage").fetchall() == before
        assert conn.execute("SELECT * FROM experiment_events").fetchall() == events
