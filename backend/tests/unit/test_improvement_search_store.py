"""Search records are transactional and cannot upgrade historical experiments."""

import json
from uuid import uuid4

import pytest

from crucible.improvement.contracts import ExperimentManifest, HarnessSnapshot
from crucible.improvement.store import ExperimentStore, RevisionConflict
from tests.unit.test_improvement_manifest import manifest_payload


def search_manifest():
    payload = manifest_payload()
    payload.update(schema_version=2, worker_protocol_version=2)
    return ExperimentManifest.model_validate_json(json.dumps(payload))


def baseline(manifest):
    return HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=manifest.initial_commit,
        source_digest=manifest.initial_source_digest,
        export_digest="d" * 64,
        patch_digest=None,
        kind="baseline",
    )


async def test_round_allocation_is_revision_guarded(tmp_path):
    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        manifest = search_manifest()
        run = await store.create(manifest, "repo", "key")
        snapshot = baseline(manifest)
        await store.register_snapshot(run.id, snapshot, "snapshot.json")
        first = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
        assert first.ordinal == 1
        assert first.baseline_snapshot_id == snapshot.snapshot_id
        assert (await store.rounds(run.id)) == (first,)
        with pytest.raises(RevisionConflict):
            await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
        assert len(await store.events(run.id, "round_started")) == 1
    finally:
        await store.close()


async def test_historical_manifest_identity_survives_migration(tmp_path):
    import sqlite3

    from tests.unit.test_improvement_manifest import make_manifest

    path = tmp_path / "state.sqlite3"
    store = await ExperimentStore.open(path)
    old = await store.create(make_manifest(), "repo", "old")
    await store.close()
    # A historical v1 database has exactly the prerequisite tables and rows.
    with sqlite3.connect(path) as conn:
        for table in (
            "experiment_roles",
            "experiment_promotions",
            "experiment_search_seals",
            "experiment_publications",
            "experiment_rounds",
        ):
            conn.execute("DROP TABLE " + table)
        conn.execute("PRAGMA user_version=1")
        original = conn.execute(
            "SELECT manifest_json,manifest_hash FROM experiment_runs"
        ).fetchone()
    store = await ExperimentStore.open(path)
    try:
        assert await store.get(old.id) == old
        with pytest.raises(RevisionConflict, match="historical"):
            await store.begin_round(old.id, uuid4(), old.revision)
    finally:
        await store.close()
    with sqlite3.connect(path) as conn:
        assert (
            conn.execute(
                "SELECT manifest_json,manifest_hash FROM experiment_runs"
            ).fetchone()
            == original
        )
        assert conn.execute("PRAGMA user_version").fetchone() == (2,)


async def test_failed_migration_rolls_back_every_new_table(tmp_path):
    import sqlite3

    path = tmp_path / "state.sqlite3"
    store = await ExperimentStore.open(path)
    await store.close()
    with sqlite3.connect(path) as conn:
        for table in (
            "experiment_roles",
            "experiment_promotions",
            "experiment_search_seals",
            "experiment_rounds",
        ):
            conn.execute("DROP TABLE " + table)
        conn.execute("PRAGMA user_version=1")
    with pytest.raises(Exception, match="already exists"):
        await ExperimentStore.open(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (1,)
        assert not conn.execute(
            "SELECT name FROM sqlite_master WHERE name='experiment_rounds'"
        ).fetchall()


def reject(snapshot):
    from crucible.improvement.roles import SelectionDecision

    return SelectionDecision(
        schema_version=1,
        verdict="reject",
        reason="no_change",
        baseline_source_digest=snapshot.source_digest,
        baseline_export_digest=snapshot.export_digest,
        candidate_source_digest=snapshot.source_digest,
        candidate_export_digest=snapshot.export_digest,
        trial_ids=(),
        counts=(),
        boundary_result_digest="a" * 64,
        policy_digest="b" * 64,
    )


async def test_fourth_round_denied_and_decisions_immutable(tmp_path):
    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        manifest = search_manifest()
        run = await store.create(manifest, "repo", "key")
        snapshot = baseline(manifest)
        await store.register_snapshot(run.id, snapshot, "snapshot.json")
        for ordinal in range(1, 4):
            run = await store.get(run.id)
            round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
            assert round.ordinal == ordinal
            revision = (await store.get(run.id)).revision
            await store.record_decision(round.id, reject(snapshot), revision)
            assert (await store.rounds(run.id))[
                -1
            ].candidate_snapshot_id == snapshot.snapshot_id
            with pytest.raises(RevisionConflict):
                await store.record_decision(round.id, reject(snapshot), revision)
        with pytest.raises(RevisionConflict, match="limit"):
            await store.begin_round(
                run.id, snapshot.snapshot_id, (await store.get(run.id)).revision
            )
        assert len(await store.events(run.id, "decision_recorded")) == 3
    finally:
        await store.close()


async def test_unaccepted_candidate_cannot_be_round_baseline(tmp_path):
    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        manifest = search_manifest()
        run = await store.create(manifest, "repo", "key")
        candidate = baseline(manifest).model_copy(
            update={
                "kind": "candidate",
                "source_digest": "e" * 64,
                "patch_digest": "f" * 64,
            }
        )
        await store.register_snapshot(run.id, candidate, "candidate.json")
        with pytest.raises(RevisionConflict, match="accepted"):
            await store.begin_round(run.id, candidate.snapshot_id, run.revision)
    finally:
        await store.close()


async def test_role_completion_is_durable_and_cannot_be_overwritten(tmp_path):
    from crucible.improvement.roles import RoleExecution

    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    try:
        manifest = search_manifest()
        run = await store.create(manifest, "repo", "key")
        snapshot = baseline(manifest)
        await store.register_snapshot(run.id, snapshot, "snapshot.json")
        round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
        execution = RoleExecution(
            role_id=uuid4(),
            job_id=uuid4(),
            experiment_id=run.id,
            round_id=round.id,
            kind="mining",
            accepted_snapshot_id=snapshot.snapshot_id,
            source_digest=snapshot.source_digest,
            input_digest="b" * 64,
            input_ref="view/index.json",
            status="running",
        )
        await store.save_role(execution, 0, (await store.get(run.id)).revision)
        completed = execution.model_copy(
            update={
                "status": "complete",
                "output_ref": "roles/output.json",
                "output_digest": "c" * 64,
            }
        )
        await store.save_role(completed, 0, (await store.get(run.id)).revision)
        assert (await store.roles(round.id))[0].execution == completed
        assert (await store.rounds(run.id))[0].mining_attempts == 1
        with pytest.raises(RevisionConflict):
            await store.save_role(
                completed.model_copy(update={"output_digest": "d" * 64}),
                0,
                (await store.get(run.id)).revision,
            )
        assert len(await store.events(run.id, "role_saved")) == 2
    finally:
        await store.close()
