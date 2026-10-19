"""Trusted assertions never execute in the candidate Python process."""

import pytest

from crucible.improvement.checks import validate_probe


def test_candidate_cannot_replace_protected_suite():
    with pytest.raises(ValueError):
        validate_probe("guidance", {"passed": True, "collected": ["forged"]})


def test_invalid_guidance_rejected_by_external_assertion():
    with pytest.raises(ValueError):
        validate_probe("guidance", {"coding": "é" * 4097, "step": ""})


async def test_infrastructure_failure_is_error_not_rejection(tmp_path):
    import json
    from uuid import uuid4

    from crucible.improvement.checks import CHECK_IDS, check_candidate, suite_digest
    from crucible.improvement.contracts import HarnessSnapshot
    from crucible.improvement.source import _digest
    from tests.unit.test_improvement_manifest import make_manifest

    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "suite.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                test_ids=CHECK_IDS,
                timeout_seconds=10,
                output_bytes=1024,
            )
        )
    )
    manifest = make_manifest().model_copy(
        update={"boundary_suite_digest": suite_digest(suite)}
    )
    harness = tmp_path / "harness"
    harness.mkdir()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=manifest.initial_commit,
        source_digest="f" * 64,
        export_digest=_digest({}),
        patch_digest="b" * 64,
        kind="candidate",
    )

    class BrokenProbe:
        async def probe(self, *args):
            raise ValueError("Docker failed")

    result = await check_candidate(
        snapshot,
        manifest,
        suite,
        tmp_path / "output",
        harness_export=harness,
        probes=BrokenProbe(),
    )
    assert result.verdict == "error"
    assert not result.cleanup_confirmed


@pytest.mark.parametrize("when", ["before", "during", "cleanup_failure"])
async def test_durable_stop_interrupts_checks_without_launching_next_probe(
    tmp_path, when
):
    import asyncio
    import json
    from uuid import uuid4

    from crucible.improvement.checks import (
        CHECK_IDS,
        DockerProbes,
        check_candidate,
        suite_digest,
    )
    from crucible.improvement.contracts import HarnessSnapshot
    from crucible.improvement.source import _digest
    from crucible.improvement.store import ExperimentStore
    from tests.unit.test_improvement_manifest import make_manifest

    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "suite.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                test_ids=CHECK_IDS,
                timeout_seconds=10,
                output_bytes=1024,
            )
        )
    )
    manifest = make_manifest().model_copy(
        update={"boundary_suite_digest": suite_digest(suite)}
    )
    harness = tmp_path / "harness"
    harness.mkdir()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=manifest.initial_commit,
        source_digest="a" * 64,
        export_digest=_digest({}),
        patch_digest="b" * 64,
        kind="candidate",
    )
    entered = asyncio.Event()

    class Docker:
        creates = 0
        running = False
        labels = {}
        kills = 0

        async def invoke(self, *args):
            if args[0] == "create":
                self.creates += 1
                self.labels = dict(
                    args[i + 1].split("=", 1)
                    for i, arg in enumerate(args)
                    if arg == "--label"
                )
                return "owned-probe"
            if args[0] == "kill":
                if when == "cleanup_failure":
                    raise RuntimeError("Docker kill failed")
                self.running = False
                self.kills += 1
                return ""
            assert args[0] == "inspect"
            return json.dumps(
                [
                    {
                        "Config": {"Labels": self.labels},
                        "State": {"Running": self.running},
                    }
                ]
            )

        async def attach(self, identity, limit):
            self.running = True
            entered.set()
            await asyncio.Event().wait()

    store = await ExperimentStore.open(tmp_path / "state.sqlite3")
    await store.create(manifest, "repo", "key")
    docker = Docker()
    task = None
    try:
        if when == "before":
            await store.request_stop(manifest.experiment_id)
        task = asyncio.create_task(
            check_candidate(
                snapshot,
                manifest,
                suite,
                tmp_path / "output",
                harness_export=harness,
                probes=DockerProbes(store, docker, manifest),
            )
        )
        if when != "before":
            await asyncio.wait_for(entered.wait(), 1)
            await store.request_stop(manifest.experiment_id)
        result = await asyncio.wait_for(asyncio.shield(task), 1)
        assert result.verdict == "error"
        if when == "cleanup_failure":
            assert result.reason == "protected_check_infrastructure_error"
            assert not result.cleanup_confirmed
            assert docker.running
            assert await store.unsettled_usage(manifest.experiment_id)
            assert any(
                effect["status"] == "uncertain"
                for effect in await store.effects(manifest.experiment_id)
            )
        else:
            assert result.reason == "operator_stopped"
            assert result.cleanup_confirmed
            assert docker.kills == (1 if when == "during" else 0)
            assert not docker.running
            assert not await store.unsettled_usage(manifest.experiment_id)
            assert all(
                effect["status"] == "stopped"
                for effect in await store.effects(manifest.experiment_id)
            )
        assert docker.creates == (0 if when == "before" else 1)
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await store.close()
