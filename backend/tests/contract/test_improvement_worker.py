"""Worker mount/protocol plumbing; not real Docker isolation proof."""

import json
from uuid import uuid4

import pytest

from crucible.improvement.worker_protocol import WorkerJob, read_worker_observation
from crucible.improvement.worker_supervisor import worker_container_arguments
from tests.unit.test_improvement_manifest import make_manifest


def make_job():
    manifest = make_manifest()
    return WorkerJob.model_validate_json(
        json.dumps(
            dict(
                schema_version=1,
                job_id=str(uuid4()),
                experiment_id=str(manifest.experiment_id),
                trial_id=str(uuid4()),
                repeat_index=0,
                harness_digest="a" * 64,
                export_digest="b" * 64,
                solver_case=dict(
                    case_id="development",
                    base_commit="a" * 40,
                    base_tree="b" * 40,
                    prompt="Fix the task",
                    image="task@sha256:" + "c" * 64,
                    platform="linux/amd64",
                    workspace="/workspace",
                ),
                model=manifest.model.model_dump(mode="json"),
                limits=manifest.trial_limits.model_dump(mode="json"),
                output_limit_bytes=1024,
                wall_deadline_seconds=90,
                protocol_version=1,
            )
        )
    )


def test_worker_mounts_exclude_host_authority(tmp_path):
    job = make_job()
    args = worker_container_arguments(
        job,
        make_manifest(),
        network="owned-network",
        harness=tmp_path / "harness",
        workspace=tmp_path / "workspace",
        state=tmp_path / "state",
        job_root=tmp_path / "job",
        driver=tmp_path / "driver.py",
    )
    joined = " ".join(args)
    assert "/var/run/docker.sock" not in joined
    assert "--privileged" not in args
    assert "--read-only" in args
    assert "--cap-drop=ALL" in args
    assert "--user" in args
    assert "OPENAI_API_KEY" not in joined
    assert "target=/harness,readonly" in joined
    assert "experiments.sqlite3" not in joined


def test_observation_cannot_claim_grade_or_other_identity():
    job = make_job()
    payload = dict(
        schema_version=1,
        job_id=str(job.job_id),
        trial_id=str(job.trial_id),
        export_digest=job.export_digest,
        termination="completed",
        trace_ref="trace.json",
        usage_ref="usage.json",
        detail_code=None,
    )
    payload["grade"] = "pass"
    with pytest.raises(ValueError):
        read_worker_observation(json.dumps(payload).encode(), job)
    del payload["grade"]
    payload["trace_ref"] = "../private"
    with pytest.raises(ValueError):
        read_worker_observation(json.dumps(payload).encode(), job)


def test_solver_runtime_import_graph_excludes_host_authority():
    # Static source inspection only. Never imports candidate/exported Python.
    import ast
    from pathlib import Path

    from crucible.improvement.source import solver_runtime_path

    source = Path(__file__).parents[2] / "src"
    pending = ["crucible.improvement.worker"]
    visited = set()
    while pending:
        module = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        path = source / Path(*module.split(".")).with_suffix(".py")
        if not path.exists():
            path = source / Path(*module.split(".")) / "__init__.py"
        if not path.exists() and path.parent.is_dir():
            continue
        assert path.exists(), module
        relative = "backend/src/" + path.relative_to(source).as_posix()
        assert solver_runtime_path(relative), relative
        tree = ast.parse(path.read_bytes())

        class RuntimeImports(ast.NodeVisitor):
            def visit_If(self, node):
                if isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
                    return
                self.generic_visit(node)

            def visit_FunctionDef(self, node):
                return  # Lazy imports in unused host helper functions are not loaded.

            def visit_AsyncFunctionDef(self, node):
                return

            def visit_ImportFrom(self, node):
                if node.module and node.module.startswith("crucible."):
                    pending.append(node.module)

        RuntimeImports().visit(tree)


def test_worker_observation_fifo_is_rejected_without_blocking(tmp_path):
    import os

    from crucible.improvement.worker_supervisor import read_regular_artifact

    path = tmp_path / "observation.json"
    os.mkfifo(path)
    with pytest.raises(ValueError, match="regular"):
        read_regular_artifact(path, 1024)


async def test_cleanup_attempts_other_resources_after_failure(tmp_path):
    from uuid import uuid4

    from crucible.improvement.worker_supervisor import WorkerSupervisor

    stopped = []

    class Store:
        async def effect_state(self, effect, status, external_id=None):
            stopped.append((effect, status))

    class Broker:
        async def stop(self, job_id):
            raise ValueError("command cleanup failed")

    class Docker:
        async def invoke(self, *args):
            name = args[-1]
            if name == "bad-worker":
                raise ValueError("inspect failed")
            return json.dumps(
                [
                    {
                        "Config": {
                            "Labels": {"crucible.improvement.job": str(job.job_id)}
                        },
                        "State": {"Running": False},
                    }
                ]
            )

    job = make_job()
    supervisor = WorkerSupervisor(
        Store(),
        Broker(),
        Docker(),
        tmp_path,
        make_manifest(),
        "http://host.docker.internal:1234",
    )
    relay_effect = uuid4()
    worker_effect = uuid4()
    supervisor.owned[job.job_id] = [
        (relay_effect, "relay", "good-relay", job),
        (worker_effect, "worker", "bad-worker", job),
    ]
    with pytest.raises(ExceptionGroup):
        await supervisor.stop(job.job_id)
    assert (relay_effect, "stopped") in stopped
    assert (worker_effect, "uncertain") in stopped


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "preflight",
        "create",
        "worker_exit",
        "cancel",
        "cleanup",
        "stream",
        "stream_cleanup",
        "behavior",
    ],
)
async def test_create_before_ack_cleanup_and_memory_only_credential(tmp_path, fault):
    from uuid import uuid4

    from crucible.improvement.contracts import HarnessSnapshot
    from crucible.improvement.source import _digest
    from crucible.improvement.worker_supervisor import WorkerSupervisor
    from tests.unit.test_improvement_broker import setup_broker

    store, broker, old_job, _ = await setup_broker(tmp_path, None)
    job = old_job.model_copy(update={"job_id": uuid4()})
    harness = tmp_path / "harness"
    harness.mkdir()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit="a" * 40,
        source_digest=job.harness_digest,
        export_digest=_digest({}),
        patch_digest=None,
        kind="baseline",
    )
    job = job.model_copy(update={"export_digest": snapshot.export_digest})
    solver = tmp_path / "workspace"
    solver.mkdir()
    (solver / ".git").mkdir()
    manifest = broker.sessions[old_job.job_id].manifest

    class Docker:
        def __init__(self):
            self.containers = {}
            self.networks = {}
            self.credentials = []

        async def invoke(self, *args):
            if args[:2] == ("image", "inspect"):
                if fault == "preflight":
                    raise OSError("image unavailable SECRET_CANARY")
                return json.dumps(
                    [{"RepoDigests": [args[2]], "Os": "linux", "Architecture": "amd64"}]
                )
            if args[:2] == ("network", "create"):
                name = args[-1]
                self.networks[name] = {
                    "Internal": True,
                    "Options": {
                        "com.docker.network.bridge.gateway_mode_ipv4": "isolated"
                    },
                    "Labels": {"crucible.improvement.job": str(job.job_id)},
                }
                assert "com.docker.network.bridge.gateway_mode_ipv4=isolated" in args
                return name
            if args[:2] == ("network", "inspect"):
                return json.dumps([self.networks[args[-1]]])
            if args[:2] == ("network", "connect"):
                return ""
            if args[0] == "create":
                name = args[args.index("--name") + 1]
                self.containers[name] = {
                    "Config": {"Labels": {"crucible.improvement.job": str(job.job_id)}},
                    "State": {"Running": False},
                }
                if name.startswith("improvement-worker") and fault == "create":
                    raise ValueError("crash after create before acknowledgment")
                return name
            if args[0] == "start":
                self.containers[args[1]]["State"]["Running"] = True
                return ""
            if args[0] == "inspect":
                return json.dumps([self.containers[args[1]]])
            if args[0] == "kill":
                if fault in ("cleanup", "stream_cleanup") and "worker" in args[1]:
                    raise ValueError("cleanup failed SECRET_CANARY")
                self.containers[args[1]]["State"]["Running"] = False
                return ""
            raise AssertionError(args)

        async def attach(self, identity, limit, credential=None):
            self.containers[identity]["State"]["Running"] = True
            self.credentials.append(credential)
            if fault == "worker_exit":
                return 1, ()
            if fault == "cancel":
                import asyncio

                raise asyncio.CancelledError
            if fault in ("stream", "stream_cleanup"):
                from uuid import uuid5

                from crucible.engine.gateway import ModelError, PreparedModelRequest

                class Upstream:
                    calls = 0

                    async def stream(self, request):
                        self.calls += 1
                        yield ModelError("provider_stream_failed", "SECRET_CANARY")

                upstream = Upstream()
                broker.gateway.upstream = upstream
                request = PreparedModelRequest(
                    uuid5(job.job_id, "run"), uuid4(), job.model.model_id, (), (), 100
                )
                events = [
                    e async for e in broker.model(credential, job.job_id, request)
                ]
                assert events[0].code == "provider_stream_failed"
                assert upstream.calls == 1
            state = tmp_path / "controller/jobs" / str(job.job_id) / "state"
            payload = dict(
                schema_version=1,
                job_id=str(job.job_id),
                trial_id=str(job.trial_id),
                export_digest=job.export_digest,
                termination="behavior_error"
                if fault == "behavior"
                else "provider_error"
                if fault in ("stream", "stream_cleanup")
                else "completed",
                trace_ref="trace.json",
                usage_ref="usage.json",
                detail_code=None,
            )
            (state / "observation.json").write_text(json.dumps(payload))
            return 0, ()

    docker = Docker()
    supervisor = WorkerSupervisor(
        store,
        broker,
        docker,
        tmp_path / "controller",
        manifest,
        "http://host.docker.internal:1234",
    )
    try:
        result = await supervisor.execute(job, snapshot, solver, harness_export=harness)
        assert result.outcome is not None
        if fault == "preflight":
            assert result.cleanup_confirmed
            assert result.outcome.stage == "admission"
            assert result.outcome.failure.dispatch == "not_sent"
            assert result.outcome.resource_reservation_id is None
            assert await store.effects(job.experiment_id) == ()
            assert job.job_id not in broker.sessions
            assert len(await store.events(job.experiment_id, "job_outcome")) == 1
            return
        assert (
            "SECRET_CANARY"
            not in (
                tmp_path / "controller/jobs" / str(job.job_id) / "job-outcome.json"
            ).read_text()
        )
        if fault in (
            "create",
            "worker_exit",
            "cancel",
            "cleanup",
            "stream",
            "stream_cleanup",
        ):
            assert result.outcome.termination == (
                "interrupted" if fault == "cancel" else "failed"
            )
            assert result.outcome.failure.category == (
                "cancellation"
                if fault == "cancel"
                else "cleanup"
                if fault == "cleanup"
                else "provider_stream_failed"
                if fault in ("stream", "stream_cleanup")
                else "execution"
            )
            assert result.cleanup_confirmed == (
                fault not in ("cleanup", "stream_cleanup")
            )
            assert len(await store.events(job.experiment_id, "job_outcome")) == 1
            if fault in ("stream", "stream_cleanup"):
                assert await store.unsettled_usage(job.experiment_id)
                assert result.outcome.failure.origin == "model"
            if fault == "stream_cleanup":
                assert result.outcome.cleanup_failure.category == "cleanup"
        else:
            assert result.outcome.termination == "completed"
            assert result.cleanup_confirmed
            assert docker.credentials[0]
        if fault not in ("cleanup", "stream_cleanup"):
            assert all(not c["State"]["Running"] for c in docker.containers.values())
        assert not any(
            p.name == "credential" for p in (tmp_path / "controller").rglob("*")
        )
        assert all(
            e["status"] == "stopped" for e in await store.effects(job.experiment_id)
        ) == (fault not in ("cleanup", "stream_cleanup"))
    finally:
        await store.close()
