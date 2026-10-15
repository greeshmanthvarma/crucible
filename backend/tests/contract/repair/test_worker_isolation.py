"""Launch-policy tests and real Docker containment probes, never repair metrics."""

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.worker_protocol import WorkerJob
from crucible.repair.worker_supervisor import (
    WorkerSupervisor,
    export_source,
    relay_allows,
    worker_container_arguments,
)
from crucible.sandbox.docker_client import SubprocessDockerClient
from tests.unit.repair.test_worker_protocol import job_payload, observed

IMAGE = "repair-worker@sha256:" + "a" * 64


def mount_paths(arguments):
    mounts = {}
    for index, item in enumerate(arguments):
        if item == "--mount":
            options = dict(
                part.split("=", 1)
                for part in arguments[index + 1].split(",")
                if "=" in part
            )
            mounts[options["dst"]] = Path(options["src"])
    return mounts


def launch_args(tmp_path, *, network="none"):
    paths = {
        name: tmp_path / name
        for name in ("harness", "workspace", "state", "job", "driver")
    }
    for path in paths.values():
        path.mkdir()
    job = WorkerJob.model_validate(job_payload())
    return worker_container_arguments(job, image=IMAGE, network=network, **paths)


def test_worker_launch_has_exact_mounts_no_secrets_socket_and_hardening(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-host-only-key")
    arguments = launch_args(tmp_path)
    assert arguments[0] == "create"
    assert arguments[arguments.index("--user") + 1] == "65532:65532"
    assert "--read-only" in arguments
    assert arguments[arguments.index("--network") + 1] == "none"
    assert arguments[arguments.index("--cpus") + 1] in {"1", "1.0"}
    assert arguments[arguments.index("--memory") + 1] == str(1024**3)
    assert arguments[arguments.index("--pids-limit") + 1] == "256"
    assert arguments[arguments.index("--cap-drop") + 1] == "ALL"
    assert arguments[arguments.index("--security-opt") + 1] == "no-new-privileges"
    mounts = mount_paths(arguments)
    assert set(mounts) == {"/harness", "/workspace", "/state", "/job", "/driver"}
    for target in ("/harness", "/job", "/driver"):
        option = next(
            item
            for item in arguments
            if f"dst={target}," in item or item.endswith(f"dst={target}")
        )
        assert "readonly" in option or "ro=true" in option
    rendered = " ".join(arguments)
    for forbidden in (
        "docker.sock",
        "synthetic-host-only-key",
        "OPENAI_API_KEY",
        "--privileged",
        "--pid=host",
        "--network=host",
    ):
        assert forbidden not in rendered
    assert IMAGE in arguments


def test_worker_rejects_unpinned_images_and_host_network(tmp_path):
    arguments = launch_args(tmp_path)
    assert IMAGE in arguments
    paths = {
        name: tmp_path / name
        for name in ("harness", "workspace", "state", "job", "driver")
    }
    for overrides in (
        {"image": "worker:latest", "network": "none"},
        {"image": IMAGE, "network": "host"},
    ):
        with pytest.raises(ValueError):
            worker_container_arguments(
                WorkerJob.model_validate(job_payload()), **paths, **overrides
            )


async def test_export_never_imports_candidate_or_copies_protected_private_inputs(
    tmp_path,
):
    source, export = tmp_path / "source", tmp_path / "export"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    (source / ".gitignore").write_text("private/\n")
    (source / "source.py").write_text(
        "raise RuntimeError('candidate must never be imported on host')"
    )
    for name in (
        ".env",
        "private/incident.json",
        "tests/test_hidden.py",
        "evals/held-out.json",
    ):
        path = source / name
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text("synthetic private input")
    await export_source(source, export)
    assert (export / "source.py").is_file()
    for name in (".git", ".env", "private", "tests", "evals"):
        assert not (export / name).exists()


@pytest.mark.parametrize(
    "method,path,allowed",
    [
        ("POST", "/internal/repair-model/responses", True),
        ("CONNECT", "/internal/repair-model/responses", False),
        ("POST", "/api/tasks", False),
        ("GET", "/internal/repair-model/responses", False),
        ("POST", "http://example.com/", False),
        ("POST", "/internal/repair-model/responses/../tasks", False),
        ("POST", "/internal/repair-model/responses?url=http://example.com", False),
    ],
)
def test_relay_is_a_single_route_forwarder_not_an_open_proxy(method, path, allowed):
    assert relay_allows(method, path) is allowed


class FakeDocker:
    def __init__(self, job, *, fail=False, block=False):
        self.job, self.fail, self.block = job, fail, block
        self.arguments = ()
        self.events = []
        self.started = asyncio.Event()

    async def create_container(self, arguments):
        self.arguments = arguments
        self.events.append("create")
        return f"owned-{self.job.job_id}"

    async def start_attached(self, container_id, on_chunk):
        self.events.append("start")
        self.started.set()
        if self.fail:
            raise RuntimeError("deterministic Docker failure")
        if self.block:
            await asyncio.Event().wait()
        paths = mount_paths(self.arguments)
        assert not (paths["/workspace"] / "new.txt").exists()
        (paths["/workspace"] / "new.txt").write_text("worker mutation")
        (paths["/state"] / "observation.json").write_text(
            observed(self.job.job_id).model_dump_json()
        )
        return 0

    async def stop_container(self, container_id, grace_seconds):
        self.events.append("stop")

    async def kill_container(self, container_id):
        self.events.append("kill")

    async def remove_container(self, container_id):
        self.events.append("remove")


async def source_job(source):
    (source / "source.py").write_text("# deterministic launch fixture\n")
    for arguments in (
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.com"),
        ("add", "source.py"),
        ("commit", "-qm", "fixture"),
    ):
        subprocess.run(["git", *arguments], cwd=source, check=True, capture_output=True)
    identity = await capture_source_identity(source)
    return WorkerJob.model_validate(
        job_payload() | {"source_digest": identity.tree_digest}
    )


@pytest.mark.parametrize("fail", [False, True])
async def test_supervisor_collects_only_after_stop_and_cleans_owned_container(
    tmp_path, fail
):
    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    fixture.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    job = await source_job(source)
    docker = FakeDocker(job, fail=fail)
    supervisor = WorkerSupervisor(docker, image=IMAGE, root=tmp_path / "jobs")
    result = await supervisor.run(job, source, fixture, None)
    assert docker.events[-1] == "remove"
    assert "stop" in docker.events or "kill" in docker.events
    assert not (fixture / "new.txt").exists()
    assert result.termination == ("backend_error" if fail else "completed")
    if not fail:
        assert result.observation.job_id == job.job_id
        assert (result.workspace_snapshot / "new.txt").read_text() == "worker mutation"


async def test_supervisor_cancellation_always_cleans_owned_container(tmp_path):
    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    fixture.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    job = await source_job(source)
    docker = FakeDocker(job, block=True)
    supervisor = WorkerSupervisor(docker, image=IMAGE, root=tmp_path / "jobs")
    task = asyncio.create_task(supervisor.run(job, source, fixture, None))
    try:
        await asyncio.wait_for(docker.started.wait(), 2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert docker.events[-1] == "remove"
    assert "stop" in docker.events or "kill" in docker.events


@pytest.fixture(scope="module")
def real_worker_image():
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI unavailable; isolation is not verified")
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=15)
    except subprocess.TimeoutExpired:
        pytest.skip("Docker daemon timed out; isolation is not verified")
    if result.returncode:
        pytest.skip("Docker daemon unavailable; isolation is not verified")
    image = os.environ.get("CRUCIBLE_REPAIR_WORKER_IMAGE")
    if not image:
        pytest.skip(
            "CRUCIBLE_REPAIR_WORKER_IMAGE is not configured; isolation is not verified"
        )
    if "@sha256:" not in image:
        pytest.fail("Repair worker test image must be digest-pinned")
    result = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, timeout=20
    )
    if result.returncode:
        pytest.skip("Pinned worker image unavailable; isolation is not verified")
    return image


async def real_command(
    tmp_path, image, script, *, timeout=20, job_id=None, credential=None, gateway=None
):
    source = Path(__file__).resolve().parents[4]
    identity = await capture_source_identity(source)
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    job = WorkerJob.model_validate(
        job_payload()
        | {
            "job_id": job_id or job_payload()["job_id"],
            "source_digest": identity.tree_digest,
            "input_manifest": {
                "kind": "tool",
                "tool_name": "execute_command",
                "arguments": {
                    "executable": "python",
                    "arguments": ["-c", script],
                    "cwd": ".",
                    "timeoutSeconds": timeout,
                    "reason": "Deterministic containment probe",
                    "limits": {"outputBytes": 4096},
                },
            },
        }
    )
    supervisor = WorkerSupervisor(
        SubprocessDockerClient(), image=image, root=tmp_path / "jobs", gateway=gateway
    )
    return await supervisor.run(job, source, fixture, credential)


@pytest.mark.docker
async def test_worker_cannot_read_host_file_or_docker_socket(
    tmp_path, real_worker_image
):
    sentinel = tmp_path / "host-private-sentinel"
    sentinel.write_text("synthetic-host-only-file")
    script = f"""
import os
assert not os.path.exists({str(sentinel)!r})
assert not os.path.exists('/var/run/docker.sock')
assert not any(key.endswith(('_KEY', '_SECRET', '_PASSWORD')) for key in os.environ)
assert os.getuid() != 0
print('containment-ok')
"""
    result = await real_command(tmp_path, real_worker_image, script)
    assert result.termination == "completed"
    assert result.observation.status == "completed"
    assert result.observation.command_records
    assert result.observation.command_records[-1]["exit_code"] == 0
    assert "containment-ok" in json.dumps(result.observation.tool_result)


@pytest.mark.docker
async def test_worker_cannot_reach_external_network(tmp_path, real_worker_image):
    # No DNS dependency: public IP and Docker Desktop's host alias are both denied.
    script = """
import socket
for host, port in [('1.1.1.1', 443), ('host.docker.internal', 8000)]:
    try:
        connection = socket.create_connection((host, port), timeout=2)
    except OSError:
        continue
    connection.close()
    raise AssertionError('worker escaped network containment')
print('network-denied')
"""
    result = await real_command(tmp_path, real_worker_image, script)
    assert result.termination == "completed"
    assert result.observation.command_records[-1]["exit_code"] == 0
    assert "network-denied" in json.dumps(result.observation.tool_result)


@pytest.mark.docker
async def test_incontainer_command_preserves_argv_and_kills_process_group(
    tmp_path, real_worker_image
):
    script = """
import asyncio, pathlib, sys
from dataclasses import replace
sys.path.insert(0, '/harness/backend/src')
from crucible.domain.commands import CommandSpec, CommandLimits, CommandNetwork
from crucible.domain.ids import new_id
from crucible.repair.incontainer_commands import InContainerSandbox
from crucible.sandbox.protocol import SandboxRequest, SandboxTermination

async def probe():
    root = pathlib.Path('/workspace')
    image = 'probe@sha256:' + 'a' * 64
    backend = InContainerSandbox(root, image)
    child = (
        "import time,pathlib; time.sleep(3); "
        "pathlib.Path('/workspace/leaked-child').write_text('leak')"
    )
    program = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
        "print('captured-output',flush=True); time.sleep(60)"
    )
    spec = CommandSpec(sys.executable, ('-c', program), '.', 1,
        CommandNetwork.NONE, {}, image, 'process group probe',
        CommandLimits(1, 1024**3, 256, 4096))
    output = []
    request = SandboxRequest(new_id(), new_id(), new_id(), root, spec, ())
    outcome = await backend.execute(request, lambda chunk: output.append(chunk.data))
    assert outcome.termination is SandboxTermination.TIMED_OUT
    assert b'captured-output' in b''.join(output)
    await asyncio.sleep(3.2)
    assert not (root / 'leaked-child').exists()
    ready = asyncio.Event()
    cancelled_program = program.replace('leaked-child', 'cancelled-child')
    cancelled_spec = replace(spec, arguments=('-c', cancelled_program),
                             timeout_seconds=10)
    request = SandboxRequest(new_id(), new_id(), new_id(), root,
                             cancelled_spec, ())
    def received(chunk):
        if b'captured-output' in chunk.data:
            ready.set()
    task = asyncio.create_task(backend.execute(request, received))
    await asyncio.wait_for(ready.wait(), 2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError('cancellation was not propagated')
    await asyncio.sleep(3.2)
    assert not (root / 'cancelled-child').exists()
    assert backend.records[-1]['termination'] == 'cancelled'
    print('process-group-terminated')

asyncio.run(probe())
"""
    result = await real_command(tmp_path, real_worker_image, script, timeout=15)
    assert result.termination == "completed"
    assert result.observation.command_records[-1]["exit_code"] == 0
    assert "process-group-terminated" in json.dumps(result.observation.tool_result)
    assert not (result.workspace_snapshot / "leaked-child").exists()


async def test_changed_source_never_starts_worker(tmp_path):
    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    fixture.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    job = await source_job(source)
    (source / "source.py").write_text("changed after job was frozen")
    docker = FakeDocker(job)
    result = await WorkerSupervisor(docker, image=IMAGE, root=tmp_path / "jobs").run(
        job, source, fixture, None
    )
    assert result.termination == "backend_error"
    assert not docker.events


@pytest.mark.parametrize("fail", [False, True])
async def test_fresh_job_uses_internal_network_relay_and_always_revokes(tmp_path, fail):
    from datetime import timedelta

    from crucible.repair.gateway import GatewayDenied, ScopedModelGateway
    from tests.integration.repair.test_gateway import Clock, Upstream
    from tests.unit.repair.test_worker_protocol import prepared_request

    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    fixture.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    job = await source_job(source)
    gateway = ScopedModelGateway(
        Upstream(),
        clock=Clock(),
        endpoint="http://host.docker.internal:8000/internal/repair-model/responses",
    )
    credential = gateway.issue(
        job.job_id,
        "test-model",
        {"max_requests": 1, "max_tokens": 256},
        timedelta(seconds=30),
    )

    class RecordingSupervisor(WorkerSupervisor):
        def __init__(self):
            super().__init__(
                FakeDocker(job, fail=fail),
                image=IMAGE,
                root=tmp_path / "jobs",
                gateway=gateway,
            )
            self.operations = []

        async def _docker(self, *args):
            self.operations.append(args)
            return "relay-id" if args[0] == "create" else "owned-resource"

    supervisor = RecordingSupervisor()
    await supervisor.run(job, source, fixture, credential)
    network = f"crucible-repair-{job.job_id}"
    create_network = next(
        args for args in supervisor.operations if args[:2] == ("network", "create")
    )
    assert "--internal" in create_network
    assert create_network[-1] == network
    relay = next(args for args in supervisor.operations if args[0] == "create")
    assert relay[relay.index("--network") + 1] == "bridge"
    assert (
        "network",
        "connect",
        "--alias",
        "repair-gateway",
        network,
        "relay-id",
    ) in supervisor.operations
    worker = supervisor.docker.arguments
    assert worker[worker.index("--network") + 1] == network
    assert supervisor.operations[-2:] == [
        ("rm", "--force", "relay-id"),
        ("network", "rm", network),
    ]
    with pytest.raises(GatewayDenied):
        await gateway.reserve(credential.token, job.job_id, prepared_request())


async def test_export_rejects_a_known_provider_key_in_source(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-source-credential")
    (source / "settings.py").write_text("key = 'synthetic-source-credential'")
    with pytest.raises(ValueError, match="credential"):
        await export_source(source, tmp_path / "export")


@pytest.mark.docker
async def test_gateway_enabled_worker_has_only_scoped_relay_access(
    tmp_path, real_worker_image
):
    import socket
    from datetime import timedelta
    from uuid import uuid4

    import uvicorn
    from fastapi import FastAPI

    from crucible.api.repair_gateway import router
    from crucible.repair.gateway import ScopedModelGateway
    from tests.integration.repair.test_gateway import Upstream

    listener = socket.socket()
    listener.bind(("0.0.0.0", 0))
    listener.listen(16)
    port = listener.getsockname()[1]
    upstream = Upstream()
    # Use a real clock for an actual bounded worker job.
    from crucible.domain.clock import SystemClock

    gateway = ScopedModelGateway(
        upstream,
        clock=SystemClock(),
        endpoint=f"http://host.docker.internal:{port}/internal/repair-model/responses",
    )
    app = FastAPI()
    app.state.repair_gateway = gateway
    app.include_router(router)
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="off"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(100):
            if server.started:
                break
            if serving.done():
                await serving
            await asyncio.sleep(0.05)
        assert server.started
        job_id = uuid4()
        credential = gateway.issue(
            job_id,
            "test-model",
            {"max_requests": 1, "max_tokens": 256},
            timedelta(seconds=120),
        )
        script = r"""
import http.client, json, pathlib, socket, uuid
values = pathlib.Path('/proc/1/environ').read_bytes().decode().split('\x00')
environment = dict(item.split('=',1) for item in values if '=' in item)
job = json.loads(pathlib.Path('/job/request.json').read_text())
request = {
    'schema_version': 1, 'job_id': job['job_id'],
    'request': {
        'schema_version': 1, 'run_id': str(uuid.uuid4()),
        'step_id': str(uuid.uuid4()), 'model': 'test-model',
        'messages': [{'role': 'user', 'parts': [
            {'kind': 'text', 'text_content': 'probe'}]}],
        'tools': [], 'max_output_tokens': 32,
    },
}
connection = http.client.HTTPConnection('repair-gateway',8080,timeout=10)
connection.request('POST','/internal/repair-model/responses',json.dumps(request),
    {'Authorization':'Bearer '+environment['REPAIR_GATEWAY_TOKEN'],
     'Content-Type':'application/json'})
response = connection.getresponse()
assert response.status == 200, response.status
assert b'deterministic' in response.read()
connection.close()
for method,path in [
    ('POST','/api/health'), ('CONNECT','/internal/repair-model/responses')
]:
    connection = http.client.HTTPConnection('repair-gateway',8080,timeout=3)
    connection.request(method,path)
    response = connection.getresponse()
    assert response.status == 403, response.status
    response.read()
    connection.close()
for host,port in [('1.1.1.1',443),('host.docker.internal',HOST_PORT)]:
    try:
        connection = socket.create_connection((host,port),timeout=1)
    except OSError:
        continue
    connection.close()
    raise AssertionError('unrestricted network escape')
print('scoped-relay-ok')
""".replace("HOST_PORT", str(port))
        result = await real_command(
            tmp_path,
            real_worker_image,
            script,
            timeout=25,
            job_id=job_id,
            credential=credential,
            gateway=gateway,
        )
        assert result.termination == "completed"
        assert result.observation.command_records[-1]["exit_code"] == 0, (
            result.observation
        )
        assert "scoped-relay-ok" in json.dumps(result.observation.tool_result)
        assert len(upstream.requests) == 1
        assert gateway.usage(job_id).requests == 1
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 5)
        listener.close()
