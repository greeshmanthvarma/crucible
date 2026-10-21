"""Owned Docker effects. No image builds/pulls or candidate host execution."""

import asyncio
import json
import os
import signal
import stat
import time
from pathlib import Path
from typing import Literal, Protocol, overload
from uuid import UUID

from crucible.benchmarks.evidence import atomic_json
from crucible.domain.clock import SystemClock
from crucible.domain.commands import CommandSpec
from crucible.improvement.broker import ImprovementBroker, WorkspaceBinding
from crucible.improvement.contracts import (
    ExecutionManifest,
    HarnessSnapshot,
    UsageAmounts,
)
from crucible.improvement.source import _digest, safe_path
from crucible.improvement.store import BudgetDenied, ExecutionStore, QuotaExhausted
from crucible.improvement.worker_protocol import (
    IsolatedExecution,
    IsolatedRoleExecution,
    JobFailure,
    JobOutcome,
    RoleJob,
    WorkerJob,
    WorkerRequest,
    job_environment,
    read_role_observation,
    read_worker_observation,
)
from crucible.repair.gateway import TrustedUsage
from crucible.repair.identity import canonical_json
from crucible.sandbox.protocol import (
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxTermination,
)

LABEL = "crucible.improvement.job"


class DockerPort(Protocol):
    async def invoke(self, *args: str) -> str: ...
    async def attach(
        self, identity: str, limit: int, credential: str | None = None
    ) -> tuple[int, tuple[OutputChunk, ...]]: ...


class DockerOutputLimit(ValueError):
    def __init__(self, chunks: tuple[OutputChunk, ...], original: int) -> None:
        self.chunks, self.original = chunks, original
        super().__init__("Docker output exceeds frozen limit")


class DockerRuntime:
    async def _run(
        self, args: tuple[str, ...], limit: int, input_data: bytes | None = None
    ) -> tuple[int, tuple[OutputChunk, ...]]:
        process = await asyncio.create_subprocess_exec(
            "docker",
            *args,
            stdin=asyncio.subprocess.PIPE
            if input_data is not None
            else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        chunks: list[OutputChunk] = []
        total = 0
        lock = asyncio.Lock()

        async def pump(reader: asyncio.StreamReader, stream: OutputStream) -> None:
            nonlocal total
            while data := await reader.read(8192):
                async with lock:
                    total += len(data)
                    if total > limit:
                        keep = data[: max(0, limit - (total - len(data)))]
                        if keep:
                            chunks.append(OutputChunk(stream, len(chunks) + 1, keep))
                        raise DockerOutputLimit(tuple(chunks), total)
                    chunks.append(OutputChunk(stream, len(chunks) + 1, data))

        assert process.stdout and process.stderr
        tasks = [
            asyncio.create_task(pump(process.stdout, OutputStream.STDOUT)),
            asyncio.create_task(pump(process.stderr, OutputStream.STDERR)),
        ]
        try:
            if input_data is not None:
                assert process.stdin is not None
                process.stdin.write(input_data)
                await process.stdin.drain()
                process.stdin.close()
            await asyncio.gather(*tasks)
            return await process.wait(), tuple(chunks)
        finally:
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            await process.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def invoke(self, *args: str) -> str:
        async with asyncio.timeout(30):
            code, chunks = await self._run(args, 1_048_576)
        if code:
            raise ValueError("owned Docker operation failed")
        return (
            b"".join(c.data for c in chunks if c.stream == OutputStream.STDOUT)
            .decode()
            .strip()
        )

    async def attach(
        self, identity: str, limit: int, credential: str | None = None
    ) -> tuple[int, tuple[OutputChunk, ...]]:
        args = (
            ("start", "--attach", "--interactive", identity)
            if credential
            else ("start", "--attach", identity)
        )
        return await self._run(
            args, limit, (credential + "\n").encode() if credential else None
        )


def labels(job: WorkerRequest) -> dict[str, str]:
    return {
        LABEL: str(job.job_id),
        "crucible.improvement.experiment": str(job.experiment_id),
        **(
            {"crucible.improvement.trial": str(job.trial_id)}
            if isinstance(job, WorkerJob)
            else {"crucible.improvement.role": str(job.role_id)}
        ),
    }


def label_args(job: WorkerRequest) -> list[str]:
    result: list[str] = []
    for key, value in labels(job).items():
        result.extend(("--label", key + "=" + value))
    return result


def mount(path: Path, target: str, readonly: bool = False) -> tuple[str, str]:
    safe_path(path)
    if "," in str(path):
        raise ValueError("unsupported mount path")
    return "--mount", f"type=bind,source={path},target={target}" + (
        ",readonly" if readonly else ""
    )


def worker_container_arguments(
    job: WorkerRequest,
    manifest: ExecutionManifest,
    *,
    network: str,
    harness: Path,
    workspace: Path,
    state: Path,
    job_root: Path,
    driver: Path,
) -> tuple[str, ...]:
    return tuple(
        [
            "create",
            "--pull=never",
            "--name",
            "improvement-worker-" + str(job.job_id),
            "--interactive",
            "--platform",
            manifest.worker_platform,
            "--network",
            network,
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--pids-limit",
            "256",
            "--cpus",
            str(manifest.grader.cpus),
            "--memory",
            str(manifest.grader.memory_bytes),
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=67108864",
            "--env",
            "PYTHONDONTWRITEBYTECODE=1",
            "--env",
            "HOME=/state",
            *label_args(job),
            *mount(harness, "/harness", True),
            *mount(
                workspace,
                "/workspace",
                isinstance(job, RoleJob) and job.job_kind != "proposal",
            ),
            *mount(workspace / ".git", "/workspace/.git", True),
            *mount(state, "/state"),
            *mount(job_root, "/job", True),
            *mount(driver, "/driver/launch.py", True),
            manifest.worker_image,
            "/opt/venv/bin/python",
            "-I",
            "/driver/launch.py",
        ]
    )


async def verify_stopped(docker: DockerPort, identity: str, job: WorkerRequest) -> None:
    data = json.loads(await docker.invoke("inspect", identity))
    if len(data) != 1 or data[0]["Config"]["Labels"].get(LABEL) != str(job.job_id):
        raise ValueError("ambiguous Docker ownership")
    if data[0]["State"]["Running"]:
        await docker.invoke("kill", identity)
    data = json.loads(await docker.invoke("inspect", identity))
    if data[0]["State"]["Running"]:
        raise ValueError("container cleanup uncertain")


class DockerCommands:
    def __init__(self, store: ExecutionStore, docker: DockerPort) -> None:
        self.store, self.docker = store, docker
        self.active: dict[UUID, list[tuple[UUID, str, WorkerRequest]]] = {}

    async def execute(
        self,
        job: WorkerRequest,
        binding: WorkspaceBinding,
        command_id: UUID,
        spec: CommandSpec,
    ) -> tuple[tuple[OutputChunk, ...], SandboxOutcome]:
        name = "improvement-command-" + str(command_id)
        effect = await self.store.begin_effect(
            job.experiment_id, job.trial_id, "command", {"name": name}, labels(job)
        )
        self.active.setdefault(job.job_id, []).append((effect, name, job))
        args = [
            "create",
            "--pull=never",
            "--name",
            name,
            "--platform",
            binding.platform,
            "--network",
            "none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--pids-limit",
            str(spec.limits.pids),
            "--cpus",
            str(spec.limits.cpus),
            "--memory",
            str(spec.limits.memory_bytes),
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=67108864",
            *label_args(job),
            *mount(binding.path, "/workspace"),
            *mount(binding.path / ".git", "/workspace/.git", True),
            "--workdir",
            "/workspace" + ("/" + spec.cwd if spec.cwd != "." else ""),
            binding.image,
            spec.executable,
            *spec.arguments,
        ]
        clock = SystemClock()
        started = clock.now()
        try:
            identity = await self.docker.invoke(*args)
            await self.store.effect_state(effect, "active", identity)
            termination = SandboxTermination.COMPLETED
            code: int | None = None
            chunks: tuple[OutputChunk, ...] = ()
            original = 0
            try:
                async with asyncio.timeout(spec.timeout_seconds):
                    code, chunks = await self.docker.attach(
                        identity, spec.limits.output_bytes
                    )
                original = sum(len(c.data) for c in chunks)
            except TimeoutError:
                termination = SandboxTermination.TIMED_OUT
            except DockerOutputLimit as error:
                termination = SandboxTermination.OUTPUT_LIMIT
                chunks, original = error.chunks, error.original
            await verify_stopped(self.docker, identity, job)
            await self.store.effect_state(effect, "stopped", identity)
            size = sum(len(c.data) for c in chunks)
            return chunks, SandboxOutcome(
                code,
                termination,
                started,
                clock.now(),
                binding.image,
                identity,
                original,
                size,
                original > size,
            )
        finally:
            await self._stop(effect, name, job)

    async def _stop(self, effect: UUID, name: str, job: WorkerRequest) -> None:
        try:
            await verify_stopped(self.docker, name, job)
            await self.store.effect_state(effect, "stopped")
        except BaseException:
            await self.store.effect_state(effect, "uncertain")
            raise

    async def stop(self, job_id: UUID) -> None:
        failures = []
        for effect, name, job in self.active.get(job_id, []):
            try:
                await self._stop(effect, name, job)
            except Exception as error:
                failures.append(error)
        if failures:
            raise ExceptionGroup("command cleanup uncertain", failures)


class WorkerSupervisor:
    def __init__(
        self,
        store: ExecutionStore,
        broker: ImprovementBroker,
        docker: DockerPort,
        root: Path,
        manifest: ExecutionManifest,
        broker_origin: str,
    ) -> None:
        self.store, self.broker, self.docker = store, broker, docker
        self.root, self.manifest, self.broker_origin = root, manifest, broker_origin
        self.owned: dict[UUID, list[tuple[UUID, str, str, WorkerRequest]]] = {}
        self.broker.pause_worker = self.pause_worker

    async def pause_worker(self, job_id: UUID, paused: bool) -> None:
        workers = [
            (name, job)
            for _, kind, name, job in self.owned.get(job_id, [])
            if kind == "worker"
        ]
        if len(workers) != 1:
            raise ValueError("approval pause requires one owned worker")
        name, job = workers[0]
        info = json.loads(await self.docker.invoke("inspect", name))[0]
        if (
            info["Config"]["Labels"].get(LABEL) != str(job_id)
            or not info["State"]["Running"]
        ):
            raise ValueError("approval worker ownership/liveness mismatch")
        await self.docker.invoke("pause" if paused else "unpause", name)

    async def _effect(
        self, job: WorkerRequest, kind: str, name: str, args: tuple[str, ...]
    ) -> str:
        effect = await self.store.begin_effect(
            job.experiment_id, job.trial_id, kind, {"name": name}, labels(job)
        )
        self.owned.setdefault(job.job_id, []).append((effect, kind, name, job))
        identity = await self.docker.invoke(*args)
        await self.store.effect_state(effect, "active", identity)
        return identity

    async def _admission_failure(
        self,
        job: WorkerRequest,
        workspace: Path,
        category: Literal["execution", "quota"],
    ) -> IsolatedExecution | IsolatedRoleExecution:
        # Revoke first: persistence failure must never leave usable authority.
        self.broker.revoke(job.job_id)
        outcome = JobOutcome(
            job.job_id,
            job.experiment_id,
            job.trial_id if isinstance(job, WorkerJob) else job.role_id,
            job.harness_digest,
            job.repeat_index if isinstance(job, WorkerJob) else None,
            "failed",
            "admission",
            JobFailure(category, "controller", "not_sent"),
            True,
            None,
            "host_measured",
            "recover",
            UsageAmounts(),
            TrustedUsage(),
            (),
            None,
        )
        root = self.root / "jobs" / str(job.job_id)
        if root.is_dir():
            atomic_json(root / "job-outcome.json", outcome)
        await self.store.record_event(
            job.experiment_id,
            "job_outcome",
            {
                "job_id": str(job.job_id),
                "termination": "failed",
                "cleanup_confirmed": True,
                "stage": "admission",
                "failure_category": category,
                "dispatch": "not_sent",
                "outcome": json.loads(canonical_json(outcome)),
            },
        )
        await self.store.fail_run(job.experiment_id, category)
        execution_type = (
            IsolatedExecution if isinstance(job, WorkerJob) else IsolatedRoleExecution
        )
        return execution_type(
            None, workspace, root / "state", TrustedUsage(), (), True, 0.0, outcome
        )

    @overload
    async def execute(
        self,
        job: WorkerJob,
        snapshot: HarnessSnapshot,
        solver_export: Path,
        *,
        harness_export: Path,
    ) -> IsolatedExecution: ...

    @overload
    async def execute(
        self,
        job: RoleJob,
        snapshot: HarnessSnapshot,
        solver_export: Path,
        *,
        harness_export: Path,
    ) -> IsolatedRoleExecution: ...

    async def execute(
        self,
        job: WorkerRequest,
        snapshot: HarnessSnapshot,
        solver_export: Path,
        *,
        harness_export: Path,
    ) -> IsolatedExecution | IsolatedRoleExecution:
        if (job.experiment_id, job.harness_digest, job.export_digest) != (
            self.manifest.experiment_id,
            snapshot.source_digest,
            snapshot.export_digest,
        ):
            raise ValueError("worker snapshot identity mismatch")
        if os.getuid() == 0:
            raise ValueError("worker requires an unprivileged operator UID")
        # Export bytes are verified before dispatch; no imports or pickle.
        files: dict[str, tuple[int, bytes]] = {}
        total = 0
        for path in harness_export.rglob("*"):
            safe_path(path)
            if path.is_file():
                if path.stat().st_size > 2 * 1024 * 1024:
                    raise ValueError("export file oversized")
                total += path.stat().st_size
                if total > 32 * 1024 * 1024 or len(files) >= 4096:
                    raise ValueError("export exceeds frozen protocol limits")
                files[path.relative_to(harness_export).as_posix()] = (
                    0o100755 if path.stat().st_mode & 0o111 else 0o100644,
                    path.read_bytes(),
                )
        if _digest(files) != snapshot.export_digest:
            raise ValueError("immutable export drift")
        try:
            for image in (self.manifest.worker_image, self.manifest.relay_image):
                info = json.loads(await self.docker.invoke("image", "inspect", image))[
                    0
                ]
                if (
                    image not in info.get("RepoDigests", [])
                    or (info.get("Os") + "/" + info.get("Architecture"))
                    != self.manifest.worker_platform
                ):
                    raise ValueError("worker image digest/platform drift")
        except (ValueError, OSError):
            return await self._admission_failure(job, solver_export, "execution")
        root = self.root / "jobs" / str(job.job_id)
        root.mkdir(parents=True, exist_ok=False, mode=0o700)
        state = root / "state"
        state.mkdir()
        job_root = root / "job"
        job_root.mkdir()
        (job_root / "request.json").write_text(job.model_dump_json())
        credential = self.broker.issue(
            job,
            self.manifest,
            WorkspaceBinding(solver_export, *job_environment(job, self.manifest)),
        )
        driver = root / "launch.py"
        driver.write_text(
            "import sys, runpy\n"
            "sys.path.insert(0, '/harness/backend/src')\n"
            "runpy.run_module('crucible.improvement.worker', run_name='__main__')\n"
        )
        started = time.monotonic()
        try:
            reservation = await self.store.reserve_usage(
                job.experiment_id,
                job.trial_id,
                UsageAmounts(
                    worker_wall_seconds=job.wall_deadline_seconds,
                    active_seconds=job.limits.max_active_seconds,
                    output_bytes=job.output_limit_bytes,
                ),
                kind="worker",
                job_id=job.job_id,
            )
        except BudgetDenied as error:
            return await self._admission_failure(
                job,
                solver_export,
                "quota" if isinstance(error, QuotaExhausted) else "execution",
            )
        active_started: float | None = None
        active_elapsed = 0.0
        cleanup_verified = False
        output_known = True
        observed_output_bytes = 0
        stdout_chunks: tuple[OutputChunk, ...] = ()
        observation = None
        failure: BaseException | None = None
        stage = "execution"
        network = "improvement-network-" + str(job.job_id)
        relay = "improvement-relay-" + str(job.job_id)
        try:
            async with asyncio.timeout(job.wall_deadline_seconds):
                await self._effect(
                    job,
                    "network",
                    network,
                    tuple(
                        [
                            "network",
                            "create",
                            "--internal",
                            "--ipv6=false",
                            "--opt",
                            "com.docker.network.bridge.gateway_mode_ipv4=isolated",
                            *label_args(job),
                            network,
                        ]
                    ),
                )
                network_info = json.loads(
                    await self.docker.invoke("network", "inspect", network)
                )[0]
                if (
                    not network_info.get("Internal")
                    or network_info.get("Options", {}).get(
                        "com.docker.network.bridge.gateway_mode_ipv4"
                    )
                    != "isolated"
                ):
                    raise ValueError("isolated network driver policy unavailable")
                relay_args = tuple(
                    [
                        "create",
                        "--pull=never",
                        "--name",
                        relay,
                        "--platform",
                        self.manifest.worker_platform,
                        "--network",
                        network,
                        "--network-alias",
                        "relay",
                        "--read-only",
                        "--cap-drop=ALL",
                        "--security-opt=no-new-privileges",
                        "--user",
                        "65534:65534",
                        "--pids-limit",
                        "64",
                        "--memory",
                        "134217728",
                        "--cpus",
                        "1",
                        "--add-host",
                        "host.docker.internal:host-gateway",
                        "--env",
                        "IMPROVEMENT_BROKER_ORIGIN=" + self.broker_origin,
                        *label_args(job),
                        self.manifest.relay_image,
                        "/opt/venv/bin/python",
                        "-I",
                        "/opt/relay.py",
                    ]
                )
                relay_id = await self._effect(job, "relay", relay, relay_args)
                await self.docker.invoke("network", "connect", "bridge", relay_id)
                await self.docker.invoke("start", relay_id)
                worker = "improvement-worker-" + str(job.job_id)
                worker_id = await self._effect(
                    job,
                    "worker",
                    worker,
                    worker_container_arguments(
                        job,
                        self.manifest,
                        network=network,
                        harness=harness_export,
                        workspace=solver_export,
                        state=state,
                        job_root=job_root,
                        driver=driver,
                    ),
                )
                active_started = time.monotonic()
                output_known = False
                attach = asyncio.create_task(
                    self.docker.attach(
                        worker_id, job.output_limit_bytes, credential.token
                    )
                )
                monitor = asyncio.create_task(self._monitor(job, active_started))
                try:
                    done, _ = await asyncio.wait(
                        (attach, monitor), return_when=asyncio.FIRST_COMPLETED
                    )
                    if monitor in done:
                        await monitor
                        raise ValueError("worker stopped by protected controller")
                    code, stdout_chunks = await attach
                    output_known = True
                finally:
                    attach.cancel()
                    monitor.cancel()
                    await asyncio.gather(attach, monitor, return_exceptions=True)
                    active_elapsed = max(
                        0.0,
                        time.monotonic()
                        - active_started
                        - self.broker.approval_wait_seconds(job.job_id),
                    )
                if code:
                    raise ValueError("worker process failed")
            stage = "cleanup"
            await self.stop(job.job_id)
            cleanup_verified = True
            stage = "observation"
            raw = state / "observation.json"
            data = read_regular_artifact(raw, 1_048_576)
            observation = (
                read_worker_observation(data, job)
                if isinstance(job, WorkerJob)
                else read_role_observation(data, job)
            )
        except BaseException as error:
            failure = error
            if isinstance(error, DockerOutputLimit):
                stdout_chunks = error.chunks
                observed_output_bytes = error.original
                output_known = False
        else:
            failure = None
        session = self.broker.sessions[job.job_id]
        root_failure = (
            JobFailure(
                session.failure,
                session.failure_origin,
                "possibly_sent" if session.failure_origin == "model" else "unknown",
            )
            if session.failure
            else JobFailure(
                "cancellation"
                if isinstance(failure, asyncio.CancelledError)
                else "cleanup"
                if stage == "cleanup"
                else "execution",
                "controller"
                if isinstance(failure, asyncio.CancelledError)
                else "cleanup"
                if stage == "cleanup"
                else "worker",
            )
            if failure
            else None
        )
        cleanup_confirmed = True
        cleanup_failure = None
        try:
            if not cleanup_verified:
                await self.stop(job.job_id)
        except Exception:
            cleanup_confirmed = False
            cleanup_failure = JobFailure("cleanup", "cleanup")
            if root_failure is None:
                root_failure = JobFailure("cleanup", "cleanup")
        elapsed = time.monotonic() - started
        measured = UsageAmounts(
            output_bytes=max(
                observed_output_bytes, sum(len(c.data) for c in stdout_chunks)
            ),
            active_seconds=int(active_elapsed + 0.999),
            worker_wall_seconds=int(elapsed + 0.999),
        )
        if cleanup_confirmed:
            if not output_known:
                await self.store.record_partial_usage(
                    reservation, measured, ("output_bytes",)
                )
            else:
                await self.store.settle_usage(reservation, measured)
        else:
            await self.store.mark_uncertain(reservation, measured)
        termination: Literal[
            "completed", "budget_exhausted", "failed", "interrupted"
        ] = (
            "interrupted"
            if (
                isinstance(failure, asyncio.CancelledError)
                or session.failure == "cancellation"
                or (observation and observation.termination == "cancelled")
            )
            else "failed"
            if root_failure
            else "budget_exhausted"
            if observation and observation.termination == "budget_exhausted"
            else "completed"
            if observation
            and observation.termination in ("completed", "behavior_error")
            else "failed"
        )
        if root_failure is None and termination == "interrupted":
            root_failure = JobFailure("cancellation", "controller")
        if root_failure is None and termination == "failed":
            root_failure = JobFailure("unknown", "worker")
        assert self.broker.gateway is not None
        reservation_ids = {
            str(e["reservation_id"])
            for e in await self.store.events(job.experiment_id, "usage_reserved")
            if e.get("job_id") == str(job.job_id)
        }
        unresolved = tuple(
            UUID(str(r["reservation_id"]))
            for r in await self.store.usage_records(job.experiment_id)
            if r["reservation_id"] in reservation_ids
            and r["status"] not in ("settled", "conservative")
        )
        outcome = JobOutcome(
            job.job_id,
            job.experiment_id,
            job.trial_id if isinstance(job, WorkerJob) else job.role_id,
            job.harness_digest,
            job.repeat_index if isinstance(job, WorkerJob) else None,
            termination,
            stage,
            root_failure,
            cleanup_confirmed,
            reservation,
            "host_measured" if cleanup_confirmed else "retained_reservation",
            "recover" if root_failure else "continue",
            measured,
            self.broker.gateway.usage(job.job_id),
            unresolved,
            cleanup_failure,
        )
        atomic_json(root / "job-outcome.json", outcome)
        await self.store.record_event(
            job.experiment_id,
            "job_outcome",
            {
                "job_id": str(job.job_id),
                "path": str((root / "job-outcome.json").relative_to(self.root)),
                "termination": termination,
                "cleanup_confirmed": cleanup_confirmed,
                "failure_category": root_failure.category if root_failure else None,
            },
        )
        if root_failure:
            await self.store.fail_run(
                job.experiment_id,
                root_failure.category,
                interrupted=termination == "interrupted",
            )
        if (
            failure
            and stage != "cleanup"
            and not isinstance(
                failure, (ValueError, OSError, TimeoutError, asyncio.CancelledError)
            )
        ):
            raise failure
        assert self.broker.gateway is not None
        execution_type = (
            IsolatedExecution if isinstance(job, WorkerJob) else IsolatedRoleExecution
        )
        return execution_type(
            observation,  # type: ignore[arg-type]
            solver_export,
            state,
            self.broker.gateway.usage(job.job_id),
            tuple(session.records),
            cleanup_confirmed,
            elapsed,
            outcome,
        )

    async def _monitor(self, job: WorkerRequest, started: float) -> None:
        while True:
            record = await self.store.get(job.experiment_id)
            elapsed = (
                time.monotonic()
                - started
                - self.broker.approval_wait_seconds(job.job_id)
            )
            if record.stop_requested or record.status not in ("ready", "running"):
                self.broker.revoke(job.job_id)
                raise ValueError("durable stop requested")
            if elapsed > job.limits.max_active_seconds:
                self.broker.revoke(job.job_id)
                raise ValueError("protected active time exceeded")
            await asyncio.sleep(0.1)

    async def stop(self, job_id: UUID) -> None:
        failures: list[Exception] = []
        try:
            await self.broker.stop(job_id)
        except Exception as error:
            failures.append(error)
        for effect, kind, name, job in reversed(self.owned.get(job_id, [])):
            try:
                if kind != "network":
                    await verify_stopped(self.docker, name, job)
                else:
                    data = json.loads(
                        await self.docker.invoke("network", "inspect", name)
                    )
                    if data[0]["Labels"].get(LABEL) != str(job_id):
                        raise ValueError("network ownership drift")
                await self.store.effect_state(effect, "stopped")
            except Exception as error:
                await self.store.effect_state(effect, "uncertain")
                failures.append(error)
        if failures:
            raise ExceptionGroup("owned effect cleanup uncertain", failures)


def read_regular_artifact(path: Path, limit: int) -> bytes:
    safe_path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError("worker artifact must be a bounded regular file")
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError("worker artifact exceeds cap")
    return data
