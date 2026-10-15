"""Protected host staging, launch, stop, collection, and owned-resource cleanup."""

import asyncio
import os
import re
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.gateway import ScopedModelGateway, TrustedUsage, WorkerCredential
from crucible.repair.worker_protocol import (
    WorkerJob,
    WorkerObservation,
    read_observation,
)

_IMAGE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_ROUTE = "/internal/repair-model/responses"


def relay_allows(method: str, path: str) -> bool:
    return method == "POST" and path == _ROUTE


async def _git(root: Path, *args: str, data: bytes | None = None) -> bytes:
    process = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=root,
        stdin=asyncio.subprocess.PIPE
        if data is not None
        else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(data), 30)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise
    if (
        process.returncode not in (0, 1)
        or process.returncode == 1
        and args[0] != "check-ignore"
    ):
        raise ValueError("Cannot export source checkout")
    return output


async def export_source(source: Path, destination: Path) -> None:
    names = (
        (
            await _git(
                source, "ls-files", "--cached", "--others", "--exclude-standard", "-z"
            )
        )
        .decode()
        .split("\0")
    )
    candidates = sorted(
        {
            name
            for name in names
            if name
            and not any(
                part
                in {
                    ".git",
                    ".env",
                    "tests",
                    "evals",
                    ".venv",
                    "node_modules",
                    "__pycache__",
                }
                or part.startswith(".env.")
                for part in Path(name).parts
            )
        }
    )
    ignored = set(
        (
            await _git(
                source,
                "check-ignore",
                "--no-index",
                "-z",
                "--stdin",
                data="\0".join(candidates).encode() + (b"\0" if candidates else b""),
            )
        )
        .decode()
        .split("\0")
    )
    known_credentials = tuple(
        value.encode()
        for key, value in os.environ.items()
        if value and key.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD"))
    )
    destination.mkdir(mode=0o755, parents=True, exist_ok=False)
    for name in candidates:
        if name in ignored:
            continue
        path = source / name
        if not path.exists():
            continue  # tracked deletion
        if path.is_symlink() or any(
            parent.is_symlink()
            for parent in path.parents
            if parent != source and parent.is_relative_to(source)
        ):
            raise ValueError("Symlink in source export")
        if not path.is_file():
            raise ValueError("Nonregular source export entry")
        target = destination / name
        target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        content = path.read_bytes()
        if any(credential in content for credential in known_credentials):
            raise ValueError("Source export contains a configured credential")
        target.write_bytes(content)
        target.chmod(0o555 if path.stat().st_mode & stat.S_IXUSR else 0o444)


def _copy_workspace(
    source: Path, destination: Path, *, exclude_git: bool = False
) -> None:
    if source.is_symlink() or not source.is_dir():
        raise ValueError("Unsafe workspace root")
    for parent, directories, files in os.walk(source, followlinks=False):
        if exclude_git:
            directories[:] = [name for name in directories if name != ".git"]
            files = [name for name in files if name != ".git"]
        for name in directories + files:
            if name == ".env" or name.startswith(".env."):
                raise ValueError("Environment files cannot enter worker fixtures")
            mode = (Path(parent) / name).lstat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise ValueError("Unsafe workspace entry")
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(".git") if exclude_git else None,
    )


def worker_container_arguments(
    request: WorkerJob,
    *,
    image: str,
    network: str,
    harness: Path,
    workspace: Path,
    state: Path,
    job_dir: Path | None = None,
    driver: Path,
    job: Path | None = None,
) -> tuple[str, ...]:
    # Accept `job` directory under the launch helper's historical test keyword.
    request_dir = job_dir or job
    if request_dir is None:
        raise ValueError("Job request directory required")
    if (
        not _IMAGE.fullmatch(image)
        or network != "none"
        and network != f"crucible-repair-{request.job_id}"
    ):
        raise ValueError("Pinned worker image and owned isolated network required")
    args = [
        "create",
        "--name",
        f"crucible-repair-worker-{request.job_id}",
        "--read-only",
        "--user",
        "65532:65532",
        "--network",
        network,
        "--cpus",
        "1",
        "--memory",
        str(1024**3),
        "--pids-limit",
        "256",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--label",
        f"crucible.repair.job={request.job_id}",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=67108864",
        "--env",
        f"CRUCIBLE_REPAIR_IMAGE={image}",
        "--env",
        "HOME=/tmp",
    ]
    for path, target, readonly in (
        (harness, "/harness", True),
        (workspace, "/workspace", False),
        (state, "/state", False),
        (request_dir, "/job", True),
        (driver, "/driver", True),
    ):
        if "," in str(path):
            raise ValueError("Docker mount path contains separator")
        args.extend(
            (
                "--mount",
                f"type=bind,src={path.resolve()},dst={target}"
                + (",readonly" if readonly else ""),
            )
        )
    args.extend((image, "python", "-I", "/driver/worker.py"))
    return tuple(args)


@dataclass(frozen=True)
class WorkerExecution:
    observation: WorkerObservation | None
    workspace_snapshot: Path
    termination: str
    usage: TrustedUsage
    wall_seconds: float
    initial_workspace_snapshot: Path | None = None


class WorkerSupervisor:
    def __init__(
        self,
        docker: Any,
        *,
        image: str,
        root: Path,
        gateway: ScopedModelGateway | None = None,
        relay_image: str | None = None,
    ) -> None:
        if (
            not _IMAGE.fullmatch(image)
            or relay_image is not None
            and not _IMAGE.fullmatch(relay_image)
        ):
            raise ValueError("Worker/relay images must be digest-pinned")
        self.docker, self.image, self.root = docker, image, root
        self.gateway, self.relay_image = gateway, relay_image or image

    async def _docker(self, *args: str) -> str:
        process = await asyncio.create_subprocess_exec(
            "docker",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 30)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.communicate()
            raise
        if process.returncode:
            raise RuntimeError("Owned Docker resource operation failed")
        return output.decode().strip()

    async def run(
        self,
        job: WorkerJob,
        source: Path,
        fixture: Path,
        credential: WorkerCredential | None,
    ) -> WorkerExecution:
        started = time.monotonic()
        directory = self.root / f"{job.job_id}-{uuid4().hex}"
        directory.mkdir(parents=True, mode=0o700)
        harness, workspace, state, request_dir, driver = (
            directory / name
            for name in ("harness", "workspace", "state", "job", "driver")
        )
        snapshot = directory / "snapshot"
        initial_snapshot = directory / "initial-snapshot"
        container: str | None = None
        relay: str | None = None
        network: str | None = None
        observation = None
        termination = "backend_error"
        stopped = False
        try:
            original_identity = await asyncio.wait_for(
                capture_source_identity(source), 30
            )
            if original_identity.tree_digest != job.source_digest:
                raise ValueError("Source differs from the frozen job identity")
            await export_source(source, harness)
            # Export content only, including for Git clones/worktrees.
            _copy_workspace(fixture, workspace, exclude_git=True)
            # Create disposable Git metadata; never inherit host Git state.
            await _git(workspace, "init", "-q")
            await _git(workspace, "add", "--all")
            await _git(
                workspace,
                "-c",
                "user.name=Crucible Evaluation",
                "-c",
                "user.email=evaluation@localhost",
                "commit",
                "--allow-empty",
                "-qm",
                "Frozen evaluation fixture",
            )
            for path in (workspace, state, request_dir, driver):
                path.mkdir(parents=True, exist_ok=True)
                path.chmod(0o777 if path in (workspace, state) else 0o755)
            for path in workspace.rglob("*"):
                path.chmod(0o777 if path.is_dir() else 0o666)
            _copy_workspace(workspace, initial_snapshot)
            (request_dir / "request.json").write_text(job.model_dump_json())
            (request_dir / "request.json").chmod(0o444)
            # The launch script is controller-owned, separate from candidate source.
            shutil.copyfile(Path(__file__).with_name("worker.py"), driver / "worker.py")
            shutil.copyfile(
                Path(__file__).with_name("worker_relay.py"), driver / "worker_relay.py"
            )
            network_mode = "none"
            environment: tuple[str, ...] = ()
            if credential is not None:
                if self.gateway is None:
                    raise ValueError("Fresh jobs require a host-owned gateway")
                network = f"crucible-repair-{job.job_id}"
                await self._docker(
                    "network",
                    "create",
                    "--internal",
                    "--label",
                    f"crucible.repair.job={job.job_id}",
                    network,
                )
                network_mode = network
                relay = await self._docker(
                    "create",
                    "--name",
                    f"crucible-repair-relay-{job.job_id}",
                    "--network",
                    "bridge",
                    "--read-only",
                    "--user",
                    "65532:65532",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "--memory",
                    "134217728",
                    "--pids-limit",
                    "32",
                    "--cpus",
                    "0.25",
                    "--label",
                    f"crucible.repair.job={job.job_id}",
                    "--add-host",
                    "host.docker.internal:host-gateway",
                    "--mount",
                    f"type=bind,src={driver.resolve()},dst=/driver,readonly",
                    "--env",
                    f"REPAIR_GATEWAY_UPSTREAM={credential.endpoint}",
                    self.relay_image,
                    "python",
                    "-I",
                    "/driver/worker_relay.py",
                )
                await self._docker(
                    "network", "connect", "--alias", "repair-gateway", network, relay
                )
                await self._docker("start", relay)
                environment = (
                    "--env",
                    f"REPAIR_GATEWAY_TOKEN={credential.token}",
                    "--env",
                    "REPAIR_GATEWAY_ENDPOINT=http://repair-gateway:8080/internal/repair-model/responses",
                )
            arguments = worker_container_arguments(
                job,
                image=self.image,
                network=network_mode,
                harness=harness,
                workspace=workspace,
                state=state,
                job_dir=request_dir,
                driver=driver,
            )
            if environment:
                index = arguments.index(self.image)
                arguments = arguments[:index] + environment + arguments[index:]
            current = await asyncio.wait_for(capture_source_identity(source), 30)
            if current.tree_digest != job.source_digest:
                raise ValueError("Source changed before worker dispatch")
            container = await asyncio.wait_for(
                self.docker.create_container(arguments), 30
            )
            try:
                async with asyncio.timeout(job.deadline_seconds + 30):
                    code = await self.docker.start_attached(
                        container, lambda _chunk: None
                    )
                termination = "completed" if code == 0 else "process_error"
            except TimeoutError:
                termination = "timed_out"
            await asyncio.wait_for(self.docker.stop_container(container, 2), 10)
            stopped = True
            _copy_workspace(workspace, snapshot)
            credential_output = False
            if credential is not None:
                for output_root in (snapshot, state):
                    for path in output_root.rglob("*"):
                        if path.is_symlink():
                            raise ValueError("Unsafe worker state artifact")
                        if path.is_file():
                            data = path.read_bytes()
                            if credential.token.encode() in data:
                                path.write_bytes(
                                    data.replace(
                                        credential.token.encode(),
                                        b"[redacted-worker-credential]",
                                    )
                                )
                                credential_output = True
            raw_path = state / "observation.json"
            if (
                termination == "completed"
                and raw_path.is_file()
                and not raw_path.is_symlink()
            ):
                with raw_path.open("rb") as stream:
                    observation = read_observation(
                        stream.read(job.observation_bytes + 1), job
                    )
                if credential_output:
                    observation = WorkerObservation(
                        job_id=job.job_id,
                        status="unsupported",
                        error_code="credential_output_redacted",
                        detail="Worker output contained a scoped credential",
                    )
        except asyncio.CancelledError:
            termination = "cancelled"
            raise
        except Exception:
            termination = "backend_error"
        finally:
            if credential is not None and self.gateway is not None:
                self.gateway.revoke(job.job_id)
            cleanup_failed = False
            if container is not None:
                if not stopped:
                    try:
                        await asyncio.wait_for(
                            self.docker.kill_container(container), 10
                        )
                    except Exception:
                        cleanup_failed = True
                try:
                    await asyncio.wait_for(self.docker.remove_container(container), 10)
                except Exception:
                    cleanup_failed = True
            if relay is not None:
                try:
                    await self._docker("rm", "--force", relay)
                except Exception:
                    cleanup_failed = True
            if network is not None:
                try:
                    await self._docker("network", "rm", network)
                except Exception:
                    cleanup_failed = True
            if cleanup_failed:
                termination = "cleanup_error"
                observation = None
        return WorkerExecution(
            observation,
            snapshot,
            termination,
            self.gateway.usage(job.job_id) if self.gateway else TrustedUsage(),
            time.monotonic() - started,
            initial_snapshot if initial_snapshot.exists() else None,
        )
