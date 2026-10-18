import asyncio
import inspect
import re
from collections.abc import Callable
from dataclasses import replace
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.commands import CommandNetwork
from crucible.domain.ids import new_id
from crucible.domain.resources import ExternalResource, ExternalResourceStatus
from crucible.sandbox.docker_client import DockerClient, DockerClientError
from crucible.sandbox.protocol import (
    EvalSandboxRequest,
    OutputCallback,
    OutputChunk,
    SandboxOutcome,
    SandboxRequest,
    SandboxTermination,
)
from crucible.sandbox.resources import (
    DEPENDENCY_TARGET,
    KIND_LABEL,
    MANAGED_LABEL,
    RESOURCE_LABEL,
    TASK_LABEL,
)


class _OutputLimitReached(Exception):
    pass


class DockerSandboxBackend:
    def __init__(
        self,
        docker: DockerClient,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        *,
        cleanup_grace_seconds: int = 3,
        repository_environment: tuple[str, str] | None = None,
    ) -> None:
        self._docker = docker
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._cleanup_grace_seconds = cleanup_grace_seconds
        if repository_environment is not None:
            image, platform = repository_environment
            if re.fullmatch(
                r".+@sha256:[0-9a-f]{64}", image
            ) is None or platform not in {"linux/amd64", "linux/arm64"}:
                raise ValueError("invalid repository environment")
        self.repository_environment = repository_environment

    async def execute(
        self, request: SandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome:
        resource_id = new_id()
        labels = {
            MANAGED_LABEL: "true",
            TASK_LABEL: str(request.task_id),
            KIND_LABEL: "container",
            RESOURCE_LABEL: str(resource_id),
            "harness.run_id": str(request.run_id),
            "harness.tool_call_id": str(request.tool_call_id),
        }
        arguments = self._create_arguments(request, labels)
        started_at = self._clock.now()
        container_id = await self._docker.create_container(arguments)
        resource = ExternalResource.container(
            resource_id,
            request.task_id,
            request.run_id,
            request.tool_call_id,
            container_id,
            started_at,
            labels=labels,
            metadata={"name": f"crucible-command-{request.tool_call_id}"},
        )
        try:
            async with self._unit_of_work() as uow:
                await uow.external_resources.add(resource)
                await uow.commit()
        except BaseException:
            await self._cleanup(container_id)
            raise

        original_bytes = 0
        retained_bytes = 0
        termination = SandboxTermination.COMPLETED
        exit_code: int | None = None

        async def bounded(chunk: OutputChunk) -> None:
            nonlocal original_bytes, retained_bytes
            original_bytes += len(chunk.data)
            remaining = request.command.limits.output_bytes - retained_bytes
            retained = chunk.data[: max(0, remaining)]
            retained_bytes += len(retained)
            if retained:
                emitted = on_chunk(OutputChunk(chunk.stream, chunk.sequence, retained))
                if inspect.isawaitable(emitted):
                    await emitted
            if len(retained) < len(chunk.data):
                raise _OutputLimitReached

        image_digest = request.command.image
        try:
            try:
                exit_code = await asyncio.wait_for(
                    self._docker.start_attached(container_id, bounded),
                    timeout=request.command.timeout_seconds,
                )
            except TimeoutError:
                termination = SandboxTermination.TIMED_OUT
            except _OutputLimitReached:
                termination = SandboxTermination.OUTPUT_LIMIT
            inspected_container = await self._docker.inspect_container(container_id)
            if inspected_container is not None:
                image_digest = inspected_container.image_digest
        except asyncio.CancelledError:
            termination = SandboxTermination.CANCELLED
            raise
        except Exception:
            termination = SandboxTermination.BACKEND_ERROR
            raise
        finally:
            cleaned = await self._cleanup(container_id)
            await self._mark_status(
                resource,
                ExternalResourceStatus.REMOVED
                if cleaned
                else ExternalResourceStatus.ERROR,
            )

        return SandboxOutcome(
            exit_code,
            termination,
            started_at,
            self._clock.now(),
            image_digest,
            container_id,
            original_bytes,
            retained_bytes,
            original_bytes > retained_bytes,
        )

    async def cancel(self, container_id: str) -> None:
        await self._cleanup(container_id)

    async def stop_task_commands(self, task_id: UUID) -> None:
        """Stop only this task's registered command containers before a snapshot."""
        async with self._unit_of_work() as uow:
            resources = await uow.external_resources.list_for_task(task_id)
        for resource in resources:
            if (
                resource.kind.value != "container"
                or resource.status is ExternalResourceStatus.REMOVED
            ):
                continue
            if not await self._cleanup(resource.external_identity):
                raise RuntimeError("solver command cleanup failed")
            await self._mark_status(resource, ExternalResourceStatus.REMOVED)

    async def evaluate(
        self, request: EvalSandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome:
        """Run protected tests against read-only inputs after the coding Run."""
        source = request.source_path.resolve(strict=True)
        tests = request.tests_path.resolve(strict=True)
        if source == tests or source in tests.parents or tests in source.parents:
            raise ValueError("evaluator source and tests must be separate")
        if not source.is_dir() or not tests.is_dir():
            raise ValueError("evaluator inputs must be directories")
        if re.fullmatch(r".+@sha256:[0-9a-f]{64}", request.image) is None:
            raise ValueError("evaluator image must be digest pinned")
        if not 1 <= request.timeout_seconds <= 3600 or request.output_bytes < 1:
            raise ValueError("invalid evaluator bounds")
        resource_id = new_id()
        labels = {
            MANAGED_LABEL: "true",
            TASK_LABEL: str(request.task_id),
            KIND_LABEL: "container",
            RESOURCE_LABEL: str(resource_id),
            "harness.run_id": str(request.run_id),
            "harness.eval_trial_id": str(request.trial_id),
        }
        arguments = [
            "create",
            "--name",
            f"crucible-eval-{request.trial_id}",
            "--read-only",
            "--user",
            "65532:65532",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--network",
            "none",
            "--cpus",
            "2",
            "--memory",
            str(1024**3),
            "--pids-limit",
            "128",
        ]
        for key, value in sorted(labels.items()):
            arguments.extend(("--label", f"{key}={value}"))
        arguments.extend(
            (
                "--env",
                "PATH=/usr/local/bin:/usr/bin:/bin",
                "--env",
                "HOME=/tmp",
                "--env",
                "PYTHONDONTWRITEBYTECODE=1",
                "--mount",
                f"type=bind,src={source},dst=/source,readonly",
                "--mount",
                f"type=bind,src={tests},dst=/tests,readonly",
                "--workdir",
                "/source",
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,size=64m",
                request.image,
                request.executable,
                *request.arguments,
            )
        )
        started_at = self._clock.now()
        container_id = await self._docker.create_container(tuple(arguments))
        resource = ExternalResource.container(
            resource_id,
            request.task_id,
            request.run_id,
            None,
            container_id,
            started_at,
            labels=labels,
            metadata={"name": f"crucible-eval-{request.trial_id}"},
        )
        try:
            async with self._unit_of_work() as uow:
                await uow.external_resources.add(resource)
                await uow.commit()
        except BaseException:
            await self._cleanup(container_id)
            raise
        original_bytes = 0
        retained_bytes = 0
        termination = SandboxTermination.COMPLETED
        exit_code: int | None = None

        async def bounded(chunk: OutputChunk) -> None:
            nonlocal original_bytes, retained_bytes
            original_bytes += len(chunk.data)
            remaining = request.output_bytes - retained_bytes
            retained = chunk.data[: max(0, remaining)]
            retained_bytes += len(retained)
            if retained:
                emitted = on_chunk(OutputChunk(chunk.stream, chunk.sequence, retained))
                if inspect.isawaitable(emitted):
                    await emitted
            if len(retained) < len(chunk.data):
                raise _OutputLimitReached

        try:
            try:
                exit_code = await asyncio.wait_for(
                    self._docker.start_attached(container_id, bounded),
                    timeout=request.timeout_seconds,
                )
            except TimeoutError:
                termination = SandboxTermination.TIMED_OUT
            except _OutputLimitReached:
                termination = SandboxTermination.OUTPUT_LIMIT
        except asyncio.CancelledError:
            termination = SandboxTermination.CANCELLED
            raise
        except Exception:
            termination = SandboxTermination.BACKEND_ERROR
            raise
        finally:
            cleaned = await self._cleanup(container_id)
            await self._mark_status(
                resource,
                ExternalResourceStatus.REMOVED
                if cleaned
                else ExternalResourceStatus.ERROR,
            )
        return SandboxOutcome(
            exit_code,
            termination,
            started_at,
            self._clock.now(),
            request.image,
            container_id,
            original_bytes,
            retained_bytes,
            original_bytes > retained_bytes,
        )

    async def reconcile(self, resources: tuple[ExternalResource, ...]) -> None:
        discovered = {
            container.identity: container
            for container in await self._docker.list_containers(f"{MANAGED_LABEL}=true")
        }
        for resource in resources:
            if resource.status not in (
                ExternalResourceStatus.ACTIVE,
                ExternalResourceStatus.ERROR,
            ):
                continue
            container = discovered.get(resource.external_identity)
            if container is None:
                container = await self._docker.inspect_container(
                    resource.external_identity
                )
                if container is None:
                    await self._mark_status(resource, ExternalResourceStatus.MISSING)
                    continue
            expected = {
                MANAGED_LABEL: "true",
                TASK_LABEL: str(resource.task_id),
                KIND_LABEL: "container",
                RESOURCE_LABEL: str(resource.id),
            }
            if all(
                container.labels.get(key) == value for key, value in expected.items()
            ):
                cleaned = await self._cleanup(container.identity)
                await self._mark_status(
                    resource,
                    ExternalResourceStatus.REMOVED
                    if cleaned
                    else ExternalResourceStatus.ERROR,
                )
            else:
                await self._mark_status(resource, ExternalResourceStatus.ERROR)

    def _create_arguments(
        self, request: SandboxRequest, labels: dict[str, str]
    ) -> tuple[str, ...]:
        command = request.command
        target = "/testbed" if self.repository_environment else "/workspace"
        if self.repository_environment and (
            command.image != self.repository_environment[0]
            or command.network is not CommandNetwork.NONE
        ):
            raise ValueError("command differs from admitted repository environment")
        if self.repository_environment and any(
            mount.target != DEPENDENCY_TARGET for mount in request.mounts
        ):
            raise ValueError("unexpected solver resource mount")
        arguments = [
            "create",
            "--name",
            f"crucible-command-{request.tool_call_id}",
            "--read-only",
            "--user",
            "65532:65532",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--network",
            "none" if command.network is CommandNetwork.NONE else "bridge",
            "--cpus",
            str(command.limits.cpus),
            "--memory",
            str(command.limits.memory_bytes),
            "--pids-limit",
            str(command.limits.pids),
        ]
        if self.repository_environment:
            user_index = arguments.index("--user")
            del arguments[user_index : user_index + 2]
            arguments.extend(
                ("--platform", self.repository_environment[1], "--pull=never")
            )
        for key, value in sorted(labels.items()):
            arguments.extend(("--label", f"{key}={value}"))
        environment = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "CRUCIBLE_TASK_ID": str(request.task_id),
            **command.environment,
        }
        if self.repository_environment:
            environment.pop("PATH")
            environment.update(
                {
                    "GIT_OPTIONAL_LOCKS": "0",
                    "GIT_CONFIG_GLOBAL": "/dev/null",
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_CONFIG_COUNT": "1",
                    "GIT_CONFIG_KEY_0": "safe.directory",
                    "GIT_CONFIG_VALUE_0": target,
                }
            )
        for key, value in sorted(environment.items()):
            arguments.extend(("--env", f"{key}={value}"))
        arguments.extend(
            (
                "--mount",
                f"type=bind,src={request.workspace_path},dst={target}",
            )
        )
        if self.repository_environment:
            metadata = request.workspace_path / ".git"
            if metadata.is_symlink() or not metadata.is_dir():
                raise ValueError("benchmark commands require portable Git metadata")
            arguments.extend(
                ("--mount", f"type=bind,src={metadata},dst={target}/.git,readonly")
            )
        for mount in request.mounts:
            mount_target = (
                f"{target}/node_modules"
                if self.repository_environment
                else mount.target
            )
            option = f"type=volume,src={mount.external_identity},dst={mount_target}"
            if mount.read_only:
                option += ",readonly"
            arguments.extend(("--mount", option))
        workdir = target
        if command.cwd != ".":
            workdir = f"{target}/{command.cwd}"
        arguments.extend(
            (
                "--workdir",
                workdir,
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,nodev,size=64m",
                command.image,
                command.executable,
                *command.arguments,
            )
        )
        return tuple(arguments)

    async def _cleanup(self, container_id: str) -> bool:
        try:
            await self._docker.stop_container(container_id, self._cleanup_grace_seconds)
        except DockerClientError:
            try:
                await self._docker.kill_container(container_id)
            except DockerClientError:
                pass
        finally:
            try:
                await self._docker.remove_container(container_id)
            except DockerClientError:
                return False
        return True

    async def _mark_status(
        self, resource: ExternalResource, status: ExternalResourceStatus
    ) -> None:
        updated = replace(
            resource,
            status=status,
            updated_at=self._clock.now(),
        )
        async with self._unit_of_work() as uow:
            await uow.external_resources.update(updated)
            await uow.commit()
