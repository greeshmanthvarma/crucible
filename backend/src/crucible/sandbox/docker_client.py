import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Mapping, Protocol, cast

from crucible.sandbox.protocol import OutputChunk, OutputStream


class DockerClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class VolumeInfo:
    identity: str
    labels: Mapping[str, str]


@dataclass(frozen=True)
class ContainerInfo:
    identity: str
    image_digest: str
    running: bool
    labels: Mapping[str, str]


class DockerClient(Protocol):
    async def create_volume(self, name: str, labels: dict[str, str]) -> VolumeInfo: ...

    async def inspect_volume(self, identity: str) -> VolumeInfo | None: ...

    async def list_volumes(self, label: str) -> tuple[VolumeInfo, ...]: ...

    async def create_container(self, arguments: tuple[str, ...]) -> str: ...

    async def start_attached(
        self,
        container_id: str,
        on_chunk: Callable[[OutputChunk], Awaitable[None] | None],
    ) -> int: ...

    async def inspect_container(self, container_id: str) -> ContainerInfo | None: ...

    async def stop_container(self, container_id: str, grace_seconds: int) -> None: ...

    async def kill_container(self, container_id: str) -> None: ...

    async def remove_container(self, container_id: str) -> None: ...

    async def list_containers(self, label: str) -> tuple[ContainerInfo, ...]: ...


class SubprocessDockerClient:
    async def create_volume(self, name: str, labels: dict[str, str]) -> VolumeInfo:
        arguments = ["volume", "create", "--name", name]
        for key, value in sorted(labels.items()):
            arguments.extend(("--label", f"{key}={value}"))
        output = await self._run(*arguments)
        assert output is not None
        identity = output.strip()
        inspected = await self.inspect_volume(identity)
        if inspected is None:
            raise DockerClientError("Docker did not return the created volume")
        return inspected

    async def inspect_volume(self, identity: str) -> VolumeInfo | None:
        output = await self._run("volume", "inspect", identity, missing_ok=True)
        if output is None:
            return None
        values = cast(list[dict[str, object]], json.loads(output))
        if len(values) != 1:
            raise DockerClientError("Unexpected Docker volume inspection response")
        value = values[0]
        labels = value.get("Labels") or {}
        if not isinstance(labels, dict):
            raise DockerClientError("Docker volume labels are malformed")
        return VolumeInfo(
            str(value["Name"]), {str(key): str(item) for key, item in labels.items()}
        )

    async def list_volumes(self, label: str) -> tuple[VolumeInfo, ...]:
        output = await self._run(
            "volume", "ls", "--filter", f"label={label}", "--format", "{{.Name}}"
        )
        assert output is not None
        names = tuple(line for line in output.splitlines() if line)
        inspected = await asyncio.gather(*(self.inspect_volume(name) for name in names))
        return tuple(item for item in inspected if item is not None)

    async def create_container(self, arguments: tuple[str, ...]) -> str:
        output = await self._run(*arguments)
        assert output is not None
        return output.strip()

    async def start_attached(
        self,
        container_id: str,
        on_chunk: Callable[[OutputChunk], Awaitable[None] | None],
    ) -> int:
        process = await asyncio.create_subprocess_exec(
            "docker",
            "start",
            "--attach",
            container_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdout is not None and process.stderr is not None
        queue: asyncio.Queue[tuple[OutputStream, bytes] | None] = asyncio.Queue()

        async def pump(stream: asyncio.StreamReader, stream_name: OutputStream) -> None:
            while data := await stream.read(8192):
                await queue.put((stream_name, data))
            await queue.put(None)

        pumps = (
            asyncio.create_task(pump(process.stdout, OutputStream.STDOUT)),
            asyncio.create_task(pump(process.stderr, OutputStream.STDERR)),
        )
        completed_streams = 0
        sequence = 0
        try:
            while completed_streams < 2:
                item = await queue.get()
                if item is None:
                    completed_streams += 1
                    continue
                sequence += 1
                emitted = on_chunk(OutputChunk(item[0], sequence, item[1]))
                if inspect.isawaitable(emitted):
                    await emitted
            return await process.wait()
        finally:
            for task in pumps:
                if not task.done():
                    task.cancel()
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def inspect_container(self, container_id: str) -> ContainerInfo | None:
        output = await self._run("inspect", container_id, missing_ok=True)
        if output is None:
            return None
        values = cast(list[dict[str, object]], json.loads(output))
        if len(values) != 1:
            raise DockerClientError("Unexpected Docker container inspection response")
        value = values[0]
        config = cast(dict[str, object], value.get("Config") or {})
        state = cast(dict[str, object], value.get("State") or {})
        labels = cast(dict[str, object], config.get("Labels") or {})
        return ContainerInfo(
            str(value["Id"]),
            str(value["Image"]),
            bool(state.get("Running")),
            {str(key): str(item) for key, item in labels.items()},
        )

    async def stop_container(self, container_id: str, grace_seconds: int) -> None:
        await self._run(
            "stop", "--time", str(grace_seconds), container_id, missing_ok=True
        )

    async def kill_container(self, container_id: str) -> None:
        await self._run("kill", container_id, missing_ok=True)

    async def remove_container(self, container_id: str) -> None:
        await self._run("rm", "--force", container_id, missing_ok=True)

    async def list_containers(self, label: str) -> tuple[ContainerInfo, ...]:
        output = await self._run(
            "ps", "-a", "--filter", f"label={label}", "--format", "{{.ID}}"
        )
        assert output is not None
        identities = tuple(line for line in output.splitlines() if line)
        inspected = await asyncio.gather(
            *(self.inspect_container(identity) for identity in identities)
        )
        return tuple(item for item in inspected if item is not None)

    async def _run(self, *arguments: str, missing_ok: bool = False) -> str | None:
        process = await asyncio.create_subprocess_exec(
            "docker",
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            detail = stderr.decode(errors="replace").strip()
            if missing_ok and any(
                marker in detail.lower()
                for marker in ("no such volume", "no such container", "no such object")
            ):
                return None
            raise DockerClientError(detail or "Docker command failed")
        return stdout.decode()
