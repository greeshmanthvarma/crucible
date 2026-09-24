import asyncio
import json
from dataclasses import dataclass
from typing import Mapping, Protocol, cast


class DockerClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class VolumeInfo:
    identity: str
    labels: Mapping[str, str]


class DockerClient(Protocol):
    async def create_volume(self, name: str, labels: dict[str, str]) -> VolumeInfo: ...

    async def inspect_volume(self, identity: str) -> VolumeInfo | None: ...

    async def list_volumes(self, label: str) -> tuple[VolumeInfo, ...]: ...


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
            if missing_ok and "No such volume" in detail:
                return None
            raise DockerClientError(detail or "Docker command failed")
        return stdout.decode()
