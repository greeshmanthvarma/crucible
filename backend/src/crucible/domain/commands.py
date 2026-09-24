import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Mapping, cast

_ENVIRONMENT_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_SECRET_SUFFIXES = ("_TOKEN", "_SECRET", "_PASSWORD", "_KEY")


class CommandNetwork(StrEnum):
    NONE = "none"
    OUTBOUND = "outbound"


@dataclass(frozen=True)
class CommandLimits:
    cpus: float
    memory_bytes: int
    pids: int
    output_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "cpus", float(self.cpus))
        if self.cpus <= 0 or self.memory_bytes <= 0 or self.pids <= 0:
            raise ValueError("Command resource limits must be positive")
        if self.output_bytes <= 0:
            raise ValueError("Command output limit must be positive")

    def as_dict(self) -> dict[str, object]:
        return {
            "cpus": self.cpus,
            "memory_bytes": self.memory_bytes,
            "pids": self.pids,
            "output_bytes": self.output_bytes,
        }


@dataclass(frozen=True)
class CommandSpec:
    executable: str
    arguments: tuple[str, ...]
    cwd: str
    timeout_seconds: int
    network: CommandNetwork
    environment: Mapping[str, str]
    image: str
    reason: str
    limits: CommandLimits

    def __post_init__(self) -> None:
        if not self.executable or "\x00" in self.executable:
            raise ValueError("Command executable must not be blank or contain NUL")
        if any("\x00" in value for value in self.arguments):
            raise ValueError("Command arguments must not contain NUL")
        cwd = PurePosixPath(self.cwd)
        if cwd.is_absolute() or ".." in cwd.parts:
            raise ValueError("Command cwd must be relative to the Workspace")
        if not 1 <= self.timeout_seconds <= 3600:
            raise ValueError("Command timeout must be between 1 and 3600 seconds")
        if not self.image.strip():
            raise ValueError("Command image must not be blank")
        copied = dict(self.environment)
        for name, value in copied.items():
            if not _ENVIRONMENT_NAME.fullmatch(name):
                raise ValueError(f"Invalid environment name: {name}")
            if name.endswith(_SECRET_SUFFIXES):
                raise ValueError(f"Environment credential is forbidden: {name}")
            if "\x00" in value:
                raise ValueError("Environment values must not contain NUL")
        object.__setattr__(self, "arguments", tuple(self.arguments))
        object.__setattr__(self, "environment", MappingProxyType(copied))

    def as_dict(self) -> dict[str, object]:
        return {
            "arguments": list(self.arguments),
            "cwd": self.cwd,
            "environment": dict(sorted(self.environment.items())),
            "executable": self.executable,
            "image": self.image,
            "limits": self.limits.as_dict(),
            "network": self.network.value,
            "reason": self.reason,
            "timeout_seconds": self.timeout_seconds,
        }

    def canonical_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "CommandSpec":
        limits = value["limits"]
        if not isinstance(limits, Mapping):
            raise ValueError("Command limits must be an object")
        return cls(
            executable=str(value["executable"]),
            arguments=tuple(
                str(item) for item in cast(list[object], value["arguments"])
            ),
            cwd=str(value["cwd"]),
            timeout_seconds=int(cast(int, value["timeout_seconds"])),
            network=CommandNetwork(str(value["network"])),
            environment={
                str(key): str(item)
                for key, item in cast(
                    Mapping[object, object], value["environment"]
                ).items()
            },
            image=str(value["image"]),
            reason=str(value["reason"]),
            limits=CommandLimits(
                float(limits["cpus"]),
                int(limits["memory_bytes"]),
                int(limits["pids"]),
                int(limits["output_bytes"]),
            ),
        )
