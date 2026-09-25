from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from crucible.domain.clock import require_utc
from crucible.domain.commands import CommandSpec
from crucible.domain.ids import ExternalResourceId, RunId, TaskId, ToolCallId
from crucible.domain.resources import ExternalResource


class OutputStream(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"


@dataclass(frozen=True)
class OutputChunk:
    stream: OutputStream
    sequence: int
    data: bytes

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("Output sequence must be positive")


@dataclass(frozen=True)
class SandboxMount:
    resource_id: ExternalResourceId
    external_identity: str
    target: str
    read_only: bool = False


@dataclass(frozen=True)
class SandboxRequest:
    task_id: TaskId
    run_id: RunId
    tool_call_id: ToolCallId
    workspace_path: Path
    command: CommandSpec
    mounts: tuple[SandboxMount, ...]


@dataclass(frozen=True)
class EvalSandboxRequest:
    task_id: TaskId
    run_id: RunId
    trial_id: ExternalResourceId
    source_path: Path
    tests_path: Path
    image: str
    executable: str
    arguments: tuple[str, ...]
    timeout_seconds: int
    output_bytes: int = 65_536


class SandboxTermination(StrEnum):
    COMPLETED = "completed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    BACKEND_ERROR = "backend_error"
    OUTPUT_LIMIT = "output_limit"


@dataclass(frozen=True)
class SandboxOutcome:
    exit_code: int | None
    termination: SandboxTermination
    started_at: datetime
    completed_at: datetime
    image_digest: str
    container_id: str
    original_bytes: int
    retained_bytes: int
    truncated: bool

    def __post_init__(self) -> None:
        require_utc(self.started_at, self.completed_at)
        if self.original_bytes < 0 or self.retained_bytes < 0:
            raise ValueError("Sandbox output byte counts cannot be negative")
        if self.retained_bytes > self.original_bytes:
            raise ValueError("Retained output cannot exceed original output")


OutputCallback = Callable[[OutputChunk], Awaitable[None] | None]


class SandboxBackend(Protocol):
    async def execute(
        self, request: SandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome: ...

    async def cancel(self, container_id: str) -> None: ...

    async def reconcile(self, resources: tuple[ExternalResource, ...]) -> None: ...


class EvalSandboxBackend(Protocol):
    async def evaluate(
        self, request: EvalSandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome: ...
