"""Command adapter for an already-contained evaluation job; never nested Docker."""

import asyncio
import inspect
import os
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

    from crucible.application.approval_service import ApprovalService
    from crucible.domain.approvals import Approval
    from crucible.domain.clock import Clock
    from crucible.domain.commands import CommandSpec

from crucible.domain.resources import ExternalResource
from crucible.sandbox.protocol import (
    OutputCallback,
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxRequest,
    SandboxTermination,
)


class InContainerSandbox:
    version = "incontainer-process-group-v1"

    def __init__(self, workspace: Path, image_digest: str) -> None:
        self.workspace = workspace.resolve(strict=True)
        self.image_digest = image_digest
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self.records: list[dict[str, object]] = []

    async def execute(
        self, request: SandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome:
        if request.workspace_path.resolve() != self.workspace or request.mounts:
            raise ValueError(
                "Worker commands cannot add mounts or select another workspace"
            )
        cwd = self.workspace / request.command.cwd
        if not cwd.resolve(strict=True).is_relative_to(self.workspace):
            raise ValueError("Command cwd escapes the disposable workspace")
        for component in (cwd, *cwd.parents):
            if component == self.workspace:
                break
            if component.is_symlink():
                raise ValueError("Symlinked command cwd is unsupported")
        started = datetime.now(UTC)
        identity = str(request.tool_call_id)
        command = request.command
        environment = {
            "PATH": "/opt/venv/bin:/usr/local/bin:/usr/bin:/bin",
            "HOME": "/tmp",
            "LANG": "C.UTF-8",
            "CI": "1",
        }
        environment.update(command.environment)
        process = await asyncio.create_subprocess_exec(
            command.executable,
            *command.arguments,
            cwd=cwd,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self._processes[identity] = process
        original = retained = sequence = 0
        termination = SandboxTermination.COMPLETED
        limit_reached = asyncio.Event()
        output_lock = asyncio.Lock()

        async def pump(reader: asyncio.StreamReader, stream: OutputStream) -> None:
            nonlocal original, retained, sequence
            while chunk := await reader.read(8192):
                async with output_lock:
                    original += len(chunk)
                    keep = chunk[: max(0, command.limits.output_bytes - retained)]
                    if keep:
                        retained += len(keep)
                        sequence += 1
                        emitted = on_chunk(OutputChunk(stream, sequence, keep))
                        if inspect.isawaitable(emitted):
                            await emitted
                    if original > command.limits.output_bytes:
                        limit_reached.set()
                        return

        assert process.stdout is not None and process.stderr is not None
        pumps = (
            asyncio.create_task(pump(process.stdout, OutputStream.STDOUT)),
            asyncio.create_task(pump(process.stderr, OutputStream.STDERR)),
        )
        wait = asyncio.create_task(process.wait())
        overflow = asyncio.create_task(limit_reached.wait())
        cancelled = False
        try:
            async with asyncio.timeout(command.timeout_seconds):
                done, _ = await asyncio.wait(
                    (wait, overflow), return_when=asyncio.FIRST_COMPLETED
                )
                if overflow in done and limit_reached.is_set():
                    termination = SandboxTermination.OUTPUT_LIMIT
                else:
                    await wait
        except TimeoutError:
            termination = SandboxTermination.TIMED_OUT
        except asyncio.CancelledError:
            termination = SandboxTermination.CANCELLED
            cancelled = True
        finally:
            # Kill descendants even when the group leader already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()
            await asyncio.gather(*pumps)
            if limit_reached.is_set() and termination is SandboxTermination.COMPLETED:
                termination = SandboxTermination.OUTPUT_LIMIT
            overflow.cancel()
            await asyncio.gather(overflow, wait, return_exceptions=True)
            self._processes.pop(identity, None)
        outcome = SandboxOutcome(
            process.returncode,
            termination,
            started,
            datetime.now(UTC),
            self.image_digest,
            identity,
            original,
            retained,
            original > retained,
        )
        self.records.append(
            {
                "tool_call_id": identity,
                "command": command.as_dict(),
                "exit_code": outcome.exit_code,
                "termination": termination.value,
                "original_bytes": original,
                "retained_bytes": retained,
                "truncated": outcome.truncated,
            }
        )
        if cancelled:
            raise asyncio.CancelledError
        return outcome

    async def cancel(self, container_id: str) -> None:
        process = self._processes.get(container_id)
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    async def reconcile(self, resources: tuple[ExternalResource, ...]) -> None:
        if resources:
            raise ValueError(
                "Private worker state cannot reconcile host Docker resources"
            )


class WorkerCommandAuthority:
    """Automatic approval exists only for the immutable worker job's private run."""

    def __init__(
        self, approvals: "ApprovalService", clock: "Clock", job_id: "UUID"
    ) -> None:
        self.approvals, self.clock, self.job_id = approvals, clock, job_id

    async def request_and_wait(
        self,
        task_id: "UUID",
        run_id: "UUID",
        step_id: "UUID",
        tool_call_id: "UUID",
        spec: "CommandSpec",
    ) -> "Approval":
        from uuid import uuid5

        from crucible.domain.approvals import Approval, ApprovalStatus
        from crucible.domain.ids import new_id
        from crucible.repair.worker_protocol import WorkerJob

        original = WorkerJob.model_validate_json(Path("/job/request.json").read_bytes())
        if (
            original.job_id != self.job_id
            or task_id != uuid5(self.job_id, "task")
            or run_id != uuid5(self.job_id, "run")
        ):
            raise ValueError("Command is not linked to this private worker job")
        if (
            spec.limits.cpus > 1
            or spec.limits.memory_bytes > 1024**3
            or spec.limits.pids > 256
            or spec.timeout_seconds > original.deadline_seconds
        ):
            raise ValueError("Command exceeds worker resource policy")
        approval = Approval.requested(
            new_id(), task_id, run_id, step_id, tool_call_id, spec, self.clock.now()
        )
        await self.approvals.request(approval)
        result = await self.approvals.decide(
            approval.id,
            ApprovalStatus.APPROVED,
            spec.digest,
            None,
            f"worker:{self.job_id}:{tool_call_id}",
            decided_by=f"repair-worker:{self.job_id}",
        )
        return result.approval


class WorkerResourceManager:
    """Dependencies are already in the pinned image; workers admit no new mounts."""

    def __init__(self, job_id: "UUID") -> None:
        self.job_id = job_id

    async def verified_mounts(self, task_id: "UUID") -> tuple[()]:
        from uuid import uuid5

        if task_id != uuid5(self.job_id, "task"):
            raise ValueError("Mount request is outside the private worker task")
        return ()
