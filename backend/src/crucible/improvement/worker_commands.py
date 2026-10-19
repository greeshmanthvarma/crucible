"""Worker projections only. Host broker owns approval, commands and accounting."""

import inspect
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid5

import httpx
from pydantic import TypeAdapter

from crucible.application.approval_service import ApprovalService
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.commands import CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.resources import ExternalResource
from crucible.repair.identity import canonical_json
from crucible.sandbox.protocol import (
    OutputCallback,
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxRequest,
    SandboxTermination,
)


class WorkerResources:
    def __init__(self, job_id: UUID) -> None:
        self.job_id = job_id

    async def verified_mounts(self, task_id: UUID) -> tuple[()]:
        if task_id != uuid5(self.job_id, "task"):
            raise ValueError("task outside worker job")
        return ()


class WorkerAuthority:
    def __init__(self, approvals: ApprovalService, clock: Clock, job_id: UUID) -> None:
        self.approvals, self.clock, self.job_id = approvals, clock, job_id

    async def request_and_wait(
        self,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        tool_call_id: UUID,
        spec: CommandSpec,
    ) -> Approval:
        if task_id != uuid5(self.job_id, "task") or run_id != uuid5(self.job_id, "run"):
            raise ValueError("command outside worker job")
        approval = Approval.requested(
            new_id(), task_id, run_id, step_id, tool_call_id, spec, self.clock.now()
        )
        await self.approvals.request(approval)
        # This disposable projection grants no host effect; host broker decides again.
        result = await self.approvals.decide(
            approval.id,
            ApprovalStatus.APPROVED,
            spec.digest,
            None,
            "worker-projection:" + str(tool_call_id),
            decided_by="host-broker-required",
        )
        return result.approval


class WorkerCommands:
    def __init__(self, job_id: UUID, token: str) -> None:
        self.job_id, self.token = job_id, token

    async def execute(
        self, request: SandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome:
        if request.workspace_path != Path("/workspace") or request.mounts:
            raise ValueError("worker cannot change bound workspace or mounts")
        limit = request.command.limits.output_bytes * 8 + 65536
        retained = 0
        buffer = bytearray()
        outcome = None
        async with httpx.AsyncClient(
            trust_env=False, timeout=request.command.timeout_seconds + 30
        ) as client:
            async with client.stream(
                "POST",
                "http://relay:8080/internal/improvement/command",
                headers={"Authorization": "Bearer " + self.token},
                content=canonical_json(
                    {
                        "schema_version": 1,
                        "job_id": str(self.job_id),
                        "command_id": str(request.tool_call_id),
                        "command": request.command.as_dict(),
                    }
                ),
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    retained += len(chunk)
                    if retained > limit:
                        raise ValueError("command response exceeds cap")
                    buffer.extend(chunk)
                    while b"\n" in buffer:
                        line, _, remaining = buffer.partition(b"\n")
                        buffer = bytearray(remaining)
                        value = json.loads(line)
                        if value["type"] == "chunk":
                            emitted = on_chunk(
                                OutputChunk(
                                    OutputStream(value["stream"]),
                                    value["sequence"],
                                    value["data"].encode(),
                                )
                            )
                            if inspect.isawaitable(emitted):
                                await emitted
                        elif value["type"] == "outcome":
                            outcome = TypeAdapter(SandboxOutcome).validate_json(
                                canonical_json(value["outcome"]), strict=True
                            )
                        elif value["type"] == "denied":
                            now = datetime.now(UTC)
                            return SandboxOutcome(
                                None,
                                SandboxTermination.BACKEND_ERROR,
                                now,
                                now,
                                request.command.image,
                                "denied",
                                0,
                                0,
                                False,
                            )
                        else:
                            raise ValueError("invalid command frame")
        if buffer or outcome is None:
            raise ValueError("incomplete command stream")
        return outcome

    async def cancel(self, container_id: str) -> None:
        pass  # The host revokes/stops all owned effects before snapshots.

    async def reconcile(self, resources: tuple[ExternalResource, ...]) -> None:
        if resources:
            raise ValueError("worker cannot reconcile host resources")
