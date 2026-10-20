"""Trusted job-bound model/command authority; no arbitrary proxy destinations."""

import asyncio
import hashlib
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid5

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import JsonValue

from crucible.domain.commands import CommandNetwork, CommandSpec
from crucible.engine.gateway import (
    ModelError,
    ModelStop,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
)
from crucible.improvement.contracts import ExecutionManifest, UsageAmounts
from crucible.improvement.store import ExecutionStore, QuotaExhausted
from crucible.improvement.worker_protocol import (
    CommandRecord,
    RoleJob,
    WorkerRequest,
    job_environment,
    validate_job_manifest,
)
from crucible.repair.gateway import GatewayDenied, ScopedModelGateway, WorkerCredential
from crucible.repair.identity import canonical_json
from crucible.repair.worker_protocol import WireRequest, decode_request, encode_event
from crucible.sandbox.protocol import OutputChunk, SandboxOutcome

MODEL_ROUTE = "/internal/improvement/model"
COMMAND_ROUTE = "/internal/improvement/command"


@dataclass(frozen=True)
class WorkspaceBinding:
    path: Path
    image: str
    platform: str


class CommandExecutor(Protocol):
    async def execute(
        self,
        job: WorkerRequest,
        binding: WorkspaceBinding,
        command_id: UUID,
        spec: CommandSpec,
    ) -> tuple[tuple[OutputChunk, ...], SandboxOutcome]: ...
    async def stop(self, job_id: UUID) -> None: ...


@dataclass
class JobSession:
    job: WorkerRequest
    manifest: ExecutionManifest
    binding: WorkspaceBinding
    token_hash: str = field(repr=False)
    revoked: bool = False
    command_calls: int = 0
    records: list[CommandRecord] = field(default_factory=list)
    commands: dict[UUID, asyncio.Task[bytes]] = field(default_factory=dict)
    command_digests: dict[UUID, str] = field(default_factory=dict)
    issued_at: float = field(default_factory=time.monotonic)
    approval_started: float | None = None
    approval_seconds: float = 0.0
    approval_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


Approval = Callable[[WorkerRequest, CommandSpec], Awaitable[bool]]


class ImprovementBroker:
    def __init__(
        self,
        store: ExecutionStore,
        gateway: ScopedModelGateway | None,
        commands: CommandExecutor | None,
        approval: Approval | None = None,
    ) -> None:
        self.store, self.gateway, self.commands, self.approval = (
            store,
            gateway,
            commands,
            approval,
        )
        self.sessions: dict[UUID, JobSession] = {}
        self.pause_worker: Callable[[UUID, bool], Awaitable[None]] | None = None

    def issue(
        self,
        job: WorkerRequest,
        manifest: ExecutionManifest,
        workspace_binding: WorkspaceBinding,
    ) -> WorkerCredential:
        if (workspace_binding.image, workspace_binding.platform) != job_environment(
            job, manifest
        ):
            raise ValueError("workspace environment is not bound to admitted task")
        if (
            not workspace_binding.path.is_absolute()
            or workspace_binding.path.is_symlink()
        ):
            raise ValueError("invalid workspace binding")
        validate_job_manifest(job, manifest)
        if job.job_id in self.sessions or self.gateway is None:
            raise ValueError("job already issued or model gateway unavailable")
        credential = self.gateway.issue(
            job.job_id,
            manifest.model.model_id,
            {
                "max_requests": manifest.budget.max_model_requests,
                "max_tokens": job.limits.max_model_tokens,
            },
            timedelta(seconds=job.wall_deadline_seconds),
        )
        self.sessions[job.job_id] = JobSession(
            job,
            manifest,
            workspace_binding,
            hashlib.sha256(credential.token.encode()).hexdigest(),
        )
        return WorkerCredential(credential.token, "http://relay:8080" + MODEL_ROUTE)

    def _authorize(self, token: str, job_id: UUID) -> JobSession:
        session = self.sessions.get(job_id)
        if (
            session is None
            or session.revoked
            or time.monotonic() - session.issued_at >= session.job.wall_deadline_seconds
            or not secrets.compare_digest(
                session.token_hash, hashlib.sha256(token.encode()).hexdigest()
            )
        ):
            raise GatewayDenied(401, "invalid job credential")
        return session

    def revoke(self, job_id: UUID) -> None:
        if job_id in self.sessions:
            self.sessions[job_id].revoked = True
        if self.gateway:
            self.gateway.revoke(job_id)

    async def model(
        self, token: str, job_id: UUID, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        session = self._authorize(token, job_id)
        async with session.operation_lock:
            async for event in self._model(token, job_id, request):
                yield event

    async def _model(
        self, token: str, job_id: UUID, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        session = self._authorize(token, job_id)
        if request.max_output_tokens > session.job.model.output_reserve:
            raise GatewayDenied(403, "request outside frozen run/model scope")
        request = replace(request, run_id=uuid5(job_id, "run"))
        assert self.gateway is not None
        try:
            reservation = await self.gateway.reserve(token, job_id, request)
            # Persist before opening upstream. A global denial spends no model request.
            ledger = await self.store.reserve_usage(
                session.job.experiment_id,
                session.job.trial_id,
                UsageAmounts(
                    tokens=reservation.tokens,
                    requests=1,
                    output_bytes=session.job.output_limit_bytes,
                ),
                job_id=session.job.job_id,
            )
        except GatewayDenied as error:
            if error.status_code != 429:
                raise
            yield ModelError("improvement_budget_exhausted", "Model quota exhausted")
            return
        except QuotaExhausted:
            yield ModelError("improvement_budget_exhausted", "Model quota exhausted")
            return
        input_tokens = output_tokens = 0
        saw_input = saw_output = stopped = failed = False
        size = 0
        try:
            async for event in self.gateway.stream(reservation):
                if isinstance(event, ModelUsage):
                    if event.input_tokens is not None:
                        input_tokens += event.input_tokens
                        saw_input = True
                    if event.output_tokens is not None:
                        output_tokens += event.output_tokens
                        saw_output = True
                if isinstance(event, ModelStop):
                    stopped = True
                if isinstance(event, ModelError):
                    failed = True
                size += len(canonical_json(encode_event(event))) + 1
                if size > session.job.output_limit_bytes:
                    raise ValueError("model output cap exceeded")
                yield event
        finally:
            if stopped and saw_input and saw_output and not failed:
                await self.store.settle_usage(
                    ledger,
                    UsageAmounts(
                        tokens=input_tokens + output_tokens,
                        requests=1,
                        output_bytes=size,
                    ),
                )
            else:
                await self.store.mark_uncertain(ledger)
                self.revoke(job_id)

    async def command(
        self, token: str, job_id: UUID, command_id: UUID, spec: CommandSpec
    ) -> bytes:
        session = self._authorize(token, job_id)
        if isinstance(session.job, RoleJob) and session.job.job_kind != "proposal":
            raise GatewayDenied(403, "mining commands forbidden")
        async with session.lock:
            existing = session.commands.get(command_id)
            if (
                existing is not None
                and session.command_digests[command_id] != spec.digest
            ):
                raise GatewayDenied(409, "command replay payload differs")
            if existing is None:
                session.command_calls += 1
                if (
                    session.command_calls
                    > session.manifest.command_policy.max_calls_per_trial
                ):
                    return (
                        canonical_json(
                            {"type": "denied", "command_id": str(command_id)}
                        )
                        + b"\n"
                    )
                session.command_digests[command_id] = spec.digest
                existing = asyncio.create_task(
                    self._command_serialized(
                        session, command_id, spec, session.command_calls
                    )
                )
                session.commands[command_id] = existing
        # Replay returns the original result; it never starts a second effect.
        return await asyncio.shield(existing)

    async def _command_serialized(
        self, session: JobSession, command_id: UUID, spec: CommandSpec, sequence: int
    ) -> bytes:
        async with session.operation_lock:
            return await self._command(session, command_id, spec, sequence)

    async def _command(
        self, session: JobSession, command_id: UUID, spec: CommandSpec, sequence: int
    ) -> bytes:
        policy = session.manifest.command_policy
        job = session.job
        allowed = (
            sequence <= policy.max_calls_per_trial
            and spec.image == session.binding.image
            and spec.network == CommandNetwork.NONE
            and not session.revoked
        )
        if (
            spec.environment
            or spec.limits.cpus > session.manifest.grader.cpus
            or spec.limits.memory_bytes > session.manifest.grader.memory_bytes
            or spec.limits.pids > 256
        ):
            allowed = False
        spec = replace(
            spec,
            timeout_seconds=min(spec.timeout_seconds, policy.max_seconds_per_call),
            limits=replace(
                spec.limits,
                output_bytes=min(
                    spec.limits.output_bytes, policy.max_output_bytes_per_call
                ),
            ),
        )
        cwd = session.binding.path / spec.cwd
        if cwd.is_symlink() or not cwd.resolve().is_relative_to(
            session.binding.path.resolve()
        ):
            allowed = False
        if allowed and policy.mode == "interactive":
            if self.approval is None or self.pause_worker is None:
                allowed = False
            else:
                await self.pause_worker(job.job_id, True)
                session.approval_started = time.monotonic()
                try:
                    await self.store.record_event(
                        job.experiment_id,
                        "command_approval_requested",
                        {
                            "job_id": str(job.job_id),
                            "command_id": str(command_id),
                            "spec_digest": spec.digest,
                        },
                    )
                    allowed = await self.approval(job, spec)
                    await self.store.record_event(
                        job.experiment_id,
                        "command_approval",
                        {
                            "job_id": str(job.job_id),
                            "command_id": str(command_id),
                            "spec_digest": spec.digest,
                            "approved": allowed,
                        },
                    )
                finally:
                    try:
                        await self.pause_worker(job.job_id, False)
                    except BaseException:
                        self.revoke(job.job_id)
                        await self.store.request_stop(job.experiment_id)
                        raise
                    elapsed = time.monotonic() - session.approval_started
                    session.approval_seconds += elapsed
                    session.approval_started = None
                    await self.store.record_event(
                        job.experiment_id,
                        "command_approval_wait",
                        {
                            "job_id": str(job.job_id),
                            "command_id": str(command_id),
                            "seconds": elapsed,
                        },
                    )
        if not allowed or self.commands is None:
            session.records.append(CommandRecord(command_id, spec.digest, False, None))
            return (
                canonical_json({"type": "denied", "command_id": str(command_id)})
                + b"\n"
            )
        reservation = await self.store.reserve_usage(
            job.experiment_id,
            job.trial_id,
            UsageAmounts(output_bytes=spec.limits.output_bytes),
            kind="command",
            job_id=job.job_id,
        )
        try:
            chunks, outcome = await self.commands.execute(
                job, session.binding, command_id, spec
            )
            payload = b"".join(
                canonical_json(
                    {
                        "type": "chunk",
                        "stream": c.stream.value,
                        "sequence": c.sequence,
                        "data": c.data.decode("utf-8", errors="replace"),
                    }
                )
                + b"\n"
                for c in chunks
            )
            if len(payload) > policy.max_output_bytes_per_call * 8:
                raise ValueError("command wire output cap exceeded")
            payload += canonical_json({"type": "outcome", "outcome": outcome}) + b"\n"
            session.records.append(
                CommandRecord(command_id, spec.digest, True, outcome)
            )
            await self.store.settle_usage(
                reservation,
                UsageAmounts(
                    output_bytes=outcome.retained_bytes,
                ),
            )
            return payload
        except BaseException:
            await self.store.mark_uncertain(reservation)
            self.revoke(job.job_id)
            raise

    def approval_wait_seconds(self, job_id: UUID) -> float:
        session = self.sessions[job_id]
        return session.approval_seconds + (
            time.monotonic() - session.approval_started
            if session.approval_started is not None
            else 0.0
        )

    async def stop(self, job_id: UUID) -> None:
        self.revoke(job_id)
        session = self.sessions.get(job_id)
        if session:
            for task in session.commands.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*session.commands.values(), return_exceptions=True)
        if self.commands:
            await self.commands.stop(job_id)


def broker_app(broker: ImprovementBroker) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    async def read(request: Request) -> tuple[str, dict[str, JsonValue]]:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(401, "job credential required")
        raw = bytearray()
        async for chunk in request.stream():
            if len(raw) + len(chunk) > 8 * 1024 * 1024:
                raise HTTPException(413, "request too large")
            raw.extend(chunk)
        import json

        body = json.loads(raw)
        if not isinstance(body, dict):
            raise HTTPException(422, "invalid request")
        return auth[7:], body

    @app.post(MODEL_ROUTE)
    async def model(request: Request) -> StreamingResponse:
        token, body = await read(request)
        try:
            if (
                set(body) != {"schema_version", "job_id", "request"}
                or body["schema_version"] != 1
            ):
                raise ValueError("invalid envelope")
            job_id = UUID(str(body["job_id"]))
            broker._authorize(token, job_id)
            prepared = decode_request(
                WireRequest.model_validate(body["request"]).model_dump(mode="json")
            )
        except GatewayDenied as error:
            raise HTTPException(error.status_code, "credential denied") from None
        except (ValueError, TypeError) as error:
            raise HTTPException(422, "invalid request") from error

        async def events() -> AsyncIterator[bytes]:
            async for event in broker.model(token, job_id, prepared):
                yield canonical_json(encode_event(event)) + b"\n"

        return StreamingResponse(events(), media_type="application/x-ndjson")

    @app.post(COMMAND_ROUTE)
    async def command(request: Request) -> StreamingResponse:
        token, body = await read(request)
        from dataclasses import fields

        from pydantic import TypeAdapter

        try:
            if (
                set(body) != {"schema_version", "job_id", "command_id", "command"}
                or body["schema_version"] != 1
            ):
                raise ValueError("invalid envelope")
            payload = body["command"]
            if not isinstance(payload, dict) or set(payload) != {
                f.name for f in fields(CommandSpec)
            }:
                raise ValueError("invalid command fields")
            spec = TypeAdapter(CommandSpec).validate_json(
                canonical_json(payload), strict=True
            )
            data = await broker.command(
                token, UUID(str(body["job_id"])), UUID(str(body["command_id"])), spec
            )
        except GatewayDenied as error:
            raise HTTPException(error.status_code, "credential denied") from None
        except (ValueError, TypeError, KeyError):
            raise HTTPException(422, "invalid command") from None

        async def output() -> AsyncIterator[bytes]:
            yield data

        return StreamingResponse(output(), media_type="application/x-ndjson")

    return app
