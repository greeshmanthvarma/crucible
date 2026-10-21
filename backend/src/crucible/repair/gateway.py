"""Memory-only, per-job model authority and trusted usage accounting."""

import asyncio
import hashlib
import secrets
from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from crucible.domain.clock import Clock
from crucible.engine.gateway import (
    ModelError,
    ModelGateway,
    ModelStop,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
    ReasoningDelta,
    TextDelta,
)
from crucible.models.token_counting import estimate_input_tokens
from crucible.repair.identity import canonical_json


class GatewayDenied(ValueError):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        super().__init__(detail)


@dataclass(frozen=True)
class WorkerCredential:
    token: str = field(repr=False)
    endpoint: str


@dataclass
class GatewaySession:
    session_id: UUID
    job_id: UUID
    token_digest: str = field(repr=False)
    model: str
    max_requests: int
    max_tokens: int
    expires_at: datetime
    revoked: bool = False
    requests: int = 0
    charged_tokens: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    estimated: bool = False


@dataclass(frozen=True)
class TrustedUsage:
    requests: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    basis: Literal["reported", "estimated"] = "reported"


@dataclass(frozen=True)
class Reservation:
    session: GatewaySession
    request: PreparedModelRequest
    input_estimate: int
    tokens: int


class ScopedModelGateway:
    def __init__(self, upstream: ModelGateway, *, clock: Clock, endpoint: str) -> None:
        self.upstream, self.clock, self.endpoint = upstream, clock, endpoint
        self._sessions: dict[str, GatewaySession] = {}
        self._jobs: dict[UUID, GatewaySession] = {}
        self._lock = asyncio.Lock()
        self._revocations: dict[UUID, asyncio.Event] = {}

    def issue(
        self, job_id: UUID, model: str, budgets: Mapping[str, object], ttl: timedelta
    ) -> WorkerCredential:
        requests, tokens = budgets.get("max_requests"), budgets.get("max_tokens")
        if (
            type(requests) is not int
            or type(tokens) is not int
            or requests <= 0
            or tokens <= 0
            or ttl.total_seconds() <= 0
        ):
            raise ValueError("Finite positive gateway budgets and TTL required")
        if job_id in self._jobs:
            raise ValueError("Job credential has already been issued")
        token = secrets.token_urlsafe(32)
        digest = hashlib.sha256(token.encode()).hexdigest()
        session = GatewaySession(
            uuid4(), job_id, digest, model, requests, tokens, self.clock.now() + ttl
        )
        self._sessions[digest] = self._jobs[job_id] = session
        self._revocations[job_id] = asyncio.Event()
        return WorkerCredential(token, self.endpoint)

    def revoke(self, job_id: UUID) -> None:
        if job_id in self._jobs:
            self._jobs[job_id].revoked = True
            self._revocations[job_id].set()

    def usage(self, job_id: UUID) -> TrustedUsage:
        session = self._jobs.get(job_id)
        if session is None:
            return TrustedUsage()
        return TrustedUsage(
            session.requests,
            session.input_tokens,
            session.output_tokens,
            "estimated" if session.estimated else "reported",
        )

    def _authorize(self, token: str, job_id: UUID) -> GatewaySession:
        digest = hashlib.sha256(token.encode()).hexdigest()
        session = self._sessions.get(digest)
        if (
            session is None
            or not secrets.compare_digest(session.token_digest, digest)
            or session.job_id != job_id
            or session.revoked
            or self.clock.now() >= session.expires_at
        ):
            raise GatewayDenied(401, "Invalid or expired worker credential")
        return session

    async def reserve(
        self, token: str, job_id: UUID, request: PreparedModelRequest
    ) -> Reservation:
        async with self._lock:
            session = self._authorize(token, job_id)
            if session.model != request.model:
                raise GatewayDenied(403, "Worker model is outside its scope")
            estimate = estimate_input_tokens(request.messages, request.tools)
            reserved = estimate + request.max_output_tokens
            if (
                session.requests >= session.max_requests
                or session.charged_tokens + reserved > session.max_tokens
            ):
                raise GatewayDenied(429, "Worker model budget exhausted")
            session.requests += 1
            session.charged_tokens += reserved
            return Reservation(session, request, estimate, reserved)

    async def stream(self, reservation: Reservation) -> AsyncIterator[ModelStreamItem]:
        session, request = reservation.session, reservation.request
        reported_input = reported_output = 0
        saw_input = saw_output = False
        output_characters = 0
        stopped = False
        iterator = self.upstream.stream(request).__aiter__()
        revoked = asyncio.create_task(self._revocations[session.job_id].wait())
        pending: asyncio.Future[ModelStreamItem] | None = None
        try:
            async with asyncio.timeout(
                max(0, (session.expires_at - self.clock.now()).total_seconds())
            ):
                while True:
                    if session.revoked:
                        yield ModelError(
                            "worker_session_expired", "Worker session ended"
                        )
                        return
                    pending = asyncio.ensure_future(anext(iterator))
                    done, _ = await asyncio.wait(
                        (pending, revoked), return_when=asyncio.FIRST_COMPLETED
                    )
                    if revoked in done:
                        yield ModelError(
                            "worker_session_expired", "Worker session ended"
                        )
                        return
                    try:
                        event = pending.result()
                    except StopAsyncIteration:
                        break
                    if session.revoked or self.clock.now() >= session.expires_at:
                        yield ModelError(
                            "worker_session_expired", "Worker session ended"
                        )
                        return
                    if isinstance(event, ModelUsage):
                        if any(
                            value is not None and value < 0
                            for value in (event.input_tokens, event.output_tokens)
                        ):
                            raise ValueError("Invalid upstream usage")
                        if event.input_tokens is not None:
                            reported_input += event.input_tokens
                            saw_input = True
                        if event.output_tokens is not None:
                            reported_output += event.output_tokens
                            saw_output = True
                    elif isinstance(event, (TextDelta, ReasoningDelta)):
                        output_characters += len(event.text)
                    else:
                        output_characters += (
                            len(canonical_json(event))
                            if not isinstance(event, ModelStop)
                            else 0
                        )
                    if isinstance(event, ModelStop):
                        stopped = True
                    if isinstance(event, ModelError):
                        yield ModelError(
                            "upstream_error", "Model request could not complete"
                        )
                    else:
                        yield event
                if not stopped:
                    yield ModelError(
                        "upstream_incomplete", "Model stream did not terminate"
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never return raw upstream exceptions, which may contain keys/URLs.
            yield ModelError("upstream_error", "Model request could not complete")
        finally:
            revoked.cancel()
            if pending is not None:
                pending.cancel()
            await asyncio.gather(
                revoked,
                *([pending] if pending is not None else []),
                return_exceptions=True,
            )
            close = getattr(iterator, "aclose", None)
            if close is not None:
                with suppress(Exception):
                    await close()
            async with self._lock:
                estimated_input = (
                    reported_input if saw_input else reservation.input_estimate
                )
                estimated_output = (
                    reported_output
                    if saw_output
                    else max(1, (output_characters + 3) // 4)
                )
                session.input_tokens += estimated_input
                session.output_tokens += estimated_output
                session.estimated |= not (saw_input and saw_output)
                # Uncertain/interrupted usage keeps the full reservation charged.
                actual = estimated_input + estimated_output
                session.charged_tokens += (
                    max(actual, reservation.tokens if not stopped else actual)
                    - reservation.tokens
                )
