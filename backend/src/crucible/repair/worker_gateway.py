"""Worker-side ModelGateway adapter; credentials never enter request JSON."""

import json
from collections.abc import AsyncIterator
from uuid import UUID

import httpx

from crucible.engine.gateway import ModelError, ModelStreamItem, PreparedModelRequest
from crucible.repair.gateway import WorkerCredential
from crucible.repair.worker_protocol import decode_event, encode_request


class WorkerModelGateway:
    def __init__(
        self,
        credential: WorkerCredential,
        job_id: UUID,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._credential, self._job_id, self._client = credential, job_id, client

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        client = self._client or httpx.AsyncClient(
            timeout=httpx.Timeout(600, connect=10), trust_env=False
        )
        try:
            async with client.stream(
                "POST",
                self._credential.endpoint,
                headers={"Authorization": f"Bearer {self._credential.token}"},
                json={
                    "schema_version": 1,
                    "job_id": str(self._job_id),
                    "request": encode_request(request),
                },
            ) as response:
                if response.status_code != 200:
                    yield ModelError(
                        "worker_gateway_denied",
                        f"Gateway rejected request ({response.status_code})",
                    )
                    return
                buffer = bytearray()
                async for chunk in response.aiter_bytes():
                    buffer.extend(chunk)
                    while b"\n" in buffer:
                        line, _, remaining = buffer.partition(b"\n")
                        buffer = bytearray(remaining)
                        if len(line) > 1_048_576:
                            raise ValueError("Gateway event too large")
                        if line:
                            yield decode_event(json.loads(line))
                    if len(buffer) > 1_048_576:
                        raise ValueError("Gateway event too large")
                if buffer:
                    raise ValueError("Incomplete NDJSON event")
        except (httpx.HTTPError, ValueError, TypeError):
            yield ModelError(
                "worker_gateway_error", "Gateway stream unavailable or invalid"
            )
        finally:
            if self._client is None:
                await client.aclose()
