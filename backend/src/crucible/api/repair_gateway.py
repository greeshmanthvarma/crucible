"""Bearer-scoped worker route, independent of browser session authority."""

import json
from collections.abc import AsyncIterator
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from crucible.repair.gateway import GatewayDenied, ScopedModelGateway
from crucible.repair.worker_protocol import (
    Versioned,
    WireRequest,
    decode_request,
    encode_event,
)

router = APIRouter()


class GatewayRequest(Versioned):
    job_id: UUID
    request: WireRequest


@router.post("/internal/repair-model/responses", include_in_schema=False)
async def responses(request: Request) -> StreamingResponse:
    authorization = request.headers.get("authorization", "")
    service: ScopedModelGateway | None = getattr(
        request.app.state, "repair_gateway", None
    )
    if not authorization.startswith("Bearer ") or service is None:
        raise HTTPException(401, "Worker bearer credential required")
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > 8 * 1024 * 1024:
            raise HTTPException(413, "Worker request too large")
    try:
        body = GatewayRequest.model_validate_json(raw)
        prepared = decode_request(body.request.model_dump(mode="json"))
        reservation = await service.reserve(authorization[7:], body.job_id, prepared)
    except GatewayDenied as error:
        raise HTTPException(error.status_code, str(error)) from None
    except (ValidationError, ValueError, TypeError):
        raise HTTPException(422, "Invalid worker request") from None

    async def events() -> AsyncIterator[bytes]:
        async for event in service.stream(reservation):
            yield (
                json.dumps(
                    encode_event(event), ensure_ascii=False, separators=(",", ":")
                )
                + "\n"
            ).encode()

    return StreamingResponse(events(), media_type="application/x-ndjson")
