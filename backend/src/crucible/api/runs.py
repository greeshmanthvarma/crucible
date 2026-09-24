from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header

from crucible.api.dependencies import get_run_service
from crucible.api.schemas import CancelledRunResponse
from crucible.application.errors import IdempotencyKeyRequired
from crucible.application.run_service import RunService

router = APIRouter(tags=["runs"])
RunServiceDependency = Annotated[RunService, Depends(get_run_service)]


@router.post("/api/runs/{run_id}/cancel", response_model=CancelledRunResponse)
async def cancel_run(
    run_id: UUID,
    service: RunServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> CancelledRunResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    result = await service.cancel(run_id, idempotency_key)
    return CancelledRunResponse(
        run_id=result.run.id,
        status=result.run.status,
        outcome_code=result.run.outcome_code,
        cancel_requested_at=result.run.cancel_requested_at,
    )
