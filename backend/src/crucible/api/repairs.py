from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header

from crucible.api.dependencies import get_repair_service
from crucible.api.schemas import RepairStatusResponse
from crucible.application.errors import IdempotencyKeyRequired
from crucible.application.repair_service import RepairService
from crucible.domain.repair import RepairRun

router = APIRouter(tags=["repairs"])
Service = Annotated[RepairService, Depends(get_repair_service)]


async def status_response(
    service: RepairService, run: RepairRun
) -> RepairStatusResponse:
    return RepairStatusResponse.model_validate(await service.public_status(run))


@router.get("/api/tasks/{task_id}/repairs", response_model=list[RepairStatusResponse])
async def list_repairs(task_id: UUID, service: Service) -> list[RepairStatusResponse]:
    return [
        await status_response(service, run)
        for run in await service.list_for_task(task_id)
    ]


@router.post("/api/repairs/{repair_id}/stop", response_model=RepairStatusResponse)
async def stop_repair(
    repair_id: UUID,
    service: Service,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> RepairStatusResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    return await status_response(
        service, await service.stop(repair_id, idempotency_key)
    )
