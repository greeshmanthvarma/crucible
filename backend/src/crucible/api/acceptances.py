from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from crucible.api.dependencies import get_acceptance_service
from crucible.api.schemas import ResultRevisionResponse
from crucible.application.acceptance_service import AcceptanceService
from crucible.application.errors import IdempotencyKeyRequired
from crucible.domain.results import ResultRevision

router = APIRouter(tags=["acceptances"])
AcceptanceServiceDependency = Annotated[
    AcceptanceService, Depends(get_acceptance_service)
]


def to_response(result: ResultRevision) -> ResultRevisionResponse:
    return ResultRevisionResponse(
        id=result.id,
        task_id=result.task_id,
        commit_sha=result.commit_sha,
        parent_revision=result.parent_revision,
        previous_result_revision_id=result.previous_result_revision_id,
        diff_artifact_id=result.diff_artifact_id,
        validation_snapshot=result.validation_snapshot,
        summary=result.summary,
        created_by=result.created_by,
        created_at=result.created_at,
    )


@router.post(
    "/api/tasks/{task_id}/acceptances",
    response_model=ResultRevisionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def accept_task(
    task_id: UUID,
    service: AcceptanceServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> ResultRevisionResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    return to_response(await service.accept(task_id, idempotency_key))
