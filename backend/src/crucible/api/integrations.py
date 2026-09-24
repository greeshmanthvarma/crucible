from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from crucible.api.dependencies import get_integration_service
from crucible.api.schemas import IntegrationResponse, IntegrationTargetRequest
from crucible.application.errors import IdempotencyKeyRequired
from crucible.application.integration_service import (
    IntegrationService,
    IntegrationTarget,
)
from crucible.domain.results import Integration

router = APIRouter(tags=["integrations"])
IntegrationServiceDependency = Annotated[
    IntegrationService, Depends(get_integration_service)
]


def to_response(integration: Integration) -> IntegrationResponse:
    return IntegrationResponse(
        id=integration.id,
        result_revision_id=integration.result_revision_id,
        repository_id=integration.repository_id,
        target_ref=integration.target_ref,
        expected_target_revision=integration.expected_target_revision,
        status=integration.status,
        observed_before_revision=integration.observed_before_revision,
        observed_after_revision=integration.observed_after_revision,
        failure_code=integration.failure_code,
        failure_detail=integration.failure_detail,
        created_at=integration.created_at,
        completed_at=integration.completed_at,
    )


@router.post(
    "/api/result-revisions/{result_revision_id}/integrations",
    response_model=IntegrationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def integrate_result(
    result_revision_id: UUID,
    request: IntegrationTargetRequest,
    service: IntegrationServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> IntegrationResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    return to_response(
        await service.integrate(
            result_revision_id,
            IntegrationTarget(
                request.repository_id,
                request.target_ref,
                request.expected_revision,
            ),
            idempotency_key,
        )
    )
