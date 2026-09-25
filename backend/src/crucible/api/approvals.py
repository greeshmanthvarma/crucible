from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header

from crucible.api.dependencies import get_approval_service
from crucible.api.schemas import (
    ApprovalCommandSpecResponse,
    ApprovalDecisionRequest,
    ApprovalResponse,
    CommandLimitsResponse,
)
from crucible.application.approval_service import ApprovalService
from crucible.application.errors import IdempotencyKeyRequired
from crucible.domain.approvals import Approval, ApprovalStatus

router = APIRouter(tags=["approvals"])
ApprovalServiceDependency = Annotated[ApprovalService, Depends(get_approval_service)]


def to_response(approval: Approval) -> ApprovalResponse:
    return ApprovalResponse(
        id=approval.id,
        task_id=approval.task_id,
        run_id=approval.run_id,
        step_id=approval.step_id,
        tool_call_id=approval.tool_call_id,
        spec=ApprovalCommandSpecResponse(
            executable=approval.spec.executable,
            arguments=list(approval.spec.arguments),
            cwd=approval.spec.cwd,
            timeout_seconds=approval.spec.timeout_seconds,
            network=approval.spec.network,
            environment_names=sorted(approval.spec.environment),
            image=approval.spec.image,
            reason=approval.spec.reason,
            limits=CommandLimitsResponse(
                cpus=approval.spec.limits.cpus,
                memory_bytes=approval.spec.limits.memory_bytes,
                pids=approval.spec.limits.pids,
                output_bytes=approval.spec.limits.output_bytes,
            ),
        ),
        spec_digest=approval.spec_digest,
        status=approval.status,
        decision_reason=approval.decision_reason,
        decided_by=approval.decided_by,
        created_at=approval.created_at,
        decided_at=approval.decided_at,
    )


@router.get("/api/tasks/{task_id}/approvals", response_model=list[ApprovalResponse])
async def list_approvals(
    task_id: UUID, service: ApprovalServiceDependency
) -> list[ApprovalResponse]:
    return [to_response(item) for item in await service.list_for_task(task_id)]


@router.post("/api/approvals/{approval_id}/decision", response_model=ApprovalResponse)
async def decide_approval(
    approval_id: UUID,
    request: ApprovalDecisionRequest,
    service: ApprovalServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> ApprovalResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    result = await service.decide(
        approval_id,
        ApprovalStatus(request.decision),
        request.spec_digest,
        request.reason,
        idempotency_key,
    )
    return to_response(result.approval)
