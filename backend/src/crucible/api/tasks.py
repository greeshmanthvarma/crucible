from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from crucible.api.dependencies import get_message_service, get_task_service
from crucible.api.schemas import (
    CreateTaskRequest,
    MessagePartResponse,
    MessageResponse,
    SubmitMessageRequest,
    SubmittedRunResponse,
    TaskResponse,
)
from crucible.application.errors import IdempotencyKeyRequired
from crucible.application.message_service import MessageService
from crucible.application.task_service import TaskService
from crucible.domain.task import Task

router = APIRouter(tags=["tasks"])
messages_router = APIRouter(tags=["messages"])

TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]
MessageServiceDependency = Annotated[MessageService, Depends(get_message_service)]


def to_response(task: Task) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        repository_id=task.repository_id,
        source_ref=task.source_ref,
        base_revision=task.base_revision,
        workspace_path=str(task.workspace_path),
        status=task.status,
        failure_code=task.failure_code,
        failure_detail=task.failure_detail,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


@router.post(
    "/api/repositories/{repository_id}/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_task(
    repository_id: UUID,
    request: CreateTaskRequest,
    service: TaskServiceDependency,
) -> TaskResponse:
    return to_response(await service.create(repository_id, request.source_ref))


@router.get("/api/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: UUID,
    service: TaskServiceDependency,
) -> TaskResponse:
    return to_response(await service.get(task_id))


@messages_router.post(
    "/api/tasks/{task_id}/messages",
    response_model=SubmittedRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_message(
    task_id: UUID,
    request: SubmitMessageRequest,
    service: MessageServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> SubmittedRunResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    result = await service.submit(task_id, request.text, idempotency_key)
    return SubmittedRunResponse(
        message_id=result.message_id,
        run_id=result.run_id,
        run_status=result.run_status,
    )


@messages_router.get(
    "/api/tasks/{task_id}/messages",
    response_model=list[MessageResponse],
)
async def list_messages(
    task_id: UUID,
    service: MessageServiceDependency,
) -> list[MessageResponse]:
    return [
        MessageResponse(
            id=message.id,
            task_id=message.task_id,
            run_id=message.run_id,
            conversation_sequence=message.conversation_sequence,
            role=message.role,
            status=message.status,
            parts=[
                MessagePartResponse(
                    id=part.id,
                    part_sequence=part.part_sequence,
                    kind=part.kind,
                    text_content=part.text_content,
                )
                for part in message.parts
            ],
            created_at=message.created_at,
            completed_at=message.completed_at,
        )
        for message in await service.list(task_id)
    ]
