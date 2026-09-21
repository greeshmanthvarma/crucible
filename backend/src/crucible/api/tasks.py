from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status

from crucible.api.dependencies import get_task_service
from crucible.api.schemas import CreateTaskRequest, TaskResponse
from crucible.application.task_service import TaskService
from crucible.domain.task import Task

router = APIRouter(tags=["tasks"])

TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]


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
