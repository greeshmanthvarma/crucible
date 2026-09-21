from fastapi import Request

from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService


def get_repository_service(request: Request) -> RepositoryService:
    service: RepositoryService = request.app.state.repository_service
    return service


def get_task_service(request: Request) -> TaskService:
    service: TaskService = request.app.state.task_service
    return service
