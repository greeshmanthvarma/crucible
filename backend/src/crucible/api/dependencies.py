from fastapi import Request

from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.message_service import MessageService
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService


def get_repository_service(request: Request) -> RepositoryService:
    service: RepositoryService = request.app.state.repository_service
    return service


def get_task_service(request: Request) -> TaskService:
    service: TaskService = request.app.state.task_service
    return service


def get_message_service(request: Request) -> MessageService:
    service: MessageService = request.app.state.message_service
    return service


def get_approval_service(request: Request) -> ApprovalService:
    service: ApprovalService = request.app.state.approval_service
    return service


def get_artifact_service(request: Request) -> ArtifactService:
    service: ArtifactService = request.app.state.artifact_service
    return service
