from fastapi import Request

from crucible.application.acceptance_service import AcceptanceService
from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.integration_service import IntegrationService
from crucible.application.message_service import MessageService
from crucible.application.repository_service import RepositoryService
from crucible.application.run_service import RunService
from crucible.application.task_service import TaskService


def get_repository_service(request: Request) -> RepositoryService:
    service: RepositoryService = request.app.state.repository_service
    return service


def get_acceptance_service(request: Request) -> AcceptanceService:
    service: AcceptanceService = request.app.state.acceptance_service
    return service


def get_integration_service(request: Request) -> IntegrationService:
    service: IntegrationService = request.app.state.integration_service
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


def get_run_service(request: Request) -> RunService:
    service: RunService = request.app.state.run_service
    return service
