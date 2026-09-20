from fastapi import Request

from crucible.application.repository_service import RepositoryService


def get_repository_service(request: Request) -> RepositoryService:
    service: RepositoryService = request.app.state.repository_service
    return service
