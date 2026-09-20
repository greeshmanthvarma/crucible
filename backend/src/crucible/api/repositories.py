from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from crucible.api.dependencies import get_repository_service
from crucible.api.schemas import RegisterRepositoryRequest, RepositoryResponse
from crucible.application.repository_service import (
    RegisteredRepository,
    RepositoryService,
)

router = APIRouter(prefix="/api/repositories", tags=["repositories"])

RepositoryServiceDependency = Annotated[
    RepositoryService, Depends(get_repository_service)
]


def to_response(result: RegisteredRepository) -> RepositoryResponse:
    repository = result.repository
    return RepositoryResponse(
        id=repository.id,
        root_path=str(repository.root_path),
        head_revision=result.head_revision,
        created_at=repository.created_at,
    )


@router.post("", response_model=RepositoryResponse)
async def register_repository(
    request: RegisterRepositoryRequest,
    response: Response,
    service: RepositoryServiceDependency,
) -> RepositoryResponse:
    result = await service.register(Path(request.path))
    response.status_code = (
        status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    )
    return to_response(result)


@router.get("", response_model=list[RepositoryResponse])
async def list_repositories(
    service: RepositoryServiceDependency,
) -> list[RepositoryResponse]:
    return [to_response(result) for result in await service.list()]
