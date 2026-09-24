from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.responses import Response

from crucible.api.dependencies import get_artifact_service
from crucible.application.artifact_service import ArtifactService

router = APIRouter(tags=["artifacts"])
ArtifactServiceDependency = Annotated[ArtifactService, Depends(get_artifact_service)]


@router.get("/api/artifacts/{artifact_id}")
async def get_artifact(
    artifact_id: UUID, service: ArtifactServiceDependency
) -> Response:
    artifact, content = await service.read(artifact_id)
    return Response(content=content, media_type=artifact.media_type)
