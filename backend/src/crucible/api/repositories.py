from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status

from crucible.api.dependencies import get_repository_service
from crucible.api.schemas import (
    CommandLimitsSettings,
    RegisterRepositoryRequest,
    RepositoryResponse,
    RepositorySettingsRequest,
    RepositorySettingsResponse,
    ValidationCommandSettings,
)
from crucible.application.repository_service import (
    RegisteredRepository,
    RepositoryService,
)
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.repository import RepositorySettings

router = APIRouter(prefix="/api/repositories", tags=["repositories"])

RepositoryServiceDependency = Annotated[
    RepositoryService, Depends(get_repository_service)
]


def to_response(result: RegisteredRepository) -> RepositoryResponse:
    repository = result.repository
    settings = repository.settings
    return RepositoryResponse(
        id=repository.id,
        root_path=str(repository.root_path),
        head_revision=result.head_revision,
        created_at=repository.created_at,
        settings=RepositorySettingsResponse(
            validation_commands=[
                ValidationCommandSettings(
                    executable=command.executable,
                    arguments=list(command.arguments),
                    cwd=command.cwd,
                    timeout_seconds=command.timeout_seconds,
                    network=command.network.value,
                    environment=dict(command.environment),
                    image=command.image,
                    reason=command.reason,
                    limits=CommandLimitsSettings(
                        cpus=command.limits.cpus,
                        memory_bytes=command.limits.memory_bytes,
                        pids=command.limits.pids,
                        output_bytes=command.limits.output_bytes,
                    ),
                )
                for command in settings.validation_commands
            ],
            sandbox_image=settings.sandbox_image,
            sandbox_network=settings.sandbox_network,
            validation_repair_limit=settings.validation_repair_limit,
            default_cwd=settings.default_cwd,
            compaction_threshold=settings.compaction_threshold,
            compaction_model=settings.compaction_model,
            compaction_prompt_version=settings.compaction_prompt_version,
            compaction_attempt_limit=settings.compaction_attempt_limit,
            model_input_limit=settings.model_input_limit,
            model_output_reserve=settings.model_output_reserve,
            schema_version=settings.schema_version,
        ),
    )


def merge_settings(
    current: RepositorySettings, request: RepositorySettingsRequest
) -> RepositorySettings:
    changes = request.model_dump(exclude_unset=True)
    commands = changes.pop("validation_commands", None)
    if commands is not None:
        changes["validation_commands"] = tuple(
            CommandSpec(
                executable=command["executable"],
                arguments=tuple(command["arguments"]),
                cwd=command["cwd"],
                timeout_seconds=command["timeout_seconds"],
                network=CommandNetwork(command["network"]),
                environment=command["environment"],
                image=command["image"],
                reason=command["reason"],
                limits=CommandLimits(**command["limits"]),
            )
            for command in commands
        )
    values = {**current.__dict__, **changes}
    return RepositorySettings(**values)


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


@router.put("/{repository_id}/settings", response_model=RepositoryResponse)
async def update_repository_settings(
    repository_id: UUID,
    request: RepositorySettingsRequest,
    service: RepositoryServiceDependency,
) -> RepositoryResponse:
    repository = await service.get(repository_id)
    return to_response(
        await service.update_settings(
            repository_id, merge_settings(repository.settings, request)
        )
    )
