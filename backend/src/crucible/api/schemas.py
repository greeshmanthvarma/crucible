from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


def to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class ApiModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class RegisterRepositoryRequest(ApiModel):
    path: str


class RepositoryResponse(ApiModel):
    id: UUID
    root_path: str
    head_revision: str
    created_at: datetime


class ErrorResponse(ApiModel):
    code: str
    detail: str
