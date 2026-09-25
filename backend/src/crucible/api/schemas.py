from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator


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


class CreateTaskRequest(ApiModel):
    source_ref: str = "HEAD"


class TaskResponse(ApiModel):
    id: UUID
    repository_id: UUID
    source_ref: str
    base_revision: str | None
    workspace_path: str
    status: str
    failure_code: str | None
    failure_detail: str | None
    created_at: datetime
    updated_at: datetime


class SubmitMessageRequest(ApiModel):
    text: str

    @field_validator("text")
    @classmethod
    def text_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be empty")
        return value


class SubmittedRunResponse(ApiModel):
    message_id: UUID
    run_id: UUID
    run_status: str


class MessagePartResponse(ApiModel):
    id: UUID
    part_sequence: int
    kind: str
    text_content: str | None
    reasoning_content: str | None
    tool_call_id: UUID | None
    tool_result_id: UUID | None


class MessageResponse(ApiModel):
    id: UUID
    task_id: UUID
    run_id: UUID | None
    step_id: UUID | None
    conversation_sequence: int
    role: str
    status: str
    parts: list[MessagePartResponse]
    created_at: datetime
    completed_at: datetime


class TaskEventEnvelope(ApiModel):
    event_id: UUID
    task_id: UUID
    run_id: UUID | None
    task_sequence: int
    run_sequence: int | None
    type: str
    schema_version: int
    payload: dict[str, object]
    created_at: datetime


class ContextManifestSummary(ApiModel):
    id: UUID
    model: str
    estimated_tokens: int
    instruction_digests: dict[str, str]
    tool_schema_digest: str


class ToolCallResponse(ApiModel):
    id: UUID
    call_sequence: int
    name: str
    arguments: dict[str, object]
    status: str
    execution_mode: str


class ToolResultResponse(ApiModel):
    id: UUID
    tool_call_id: UUID
    status: str
    result: dict[str, object]
    display_text: str
    error_code: str | None
    completion_sequence: int


class StepTraceResponse(ApiModel):
    id: UUID
    run_id: UUID
    step_sequence: int
    status: str
    manifest: ContextManifestSummary | None
    calls: list[ToolCallResponse]
    results: list[ToolResultResponse]


class WorkspaceStateResponse(ApiModel):
    status: str
    diff: str
    status_truncated: bool
    diff_truncated: bool
