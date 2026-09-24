from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


def to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class ApiModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class RegisterRepositoryRequest(ApiModel):
    path: str


class CommandLimitsSettings(ApiModel):
    cpus: float = Field(gt=0)
    memory_bytes: int = Field(gt=0)
    pids: int = Field(gt=0)
    output_bytes: int = Field(gt=0)


class ValidationCommandSettings(ApiModel):
    executable: str
    arguments: list[str]
    cwd: str
    timeout_seconds: int = Field(ge=1, le=3600)
    network: Literal["none", "outbound"]
    environment: dict[str, str]
    image: str
    reason: str
    limits: CommandLimitsSettings


class RepositorySettingsRequest(ApiModel):
    validation_commands: list[ValidationCommandSettings] | None = None
    sandbox_image: str | None = None
    sandbox_network: Literal["none", "outbound"] | None = None
    validation_repair_limit: int | None = Field(default=None, ge=0)
    default_cwd: str | None = None
    compaction_threshold: float | None = Field(default=None, gt=0, le=1)
    compaction_model: str | None = None
    compaction_prompt_version: str | None = None
    compaction_attempt_limit: int | None = Field(default=None, ge=0)
    model_input_limit: int | None = Field(default=None, gt=0)
    model_output_reserve: int | None = Field(default=None, ge=0)


class RepositorySettingsResponse(ApiModel):
    validation_commands: list[ValidationCommandSettings]
    sandbox_image: str
    sandbox_network: str
    validation_repair_limit: int
    default_cwd: str
    compaction_threshold: float
    compaction_model: str | None
    compaction_prompt_version: str
    compaction_attempt_limit: int
    model_input_limit: int
    model_output_reserve: int
    schema_version: int


class RepositoryResponse(ApiModel):
    id: UUID
    root_path: str
    head_revision: str
    created_at: datetime
    settings: RepositorySettingsResponse


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
    kind: str


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
    artifact_id: UUID | None


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


class ApprovalDecisionRequest(ApiModel):
    decision: Literal["approved", "denied"]
    spec_digest: str
    reason: str | None = None


class CommandLimitsResponse(ApiModel):
    cpus: float
    memory_bytes: int
    pids: int
    output_bytes: int


class ApprovalCommandSpecResponse(ApiModel):
    executable: str
    arguments: list[str]
    cwd: str
    timeout_seconds: int
    network: str
    environment_names: list[str]
    image: str
    reason: str
    limits: CommandLimitsResponse


class ApprovalResponse(ApiModel):
    id: UUID
    task_id: UUID
    run_id: UUID
    step_id: UUID
    tool_call_id: UUID
    spec: ApprovalCommandSpecResponse
    spec_digest: str
    status: str
    decision_reason: str | None
    decided_by: str | None
    created_at: datetime
    decided_at: datetime | None


class CancelledRunResponse(ApiModel):
    run_id: UUID
    status: str
    outcome_code: str | None
    cancel_requested_at: datetime | None


class ResultRevisionResponse(ApiModel):
    id: UUID
    task_id: UUID
    commit_sha: str
    parent_revision: str
    previous_result_revision_id: UUID | None
    diff_artifact_id: UUID
    validation_snapshot: dict[str, object]
    summary: str
    created_by: str
    created_at: datetime
