from collections.abc import Mapping
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from crucible.api.dependencies import get_message_service, get_task_service
from crucible.api.schemas import (
    ContextManifestSummary,
    CreateTaskRequest,
    IntegrationResponse,
    MessagePartResponse,
    MessageResponse,
    ResultRevisionResponse,
    StepTraceResponse,
    SubmitMessageRequest,
    SubmittedRunResponse,
    TaskResponse,
    TaskReviewResponse,
    ToolCallResponse,
    ToolResultResponse,
    ValidationAttemptReviewResponse,
    ValidationCommandReviewResponse,
    WorkspaceStateResponse,
)
from crucible.application.errors import IdempotencyKeyRequired
from crucible.application.message_service import MessageService
from crucible.application.task_service import TaskService
from crucible.domain.task import Task

router = APIRouter(tags=["tasks"])
messages_router = APIRouter(tags=["messages"])

TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]
MessageServiceDependency = Annotated[MessageService, Depends(get_message_service)]


def to_response(task: Task) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        repository_id=task.repository_id,
        source_ref=task.source_ref,
        base_revision=task.base_revision,
        workspace_path=str(task.workspace_path),
        workspace_generation=task.workspace_generation,
        workspace_base_revision=task.workspace_base_revision or task.base_revision,
        status=task.status,
        failure_code=task.failure_code,
        failure_detail=task.failure_detail,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


@router.post(
    "/api/repositories/{repository_id}/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_task(
    repository_id: UUID,
    request: CreateTaskRequest,
    service: TaskServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> TaskResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    return to_response(
        await service.create(repository_id, request.source_ref, idempotency_key)
    )


@router.get("/api/tasks", response_model=list[TaskResponse])
async def list_tasks(service: TaskServiceDependency) -> list[TaskResponse]:
    return [to_response(task) for task in await service.list()]


@router.get("/api/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: UUID,
    service: TaskServiceDependency,
) -> TaskResponse:
    return to_response(await service.get(task_id))


@router.get("/api/tasks/{task_id}/trace", response_model=list[StepTraceResponse])
async def get_task_trace(
    task_id: UUID,
    service: TaskServiceDependency,
) -> list[StepTraceResponse]:
    return [
        StepTraceResponse(
            id=trace.step.id,
            run_id=trace.step.run_id,
            step_sequence=trace.step.step_sequence,
            status=trace.step.status,
            manifest=ContextManifestSummary(
                id=trace.manifest.id,
                model=trace.manifest.model,
                estimated_tokens=trace.manifest.estimated_tokens,
                instruction_digests=dict(trace.manifest.instruction_digests),
                tool_schema_digest=trace.manifest.tool_schema_digest,
            )
            if trace.manifest is not None
            else None,
            calls=[
                ToolCallResponse(
                    id=call.id,
                    call_sequence=call.call_sequence,
                    name=call.name,
                    arguments=_public_tool_arguments(call.name, call.arguments),
                    status=call.status,
                    execution_mode=call.execution_mode,
                )
                for call in trace.calls
            ],
            results=[
                ToolResultResponse(
                    id=result.id,
                    tool_call_id=result.tool_call_id,
                    status=result.status,
                    result=dict(result.result),
                    display_text=result.display_text,
                    error_code=result.error_code,
                    completion_sequence=result.completion_sequence,
                    artifact_id=result.artifact_id,
                )
                for result in trace.results
            ],
        )
        for trace in await service.trace(task_id)
    ]


def _public_tool_arguments(
    name: str, arguments: Mapping[str, object]
) -> dict[str, object]:
    public = dict(arguments)
    if name == "execute_command":
        environment = public.pop("environment", {})
        if isinstance(environment, Mapping):
            public["environmentNames"] = sorted(str(key) for key in environment)
    return public


@router.get("/api/tasks/{task_id}/workspace", response_model=WorkspaceStateResponse)
async def get_workspace_state(
    task_id: UUID,
    service: TaskServiceDependency,
) -> WorkspaceStateResponse:
    state = await service.workspace_state(task_id)
    return WorkspaceStateResponse(
        status=state.status,
        diff=state.diff,
        status_truncated=state.status_truncated,
        diff_truncated=state.diff_truncated,
    )


@router.get("/api/tasks/{task_id}/review", response_model=TaskReviewResponse)
async def get_task_review(
    task_id: UUID, service: TaskServiceDependency
) -> TaskReviewResponse:
    review = await service.review(task_id)
    return TaskReviewResponse(
        latest_run_status=review.latest_run_status,
        completion_summary=review.completion_summary,
        claimed_files=list(review.claimed_files),
        validation_attempts=[
            ValidationAttemptReviewResponse(
                id=item.attempt.id,
                run_id=item.attempt.run_id,
                attempt_number=item.attempt.attempt_number,
                status=item.attempt.status,
                created_at=item.attempt.created_at,
                completed_at=item.attempt.completed_at,
                commands=[
                    ValidationCommandReviewResponse(
                        id=command.id,
                        command_sequence=command.command_sequence,
                        status=command.status,
                        approval_id=command.approval_id,
                        tool_call_id=command.tool_call_id,
                        artifact_id=command.artifact_id,
                        exit_code=command.exit_code,
                        summary=command.summary,
                        created_at=command.created_at,
                        completed_at=command.completed_at,
                    )
                    for command in item.commands
                ],
            )
            for item in review.validations
        ],
        result_revisions=[
            ResultRevisionResponse(
                id=result.id,
                task_id=result.task_id,
                commit_sha=result.commit_sha,
                parent_revision=result.parent_revision,
                previous_result_revision_id=result.previous_result_revision_id,
                diff_artifact_id=result.diff_artifact_id,
                validation_snapshot=result.validation_snapshot,
                summary=result.summary,
                created_by=result.created_by,
                created_at=result.created_at,
            )
            for result in review.result_revisions
        ],
        integrations=[
            IntegrationResponse(
                id=item.id,
                result_revision_id=item.result_revision_id,
                repository_id=item.repository_id,
                target_ref=item.target_ref,
                expected_target_revision=item.expected_target_revision,
                status=item.status,
                observed_before_revision=item.observed_before_revision,
                observed_after_revision=item.observed_after_revision,
                failure_code=item.failure_code,
                failure_detail=item.failure_detail,
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in review.integrations
        ],
    )


@messages_router.post(
    "/api/tasks/{task_id}/messages",
    response_model=SubmittedRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_message(
    task_id: UUID,
    request: SubmitMessageRequest,
    service: MessageServiceDependency,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> SubmittedRunResponse:
    if idempotency_key is None or not idempotency_key.strip():
        raise IdempotencyKeyRequired("Idempotency-Key header is required")
    result = await service.submit(task_id, request.text, idempotency_key)
    return SubmittedRunResponse(
        message_id=result.message_id,
        run_id=result.run_id,
        run_status=result.run_status,
        kind=result.kind,
    )


@messages_router.get(
    "/api/tasks/{task_id}/messages",
    response_model=list[MessageResponse],
)
async def list_messages(
    task_id: UUID,
    service: MessageServiceDependency,
) -> list[MessageResponse]:
    return [
        MessageResponse(
            id=message.id,
            task_id=message.task_id,
            run_id=message.run_id,
            step_id=message.step_id,
            conversation_sequence=message.conversation_sequence,
            role=message.role,
            status=message.status,
            parts=[
                MessagePartResponse(
                    id=part.id,
                    part_sequence=part.part_sequence,
                    kind=part.kind,
                    text_content=part.text_content,
                    reasoning_content=part.reasoning_content,
                    tool_call_id=part.tool_call_id,
                    tool_result_id=part.tool_result_id,
                )
                for part in message.parts
            ],
            created_at=message.created_at,
            completed_at=message.completed_at,
        )
        for message in await service.list(task_id)
    ]
