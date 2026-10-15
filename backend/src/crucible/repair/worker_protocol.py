"""Bounded, versioned JSON at the protected worker/gateway boundary."""

import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, JsonValue, TypeAdapter, field_validator

from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    ModelToolDefinition,
    ModelUsage,
    PreparedModelRequest,
    ReasoningDelta,
    TextDelta,
)
from crucible.repair.cases import StrictModel, relative_path
from crucible.repair.identity import canonical_json


class Versioned(StrictModel):
    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("Schema version must be an integer")
        return value


class WorkerJob(Versioned):
    job_id: UUID
    case_id: str = Field(min_length=1)
    repeat_index: int = Field(ge=0, strict=True)
    purpose: Literal["baseline", "candidate", "regression"]
    source_digest: str = Field(min_length=1)
    input_manifest: dict[str, JsonValue]
    settings: dict[str, JsonValue]
    deadline_seconds: float = Field(gt=0, le=600)
    observation_bytes: int = Field(default=1_048_576, gt=0, le=1_048_576, strict=True)


class WorkerObservation(Versioned):
    job_id: UUID
    status: Literal["completed", "unsupported", "error"]
    tool_result: dict[str, JsonValue] | None = None
    context_messages: list[dict[str, JsonValue]] | None = None
    summary: dict[str, JsonValue] | None = None
    error_code: str | None = None
    detail: str | None = None
    artifact_refs: tuple[str, ...] = ()
    command_records: list[dict[str, JsonValue]] = Field(default_factory=list)

    @field_validator("artifact_refs")
    @classmethod
    def contained_references(cls, references: tuple[str, ...]) -> tuple[str, ...]:
        for reference in references:
            relative_path(reference)
        return references


def read_observation(raw: bytes, job: WorkerJob) -> WorkerObservation:
    if len(raw) > job.observation_bytes:
        raise ValueError("Observation exceeds the protected byte limit")
    observation = WorkerObservation.model_validate_json(raw)
    if observation.job_id != job.job_id:
        raise ValueError("Observation job identity mismatch")
    return observation


class WirePart(StrictModel):
    kind: str
    text_content: str | None = None
    tool_call_id: UUID | None = None
    tool_name: str | None = None
    arguments: dict[str, JsonValue] | None = None
    provider_correlation_id: str | None = None


class WireMessage(StrictModel):
    role: ModelRole
    parts: tuple[WirePart, ...]


class WireTool(StrictModel):
    name: str
    description: str
    input_schema: dict[str, JsonValue]
    schema_version: int = Field(default=1, strict=True, ge=1, le=1)


class WireRequest(Versioned):
    run_id: UUID
    step_id: UUID
    model: str = Field(min_length=1)
    messages: tuple[WireMessage, ...]
    tools: tuple[WireTool, ...]
    max_output_tokens: int = Field(gt=0, le=200_000, strict=True)


def encode_request(request: PreparedModelRequest) -> dict[str, JsonValue]:
    payload = json.loads(canonical_json(request))
    return WireRequest.model_validate(payload).model_dump(mode="json")


def decode_request(payload: object) -> PreparedModelRequest:
    wire = WireRequest.model_validate(payload)
    return PreparedModelRequest(
        wire.run_id,
        wire.step_id,
        wire.model,
        tuple(
            ModelMessage(
                message.role,
                tuple(
                    ModelPart(
                        part.kind,
                        part.text_content,
                        part.tool_call_id if part.tool_call_id is not None else None,
                        part.tool_name,
                        part.arguments,
                        part.provider_correlation_id,
                    )
                    for part in message.parts
                ),
            )
            for message in wire.messages
        ),
        tuple(
            ModelToolDefinition(
                tool.name, tool.description, tool.input_schema, tool.schema_version
            )
            for tool in wire.tools
        ),
        wire.max_output_tokens,
    )


class TextEvent(Versioned):
    kind: Literal["text_delta", "reasoning_delta"]
    text: str


class CallEvent(Versioned):
    kind: Literal["tool_call"] = "tool_call"
    id: UUID
    name: str
    arguments: dict[str, JsonValue]
    provider_correlation_id: str | None = None


class UsageEvent(Versioned):
    kind: Literal["usage"] = "usage"
    input_tokens: int | None = Field(default=None, ge=0, strict=True)
    output_tokens: int | None = Field(default=None, ge=0, strict=True)


class StopEvent(Versioned):
    kind: Literal["stop"] = "stop"
    reason: ModelStopReason


class ErrorEvent(Versioned):
    kind: Literal["error"] = "error"
    code: str
    detail: str


Event = Annotated[
    TextEvent | CallEvent | UsageEvent | StopEvent | ErrorEvent,
    Field(discriminator="kind"),
]
_EVENT: TypeAdapter[Event] = TypeAdapter(Event)


def encode_event(event: ModelStreamItem) -> dict[str, JsonValue]:
    if isinstance(event, (TextDelta, ReasoningDelta)):
        return TextEvent(
            kind="text_delta" if isinstance(event, TextDelta) else "reasoning_delta",
            text=event.text,
        ).model_dump(mode="json")
    if isinstance(event, CompleteToolCall):
        return CallEvent.model_validate(json.loads(canonical_json(event))).model_dump(
            mode="json"
        )
    if isinstance(event, ModelUsage):
        return UsageEvent(
            input_tokens=event.input_tokens, output_tokens=event.output_tokens
        ).model_dump(mode="json")
    if isinstance(event, ModelStop):
        return StopEvent(reason=event.reason).model_dump(mode="json")
    if isinstance(event, ModelError):
        return ErrorEvent(code=event.code, detail=event.detail).model_dump(mode="json")
    raise ValueError("Unknown model event")


def decode_event(payload: object) -> ModelStreamItem:
    if not isinstance(payload, dict) or "schema_version" not in payload:
        raise ValueError("Missing event version")
    wire = _EVENT.validate_python(payload)
    if isinstance(wire, TextEvent):
        return (
            TextDelta(wire.text)
            if wire.kind == "text_delta"
            else ReasoningDelta(wire.text)
        )
    if isinstance(wire, CallEvent):
        return CompleteToolCall(
            wire.id, wire.name, wire.arguments, wire.provider_correlation_id
        )
    if isinstance(wire, UsageEvent):
        return ModelUsage(wire.input_tokens, wire.output_tokens)
    if isinstance(wire, StopEvent):
        return ModelStop(wire.reason)
    return ModelError(wire.code, wire.detail)
