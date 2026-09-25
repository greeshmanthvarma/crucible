from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import EventId, RunId, TaskId, new_id


class EventType(StrEnum):
    TASK_PROVISIONING_STARTED = "task.provisioning_started"
    TASK_PROVISIONING_SUCCEEDED = "task.provisioning_succeeded"
    TASK_PROVISIONING_FAILED = "task.provisioning_failed"
    RUN_QUEUED = "run.queued"
    RUN_STARTED = "run.started"
    STEP_PREPARING = "step.preparing"
    STEP_MODEL_ACTIVE = "step.model_active"
    STEP_TOOLS_ACTIVE = "step.tools_active"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"
    STEP_INTERRUPTED = "step.interrupted"
    CONTEXT_PREPARED = "context.prepared"
    TOOL_CALL_ADMITTED = "tool_call.admitted"
    TOOL_CALL_STARTED = "tool_call.started"
    TOOL_CALL_COMPLETED = "tool_call.completed"
    APPROVAL_REQUESTED = "approval.requested"
    APPROVAL_APPROVED = "approval.approved"
    APPROVAL_DENIED = "approval.denied"
    APPROVAL_CANCELLED = "approval.cancelled"
    APPROVAL_INVALIDATED = "approval.invalidated"
    COMMAND_OUTPUT = "command.output"
    MESSAGE_COMPLETED = "message.completed"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"
    RUN_INTERRUPTED = "run.interrupted"
    RUN_CANCEL_REQUESTED = "run.cancel_requested"
    RUN_CANCELLED = "run.cancelled"


@dataclass(frozen=True)
class Event:
    id: EventId
    task_id: TaskId
    run_id: RunId | None
    task_sequence: int
    run_sequence: int | None
    type: EventType
    schema_version: int
    payload: Mapping[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        if self.schema_version != 1 or self.payload.get("schema_version") != 1:
            raise ValueError("Event schema_version must be 1 in the row and payload")
        object.__setattr__(self, "payload", _freeze_mapping(self.payload))

    def payload_json(self) -> dict[str, object]:
        return {key: _thaw_value(value) for key, value in self.payload.items()}


class EventFactory:
    """Creates schema-versioned Events without allocating persistence sequences."""

    def create(
        self,
        *,
        task_id: TaskId,
        run_id: RunId | None,
        type: EventType,
        created_at: datetime,
        payload: Mapping[str, object] | None = None,
    ) -> Event:
        return Event(
            id=new_id(),
            task_id=task_id,
            run_id=run_id,
            task_sequence=0,
            run_sequence=0 if run_id is not None else None,
            type=type,
            schema_version=1,
            payload={"schema_version": 1, **(payload or {})},
            created_at=created_at,
        )


def _freeze_mapping(payload: Mapping[str, object]) -> Mapping[str, object]:
    if not all(isinstance(key, str) for key in payload):
        raise TypeError("Event payload object keys must be strings")
    return MappingProxyType(
        {key: _freeze_value(value) for key, value in payload.items()}
    )


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(item) for item in value)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    raise TypeError("Event payload values must be JSON-compatible")


def _thaw_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_value(item) for item in value]
    return value
