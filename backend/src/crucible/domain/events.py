from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import EventId, RunId, TaskId


class EventType(StrEnum):
    TASK_PROVISIONING_STARTED = "task.provisioning_started"
    TASK_PROVISIONING_SUCCEEDED = "task.provisioning_succeeded"
    TASK_PROVISIONING_FAILED = "task.provisioning_failed"
    RUN_QUEUED = "run.queued"
    RUN_STARTED = "run.started"
    MESSAGE_COMPLETED = "message.completed"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"
    RUN_INTERRUPTED = "run.interrupted"


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
