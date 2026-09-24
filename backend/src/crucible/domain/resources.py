from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import ExternalResourceId, RunId, TaskId, ToolCallId


class ExternalResourceKind(StrEnum):
    VOLUME = "volume"
    CONTAINER = "container"


class ExternalResourceStatus(StrEnum):
    PRESENT = "present"
    ACTIVE = "active"
    REMOVED = "removed"
    MISSING = "missing"
    ORPHANED = "orphaned"
    ERROR = "error"


@dataclass(frozen=True)
class ExternalResource:
    id: ExternalResourceId
    task_id: TaskId
    run_id: RunId | None
    tool_call_id: ToolCallId | None
    kind: ExternalResourceKind
    external_identity: str
    mount_target: str | None
    status: ExternalResourceStatus
    labels: Mapping[str, str]
    metadata: Mapping[str, object]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.updated_at)
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @classmethod
    def volume(
        cls,
        resource_id: ExternalResourceId,
        task_id: TaskId,
        external_identity: str,
        mount_target: str,
        now: datetime,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> "ExternalResource":
        return cls(
            resource_id,
            task_id,
            None,
            None,
            ExternalResourceKind.VOLUME,
            external_identity,
            mount_target,
            ExternalResourceStatus.PRESENT,
            labels or {},
            {},
            now,
            now,
        )
