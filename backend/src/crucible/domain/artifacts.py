from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import ArtifactId, TaskId


@dataclass(frozen=True)
class Artifact:
    id: ArtifactId
    task_id: TaskId
    content_hash: str
    media_type: str
    byte_length: int
    storage_identity: str
    sensitivity: str
    metadata: Mapping[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        if self.byte_length < 0:
            raise ValueError("Artifact byte length cannot be negative")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
