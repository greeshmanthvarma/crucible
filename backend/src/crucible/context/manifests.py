from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping

from crucible.domain.clock import require_utc
from crucible.domain.ids import (
    ContextManifestId,
    MessageId,
    MessagePartId,
    RunId,
    StepId,
    TaskId,
)


@dataclass(frozen=True)
class ContextManifest:
    id: ContextManifestId
    task_id: TaskId
    run_id: RunId
    step_id: StepId
    model: str
    parameters: Mapping[str, object]
    input_limit: int
    output_reserve: int
    threshold: float
    estimated_tokens: int
    message_ids: tuple[MessageId, ...] | list[MessageId]
    part_ids: tuple[MessagePartId, ...] | list[MessagePartId]
    instruction_digests: Mapping[str, str]
    tool_schema_digest: str
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        if self.input_limit < 1 or self.output_reserve < 0:
            raise ValueError("Context limits must be non-negative")
        if not 0 < self.threshold <= 1:
            raise ValueError("threshold must be within (0, 1]")
        object.__setattr__(self, "message_ids", tuple(self.message_ids))
        object.__setattr__(self, "part_ids", tuple(self.part_ids))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(
            self,
            "instruction_digests",
            MappingProxyType(dict(self.instruction_digests)),
        )
