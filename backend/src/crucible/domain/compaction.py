from dataclasses import dataclass
from datetime import datetime
from typing import Any

from crucible.domain.clock import require_utc
from crucible.domain.ids import ArtifactId, CompactionId, TaskId


@dataclass(frozen=True)
class Compaction:
    id: CompactionId
    task_id: TaskId
    source_start_sequence: int
    source_end_sequence: int
    retained_tail_start_sequence: int
    summary_artifact_id: ArtifactId
    rendered_summary: str
    previous_compaction_id: CompactionId | None
    model: str
    parameters: dict[str, Any]
    prompt_version: str
    input_tokens: int
    output_tokens: int
    resulting_context_estimate: int
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        if (
            self.source_start_sequence <= 0
            or self.source_end_sequence < self.source_start_sequence
        ):
            raise ValueError("Compaction source range must be positive and ordered")
        if self.retained_tail_start_sequence <= self.source_end_sequence:
            raise ValueError(
                "Compaction retained tail must begin after its source range"
            )
        if (
            min(self.input_tokens, self.output_tokens, self.resulting_context_estimate)
            < 0
        ):
            raise ValueError("Compaction usage and estimates cannot be negative")
