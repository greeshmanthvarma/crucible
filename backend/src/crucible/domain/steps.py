from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Self

from crucible.domain.clock import require_utc
from crucible.domain.ids import RunId, StepId, TaskId


class StepStatus(StrEnum):
    PREPARING = "preparing"
    MODEL_ACTIVE = "model_active"
    TOOLS_ACTIVE = "tools_active"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class Step:
    id: StepId
    task_id: TaskId
    run_id: RunId
    step_sequence: int
    status: StepStatus
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.started_at, self.completed_at)
        if self.step_sequence < 1:
            raise ValueError("step_sequence must be positive")

    @classmethod
    def preparing(
        cls,
        step_id: StepId,
        task_id: TaskId,
        run_id: RunId,
        step_sequence: int,
        now: datetime,
    ) -> Self:
        return cls(
            step_id,
            task_id,
            run_id,
            step_sequence,
            StepStatus.PREPARING,
            now,
        )

    def activate_model(self, now: datetime) -> Self:
        self._require(StepStatus.PREPARING, StepStatus.MODEL_ACTIVE)
        return replace(self, status=StepStatus.MODEL_ACTIVE, started_at=now)

    def activate_tools(self, now: datetime) -> Self:
        self._require(StepStatus.MODEL_ACTIVE, StepStatus.TOOLS_ACTIVE)
        return replace(self, status=StepStatus.TOOLS_ACTIVE)

    def complete(self, now: datetime) -> Self:
        if self.status not in (StepStatus.MODEL_ACTIVE, StepStatus.TOOLS_ACTIVE):
            raise ValueError(
                f"cannot transition Step from {self.status} to {StepStatus.COMPLETED}"
            )
        return replace(self, status=StepStatus.COMPLETED, completed_at=now)

    def _require(self, source: StepStatus, target: StepStatus) -> None:
        if self.status is not source:
            raise ValueError(f"cannot transition Step from {self.status} to {target}")
