from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Self

from crucible.domain.clock import require_utc
from crucible.domain.errors import InvalidTransition
from crucible.domain.ids import ExecutionId, MessageId, RunId, TaskId


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class Run:
    id: RunId
    task_id: TaskId
    triggering_message_id: MessageId | None
    status: RunStatus
    execution_id: ExecutionId | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    outcome_code: str | None
    outcome_detail: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cancel_requested_at: datetime | None = None
    cancel_code: str | None = None

    def __post_init__(self) -> None:
        require_utc(
            self.lease_expires_at,
            self.heartbeat_at,
            self.created_at,
            self.started_at,
            self.completed_at,
            self.cancel_requested_at,
        )

    @classmethod
    def queued(
        cls,
        *,
        run_id: RunId,
        task_id: TaskId,
        triggering_message_id: MessageId | None,
        created_at: datetime,
    ) -> Self:
        return cls(
            id=run_id,
            task_id=task_id,
            triggering_message_id=triggering_message_id,
            status=RunStatus.QUEUED,
            execution_id=None,
            lease_expires_at=None,
            heartbeat_at=None,
            outcome_code=None,
            outcome_detail=None,
            created_at=created_at,
            started_at=None,
            completed_at=None,
        )

    def claim(
        self,
        *,
        execution_id: ExecutionId,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Self:
        self._require_status(RunStatus.QUEUED, RunStatus.RUNNING)
        return replace(
            self,
            status=RunStatus.RUNNING,
            execution_id=execution_id,
            lease_expires_at=lease_expires_at,
            heartbeat_at=now,
            started_at=now,
        )

    def complete(self, *, now: datetime) -> Self:
        self._require_status(RunStatus.RUNNING, RunStatus.COMPLETED)
        return replace(
            self,
            status=RunStatus.COMPLETED,
            lease_expires_at=None,
            completed_at=now,
        )

    def fail(self, code: str, detail: str, *, now: datetime) -> Self:
        self._require_status(RunStatus.RUNNING, RunStatus.FAILED)
        return replace(
            self,
            status=RunStatus.FAILED,
            lease_expires_at=None,
            outcome_code=code,
            outcome_detail=detail,
            completed_at=now,
        )

    def interrupt(self, code: str, detail: str, *, now: datetime) -> Self:
        self._require_status(RunStatus.RUNNING, RunStatus.INTERRUPTED)
        return replace(
            self,
            status=RunStatus.INTERRUPTED,
            lease_expires_at=None,
            outcome_code=code,
            outcome_detail=detail,
            completed_at=now,
        )

    def request_cancel(self, code: str, now: datetime) -> Self:
        if self.status not in (RunStatus.QUEUED, RunStatus.RUNNING):
            return self
        if self.cancel_requested_at is not None:
            return self
        return replace(self, cancel_requested_at=now, cancel_code=code)

    def cancel(self, code: str, detail: str, *, now: datetime) -> Self:
        if self.status not in (RunStatus.QUEUED, RunStatus.RUNNING):
            return self
        return replace(
            self,
            status=RunStatus.INTERRUPTED,
            lease_expires_at=None,
            outcome_code=code,
            outcome_detail=detail,
            cancel_requested_at=self.cancel_requested_at or now,
            cancel_code=self.cancel_code or code,
            completed_at=now,
        )

    def _require_status(self, expected: RunStatus, target: RunStatus) -> None:
        if self.status is not expected:
            raise InvalidTransition(
                f"cannot transition Run from {self.status} to {target}"
            )
