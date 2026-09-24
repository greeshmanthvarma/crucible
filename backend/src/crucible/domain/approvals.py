from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from crucible.domain.clock import require_utc
from crucible.domain.commands import CommandSpec
from crucible.domain.ids import ApprovalId, RunId, StepId, TaskId, ToolCallId


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    CANCELLED = "cancelled"
    INVALIDATED = "invalidated"


class InvalidApprovalTransition(ValueError):
    pass


@dataclass(frozen=True)
class Approval:
    id: ApprovalId
    task_id: TaskId
    run_id: RunId
    step_id: StepId
    tool_call_id: ToolCallId
    spec: CommandSpec
    spec_digest: str
    status: ApprovalStatus
    decision_reason: str | None
    decided_by: str | None
    created_at: datetime
    decided_at: datetime | None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.decided_at)
        if self.spec_digest != self.spec.digest:
            raise ValueError("Approval digest does not match its Command specification")

    @classmethod
    def requested(
        cls,
        approval_id: ApprovalId,
        task_id: TaskId,
        run_id: RunId,
        step_id: StepId,
        tool_call_id: ToolCallId,
        spec: CommandSpec,
        now: datetime,
    ) -> "Approval":
        return cls(
            approval_id,
            task_id,
            run_id,
            step_id,
            tool_call_id,
            spec,
            spec.digest,
            ApprovalStatus.PENDING,
            None,
            None,
            now,
            None,
        )

    def approve(self, decided_by: str, now: datetime) -> "Approval":
        return self._decide(ApprovalStatus.APPROVED, decided_by, None, now)

    def deny(self, decided_by: str, reason: str | None, now: datetime) -> "Approval":
        return self._decide(ApprovalStatus.DENIED, decided_by, reason, now)

    def cancel(self, reason: str, now: datetime) -> "Approval":
        return self._decide(ApprovalStatus.CANCELLED, "harness", reason, now)

    def invalidate(self, reason: str, now: datetime) -> "Approval":
        return self._decide(ApprovalStatus.INVALIDATED, "harness", reason, now)

    def _decide(
        self,
        status: ApprovalStatus,
        decided_by: str,
        reason: str | None,
        now: datetime,
    ) -> "Approval":
        if self.status is not ApprovalStatus.PENDING:
            raise InvalidApprovalTransition(
                f"Cannot decide Approval in state {self.status}"
            )
        return replace(
            self,
            status=status,
            decided_by=decided_by,
            decision_reason=reason,
            decided_at=now,
        )
