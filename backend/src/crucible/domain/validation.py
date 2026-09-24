from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from crucible.domain.clock import require_utc
from crucible.domain.ids import (
    ApprovalId,
    ArtifactId,
    RunId,
    ToolCallId,
    ValidationAttemptId,
    ValidationCommandResultId,
)


class ValidationStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    DENIED = "denied"
    CANCELLED = "cancelled"
    NOT_CONFIGURED = "not_configured"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class CompletionProposal:
    summary: str
    claimed_files: tuple[str, ...] = ()
    notes: str | None = None


@dataclass(frozen=True)
class ValidationAttempt:
    id: ValidationAttemptId
    run_id: RunId
    attempt_number: int
    status: ValidationStatus
    created_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
        if self.attempt_number <= 0:
            raise ValueError("Validation attempt number must be positive")


@dataclass(frozen=True)
class ValidationCommandResult:
    id: ValidationCommandResultId
    validation_attempt_id: ValidationAttemptId
    command_sequence: int
    status: ValidationStatus
    approval_id: ApprovalId | None
    tool_call_id: ToolCallId | None
    artifact_id: ArtifactId | None
    exit_code: int | None
    summary: str
    created_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
        if self.command_sequence <= 0:
            raise ValueError("Validation command sequence must be positive")
