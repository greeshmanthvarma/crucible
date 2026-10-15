"""Durable repair lifecycle, separate from ordinary coding Runs."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID


class RepairStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    STOPPED = "stopped"
    INTERRUPTED = "interrupted"
    RETRYABLE_ERROR = "retryable_error"
    COMPLETED = "completed"
    EXHAUSTED = "exhausted"
    CLOSED = "closed"
    INVALIDATED = "invalidated"


class RepairStage(StrEnum):
    INTAKE = "intake"
    DIAGNOSIS = "diagnosis"
    EVAL_REVIEW = "eval_review"
    BASELINE_REPRODUCTION = "baseline_reproduction"
    PROPOSAL = "proposal"
    CANDIDATE_VALIDATION = "candidate_validation"
    COMMIT_ACCEPTANCE = "commit_acceptance"
    PUBLICATION = "publication"


TERMINAL_STATUSES = frozenset(
    {
        RepairStatus.COMPLETED,
        RepairStatus.EXHAUSTED,
        RepairStatus.CLOSED,
        RepairStatus.INVALIDATED,
    }
)
RepairCommand = Literal["/repair", "/repair resume", "/repair close"]


@dataclass(frozen=True)
class RepairCommandResult:
    status: Literal["admitted", "resumed", "closed", "nothing_eligible", "blocked"]
    repair_id: UUID | None
    code: str | None
    detail: str
    pr_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class RepairRun:
    id: UUID
    source_task_id: UUID
    status: RepairStatus
    stage: RepairStage
    initial_identity_json: dict[str, object]
    branch: str | None
    worktree_path: str | None
    last_accepted_commit: str
    current_fix_id: UUID | None
    created_at: datetime
    updated_at: datetime
    outcome_code: str | None = None
    outcome_detail: str | None = None
    predecessor_repair_id: UUID | None = None


@dataclass(frozen=True)
class RepairOwnership:
    repair_id: UUID | None
    owner_execution_id: str | None
    heartbeat_at: datetime | None
    lease_expires_at: datetime | None


@dataclass(frozen=True)
class RepairAttempt:
    id: UUID
    repair_id: UUID
    fix_id: UUID | None
    role: str
    attempt_index: int
    harness_run_id: UUID | None
    status: str
    charged: bool
    started_at: datetime
    completed_at: datetime | None = None
    output_artifact_id: UUID | None = None


@dataclass(frozen=True)
class RepairCheckpoint:
    id: UUID
    repair_id: UUID
    stage: str
    fix_id: UUID | None
    trial_job_id: UUID | None
    input_fingerprint: str
    accepted_commit: str
    attempts_json: dict[str, object]
    result_refs_json: dict[str, object]
    created_at: datetime


@dataclass(frozen=True)
class RepairFix:
    id: UUID
    repair_id: UUID
    ordinal: int
    diagnosis_case_digest: str
    approved_case_digest: str | None
    status: str
    accepted_commit: str | None = None
    attempts_used: int = 0


@dataclass(frozen=True)
class RepairAcceptance:
    id: UUID
    repair_id: UUID
    fix_id: UUID
    expected_parent: str
    candidate_tree_digest: str
    evaluated_source_digest: str
    result_refs_json: dict[str, object]
    status: str
    commit_sha: str | None = None


@dataclass(frozen=True)
class RepairPublication:
    repair_id: UUID
    head_branch: str
    base_branch: str
    head_sha: str
    status: str
    pr_number: int | None = None
    pr_url: str | None = None
    last_error: str | None = None
