"""Durable identities and legal lifecycle transitions for behavioral Trials."""

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from crucible.domain.clock import require_utc


class TrialStatus(StrEnum):
    QUEUED = "queued"
    PREPARING = "preparing"
    RUNNING = "running"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class ResultVerdict(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"


class UsageSource(StrEnum):
    REPORTED = "reported"
    ESTIMATED = "estimated"


@dataclass(frozen=True)
class EvalSuite:
    id: UUID
    partition: str
    name: str
    definition_digest: str
    definition: dict[str, object]
    created_at: datetime


@dataclass(frozen=True)
class EvalCase:
    id: UUID
    suite_id: UUID
    name: str
    definition_digest: str
    definition: dict[str, object]
    created_at: datetime


@dataclass(frozen=True)
class EvalTrial:
    id: UUID
    suite_id: UUID
    case_id: UUID
    invocation_id: UUID
    repeat_index: int
    partition: str
    case_digest: str
    configuration_digest: str
    status: TrialStatus
    created_at: datetime
    updated_at: datetime
    fixture_commit: str | None = None
    repository_id: UUID | None = None
    task_id: UUID | None = None
    run_id: UUID | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.updated_at)
        if self.repeat_index < 1:
            raise ValueError("repeat index must be positive")

    @classmethod
    def queued(
        cls,
        id: UUID,
        suite_id: UUID,
        case_id: UUID,
        invocation_id: UUID,
        repeat_index: int,
        partition: str,
        case_digest: str,
        configuration_digest: str,
        now: datetime,
    ) -> Self:
        return cls(
            id,
            suite_id,
            case_id,
            invocation_id,
            repeat_index,
            partition,
            case_digest,
            configuration_digest,
            TrialStatus.QUEUED,
            now,
            now,
        )

    def _advance(
        self, expected: TrialStatus, target: TrialStatus, now: datetime
    ) -> Self:
        if self.status in (
            TrialStatus.COMPLETED,
            TrialStatus.FAILED,
            TrialStatus.INTERRUPTED,
        ):
            raise ValueError("terminal Trial cannot be rewritten")
        if self.status is not expected:
            raise ValueError(f"cannot advance Trial from {self.status} to {target}")
        return replace(self, status=target, updated_at=now)

    def prepare(self, now: datetime) -> Self:
        return self._advance(TrialStatus.QUEUED, TrialStatus.PREPARING, now)

    def run(
        self,
        repository_id: UUID,
        task_id: UUID,
        run_id: UUID,
        fixture_commit: str,
        now: datetime,
    ) -> Self:
        trial = self._advance(TrialStatus.PREPARING, TrialStatus.RUNNING, now)
        return replace(
            trial,
            repository_id=repository_id,
            task_id=task_id,
            run_id=run_id,
            fixture_commit=fixture_commit,
        )

    def evaluate(self, now: datetime) -> Self:
        return self._advance(TrialStatus.RUNNING, TrialStatus.EVALUATING, now)

    def complete(self, now: datetime) -> Self:
        return self._advance(TrialStatus.EVALUATING, TrialStatus.COMPLETED, now)

    def fail(self, code: str, now: datetime) -> Self:
        if self.status in (
            TrialStatus.COMPLETED,
            TrialStatus.FAILED,
            TrialStatus.INTERRUPTED,
        ):
            raise ValueError("terminal Trial cannot be rewritten")
        return replace(
            self, status=TrialStatus.FAILED, failure_code=code, updated_at=now
        )

    def interrupt(self, now: datetime) -> Self:
        if self.status in (
            TrialStatus.COMPLETED,
            TrialStatus.FAILED,
            TrialStatus.INTERRUPTED,
        ):
            raise ValueError("terminal Trial cannot be rewritten")
        return replace(
            self,
            status=TrialStatus.INTERRUPTED,
            failure_code="interrupted",
            updated_at=now,
        )


@dataclass(frozen=True)
class EvalResult:
    trial_id: UUID
    verdict: ResultVerdict
    evaluator_results: tuple[dict[str, object], ...]
    report_artifact_id: UUID | None
    created_at: datetime


@dataclass(frozen=True)
class StepUsage:
    step_id: UUID
    run_id: UUID
    model_id: str
    input_tokens: int
    output_tokens: int
    source: UsageSource
    created_at: datetime


@dataclass(frozen=True)
class RunSummary:
    run_id: UUID
    schema_version: int
    projection: dict[str, object]
    created_at: datetime
