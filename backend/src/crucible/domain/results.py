from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, Self

from crucible.domain.clock import require_utc
from crucible.domain.errors import InvalidTransition
from crucible.domain.ids import (
    ArtifactId,
    IntegrationId,
    RepositoryId,
    ResultRevisionId,
    TaskId,
)


def _require_sha(value: str, label: str) -> None:
    if len(value) != 40 or any(
        character not in "0123456789abcdef" for character in value.lower()
    ):
        raise ValueError(f"{label} must be a full Git commit SHA")


@dataclass(frozen=True)
class ResultRevision:
    id: ResultRevisionId
    task_id: TaskId
    commit_sha: str
    parent_revision: str
    previous_result_revision_id: ResultRevisionId | None
    diff_artifact_id: ArtifactId
    validation_snapshot: dict[str, Any]
    summary: str
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
        _require_sha(self.commit_sha, "Result Revision commit SHA")
        _require_sha(self.parent_revision, "Result Revision parent")


class IntegrationStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CONFLICT = "conflict"
    RECOVERY_REQUIRED = "recovery_required"


@dataclass(frozen=True)
class Integration:
    id: IntegrationId
    result_revision_id: ResultRevisionId
    repository_id: RepositoryId
    target_ref: str
    expected_target_revision: str
    idempotency_key: str
    status: IntegrationStatus
    observed_before_revision: str | None
    observed_after_revision: str | None
    failure_code: str | None
    failure_detail: str | None
    created_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.completed_at)
        _require_sha(self.expected_target_revision, "Expected target revision")

    @classmethod
    def pending(
        cls,
        integration_id: IntegrationId,
        result_revision_id: ResultRevisionId,
        repository_id: RepositoryId,
        target_ref: str,
        expected_target_revision: str,
        idempotency_key: str,
        now: datetime,
    ) -> Self:
        return cls(
            integration_id,
            result_revision_id,
            repository_id,
            target_ref,
            expected_target_revision,
            idempotency_key,
            IntegrationStatus.PENDING,
            None,
            None,
            None,
            None,
            now,
            None,
        )

    def succeed(
        self, observed_before_revision: str, observed_after_revision: str, now: datetime
    ) -> Self:
        if self.status is IntegrationStatus.COMPLETED:
            return self
        if self.status is not IntegrationStatus.PENDING:
            raise InvalidTransition(f"cannot complete Integration from {self.status}")
        _require_sha(observed_before_revision, "Observed target revision")
        _require_sha(observed_after_revision, "Integrated target revision")
        return replace(
            self,
            status=IntegrationStatus.COMPLETED,
            observed_before_revision=observed_before_revision,
            observed_after_revision=observed_after_revision,
            completed_at=now,
        )
