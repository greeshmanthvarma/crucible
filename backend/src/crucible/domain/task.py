from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Self

from crucible.domain.clock import Clock, require_utc
from crucible.domain.errors import InvalidTransition
from crucible.domain.ids import RepositoryId, TaskId


class TaskStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    ACCEPTED = "accepted"
    PROVISIONING_FAILED = "provisioning_failed"


@dataclass(frozen=True)
class Task:
    id: TaskId
    repository_id: RepositoryId
    source_ref: str
    base_revision: str | None
    workspace_path: Path
    status: TaskStatus
    failure_code: str | None
    failure_detail: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.updated_at)
        if self.base_revision is None and not (
            self.status is TaskStatus.PROVISIONING_FAILED
            and self.failure_code == "revision_not_found"
        ):
            raise ValueError("Only a revision_not_found Task may lack a Base Revision")

    @classmethod
    def provisioning(
        cls,
        *,
        task_id: TaskId,
        repository_id: RepositoryId,
        source_ref: str,
        base_revision: str,
        workspace_path: Path,
        clock: Clock,
    ) -> Self:
        now = clock.now()
        return cls(
            id=task_id,
            repository_id=repository_id,
            source_ref=source_ref,
            base_revision=base_revision,
            workspace_path=workspace_path,
            status=TaskStatus.PROVISIONING,
            failure_code=None,
            failure_detail=None,
            created_at=now,
            updated_at=now,
        )

    @classmethod
    def failed_without_revision(
        cls,
        *,
        task_id: TaskId,
        repository_id: RepositoryId,
        source_ref: str,
        workspace_path: Path,
        detail: str,
        clock: Clock,
    ) -> Self:
        now = clock.now()
        return cls(
            id=task_id,
            repository_id=repository_id,
            source_ref=source_ref,
            base_revision=None,
            workspace_path=workspace_path,
            status=TaskStatus.PROVISIONING_FAILED,
            failure_code="revision_not_found",
            failure_detail=detail,
            created_at=now,
            updated_at=now,
        )

    def activate(self, *, workspace_path: Path, clock: Clock) -> Self:
        self._require_provisioning(TaskStatus.ACTIVE)
        return replace(
            self,
            workspace_path=workspace_path,
            status=TaskStatus.ACTIVE,
            updated_at=clock.now(),
        )

    def fail_provisioning(self, code: str, detail: str, clock: Clock) -> Self:
        self._require_provisioning(TaskStatus.PROVISIONING_FAILED)
        return replace(
            self,
            status=TaskStatus.PROVISIONING_FAILED,
            failure_code=code,
            failure_detail=detail,
            updated_at=clock.now(),
        )

    def accept(self, clock: Clock) -> Self:
        if self.status is not TaskStatus.ACTIVE:
            raise InvalidTransition(f"cannot accept Task from {self.status}")
        return replace(self, status=TaskStatus.ACCEPTED, updated_at=clock.now())

    def reopen(self, clock: Clock) -> Self:
        if self.status is not TaskStatus.ACCEPTED:
            raise InvalidTransition(f"cannot reopen Task from {self.status}")
        return replace(self, status=TaskStatus.ACTIVE, updated_at=clock.now())

    def _require_provisioning(self, target: TaskStatus) -> None:
        if self.status is not TaskStatus.PROVISIONING:
            raise InvalidTransition(
                f"cannot transition Task from {self.status} to {target}"
            )
