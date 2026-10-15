"""Host-only repair effects. Role models never choose authority or credentials."""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from crucible.domain.repair import RepairAcceptance
from crucible.repair.identity import RuntimeIdentity


@dataclass(frozen=True)
class AdmissionCheck:
    ok: bool
    code: str | None
    detail: str
    retryable: bool = False


@dataclass(frozen=True)
class WorktreeIdentity:
    path: str
    branch: str


@dataclass(frozen=True)
class FrozenSource:
    path: Path
    digest: str


@dataclass(frozen=True)
class PRIdentity:
    number: int
    url: str
    state: str
    head_sha: str


class RepairAdmissionGitPort(Protocol):
    async def main_identity(self) -> RuntimeIdentity: ...
    async def check_fresh_main(self) -> AdmissionCheck: ...
    async def create_worktree(
        self, repair_id: UUID, baseline_commit: str
    ) -> WorktreeIdentity: ...


class RepairGitPort(RepairAdmissionGitPort, Protocol):
    async def snapshot(self, path: Path) -> FrozenSource: ...
    async def archive_unaccepted(self, path: Path, expected_parent: str) -> UUID: ...
    async def restore_owned_changes(
        self, path: Path, patch_artifact_id: UUID, expected_parent: str
    ) -> None: ...
    async def accept(self, intent: RepairAcceptance) -> str: ...
    async def reconcile_acceptance(self, intent: RepairAcceptance) -> str | None: ...


class RepairAdmissionGitHubPort(Protocol):
    async def list_open_repairs(self) -> tuple[PRIdentity, ...]: ...


class RepairGitHubPort(RepairAdmissionGitHubPort, Protocol):
    async def push_branch(self, branch: str, head_sha: str) -> None: ...
    async def find_pr(self, head: str, base: str) -> PRIdentity | None: ...
    async def create_pr(
        self, head: str, base: str, title: str, body: str
    ) -> PRIdentity: ...
    async def ensure_label(self, pr_number: int, label: str) -> None: ...


class RepairScheduler(Protocol):
    async def submit(self, repair_id: UUID) -> None: ...
    async def stop(self, repair_id: UUID) -> None: ...


class RepairWorkerRecovery(Protocol):
    async def stop_jobs(self, job_ids: tuple[UUID, ...]) -> None: ...
