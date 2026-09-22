from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from crucible.application.errors import (
    WorkspaceDestinationExists,
    WorkspaceRevisionMismatch,
)
from crucible.workspaces.git import GitClient, GitResult


@dataclass(frozen=True)
class WorkspacePlan:
    repository_root: Path
    source_ref: str
    base_revision: str
    workspace_path: Path


@dataclass(frozen=True)
class ProvisionedWorkspace:
    base_revision: str
    workspace_path: Path


class WorkspaceManager:
    def __init__(self, git: GitClient, data_dir: Path) -> None:
        self._git = git
        self._data_dir = data_dir

    def destination_for(self, task_id: UUID) -> Path:
        return self._data_dir / "workspaces" / str(task_id)

    async def plan(
        self, repository_root: Path, source_ref: str, task_id: UUID
    ) -> WorkspacePlan:
        destination = self.destination_for(task_id)
        if destination.exists():
            raise WorkspaceDestinationExists(
                f"Workspace destination already exists: {destination}"
            )
        revision = await self._git.resolve_revision(repository_root, source_ref)
        return WorkspacePlan(
            repository_root=repository_root,
            source_ref=source_ref,
            base_revision=revision,
            workspace_path=destination,
        )

    async def create(self, plan: WorkspacePlan) -> ProvisionedWorkspace:
        if plan.workspace_path.exists():
            raise WorkspaceDestinationExists(
                f"Workspace destination already exists: {plan.workspace_path}"
            )
        plan.workspace_path.parent.mkdir(parents=True, exist_ok=True)
        await self._git.add_worktree(
            plan.repository_root, plan.workspace_path, plan.base_revision
        )
        actual_revision = await self._git.worktree_revision(plan.workspace_path)
        if actual_revision != plan.base_revision:
            raise WorkspaceRevisionMismatch(
                "Workspace revision does not match the recorded Base Revision"
            )
        return ProvisionedWorkspace(plan.base_revision, plan.workspace_path)

    async def provision(
        self, repository_root: Path, source_ref: str, task_id: UUID
    ) -> ProvisionedWorkspace:
        return await self.create(await self.plan(repository_root, source_ref, task_id))

    async def recover(self, plan: WorkspacePlan) -> ProvisionedWorkspace:
        if not plan.workspace_path.exists():
            return await self.create(plan)
        if not await self._git.owns_worktree(plan.repository_root, plan.workspace_path):
            raise WorkspaceDestinationExists(
                "Destination is not a worktree owned by the recorded Repository"
            )
        actual_revision = await self._git.worktree_revision(plan.workspace_path)
        if actual_revision != plan.base_revision:
            raise WorkspaceRevisionMismatch(
                "Existing workspace does not match the recorded Base Revision"
            )
        return ProvisionedWorkspace(plan.base_revision, plan.workspace_path)

    async def status(self, workspace: Path) -> GitResult:
        return await self._git.status(workspace)

    async def diff(self, workspace: Path) -> GitResult:
        return await self._git.diff(workspace)
