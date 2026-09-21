from pathlib import Path
from uuid import UUID

import pytest

from crucible.application.errors import (
    WorkspaceDestinationExists,
    WorkspaceRevisionMismatch,
)
from crucible.domain.ids import new_id
from crucible.workspaces.git import ResolvedRepository
from crucible.workspaces.manager import WorkspaceManager


class FakeGitClient:
    def __init__(self, revision: str, verified_revision: str | None = None) -> None:
        self.revision = revision
        self.verified_revision = verified_revision or revision
        self.calls: list[tuple[object, ...]] = []

    async def resolve_repository(self, candidate: Path) -> ResolvedRepository:
        raise AssertionError("not used")

    async def resolve_revision(self, root: Path, source_ref: str) -> str:
        self.calls.append(("resolve", root, source_ref))
        return self.revision

    async def add_worktree(self, root: Path, destination: Path, revision: str) -> None:
        self.calls.append(("add", root, destination, revision))

    async def worktree_revision(self, workspace: Path) -> str:
        self.calls.append(("verify", workspace))
        return self.verified_revision


async def test_provisions_derived_worktree_at_resolved_revision(tmp_path: Path) -> None:
    git = FakeGitClient("a" * 40)
    task_id = new_id()
    manager = WorkspaceManager(git, tmp_path / "data")

    provisioned = await manager.provision(Path("/repo"), "main", task_id)

    destination = tmp_path / "data" / "workspaces" / str(task_id)
    assert provisioned.base_revision == "a" * 40
    assert provisioned.workspace_path == destination
    assert git.calls == [
        ("resolve", Path("/repo"), "main"),
        ("add", Path("/repo"), destination, "a" * 40),
        ("verify", destination),
    ]


async def test_rejects_revision_mismatch(tmp_path: Path) -> None:
    manager = WorkspaceManager(FakeGitClient("a" * 40, "b" * 40), tmp_path / "data")

    with pytest.raises(WorkspaceRevisionMismatch):
        await manager.provision(Path("/repo"), "main", new_id())


async def test_refuses_preexisting_destination(tmp_path: Path) -> None:
    task_id: UUID = new_id()
    destination = tmp_path / "data" / "workspaces" / str(task_id)
    destination.mkdir(parents=True)
    manager = WorkspaceManager(FakeGitClient("a" * 40), tmp_path / "data")

    with pytest.raises(WorkspaceDestinationExists):
        await manager.provision(Path("/repo"), "main", task_id)


async def test_rechecks_destination_after_planning(tmp_path: Path) -> None:
    task_id = new_id()
    manager = WorkspaceManager(FakeGitClient("a" * 40), tmp_path / "data")
    plan = await manager.plan(Path("/repo"), "main", task_id)
    plan.workspace_path.mkdir(parents=True)

    with pytest.raises(WorkspaceDestinationExists):
        await manager.create(plan)
