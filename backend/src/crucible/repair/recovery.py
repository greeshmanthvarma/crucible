"""Archive and reverse only verified, controller-owned editable file changes."""

import base64
import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import UnitOfWork
from crucible.domain.repair import RepairRun
from crucible.repair.admission import AdmissionProcess
from crucible.repair.controller import RepairNeedsReview, RepairSourceDrift
from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.identity import (
    RuntimeIdentity,
    canonical_json,
    capture_runtime_identity,
)
from crucible.repair.roles import EDITABLE_PATHS
from crucible.repair.trials import private_artifact
from crucible.tools.workspace_authority import WorkspaceAuthority


class ManagedRepairWorkspace:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        artifacts: ArtifactService,
        *,
        main_root: Path | None = None,
        runtime_identity: RuntimeIdentity | None = None,
    ) -> None:
        self.factory, self.artifacts, self.main_root, self.runtime = (
            unit_of_work,
            artifacts,
            main_root,
            runtime_identity,
        )
        self.process = AdmissionProcess()

    async def _git(self, repair: RepairRun, *args: str) -> str:
        if repair.worktree_path is None:
            raise RepairNeedsReview("Worktree is missing")
        return await self.process.run(Path(repair.worktree_path), "git", *args)

    async def verify(self, repair: RepairRun) -> None:
        if self.main_root is not None and self.runtime is not None:
            if await self.process.run(
                self.main_root, "git", "status", "--porcelain", "--untracked-files=all"
            ):
                raise RepairSourceDrift("Main checkout changed")
            if await capture_runtime_identity(
                self.main_root
            ) != self.runtime or repair.initial_identity_json != asdict(self.runtime):
                raise RepairSourceDrift("Running harness source changed")
        if await self._git(repair, "rev-parse", "HEAD") != repair.last_accepted_commit:
            raise RepairNeedsReview("Accepted parent changed")
        if await self._git(repair, "symbolic-ref", "--short", "HEAD") != repair.branch:
            raise RepairNeedsReview("Repair branch changed")

    async def prepare(self, repair: RepairRun) -> None:
        await self.verify(repair)
        if await self._git(repair, "status", "--porcelain", "--untracked-files=all"):
            raise RepairNeedsReview("Unexpected initial changes")
        # Worker commands mount this worktree, never the checkout's .git target.
        # Keep held-out definitions and protected test fixtures physically absent.
        await self._git(
            repair,
            "sparse-checkout",
            "set",
            "--no-cone",
            "/*",
            "!**/evals/",
            "!**/tests/",
        )

    async def _owned_digest(self, repair: RepairRun) -> str | None:
        async with self.factory() as uow:
            attempts = [
                a
                for a in await uow.repairs.list_attempts(repair.id)
                if a.role == "proposal" and a.fix_id == repair.current_fix_id
            ]
            checkpoints = await uow.repairs.list_checkpoints(repair.id)
        if not attempts:
            return None
        for checkpoint in reversed(checkpoints):
            mutation = checkpoint.result_refs_json.get("owned_mutation")
            if isinstance(mutation, dict) and mutation.get("attempt_id") == str(
                attempts[-1].id
            ):
                return str(mutation["source_digest"])
        return None

    async def snapshot(
        self, repair: RepairRun, declared_paths: tuple[str, ...] | None = None
    ) -> dict[str, Any]:
        await self.verify(repair)
        root = Path(repair.worktree_path or "")
        changed = set(
            (await self._git(repair, "diff", "--name-only", "-z", "HEAD"))
            .strip("\0")
            .split("\0")
        ) | set(
            (
                await self._git(
                    repair, "ls-files", "--others", "--exclude-standard", "-z"
                )
            )
            .strip("\0")
            .split("\0")
        )
        changed.discard("")
        if not changed <= EDITABLE_PATHS:
            raise RepairNeedsReview("Protected or unknown files changed")
        if await self._git(repair, "diff", "--cached", "--name-only"):
            raise RepairNeedsReview("Unexpected staged edits")
        observed_digest = await self._owned_digest(repair)
        source = await capture_source_identity(root)
        if changed and observed_digest != source.tree_digest:
            raise RepairNeedsReview(
                "Candidate differs from controller-observed tool output"
            )
        admitted = (
            set(declared_paths) if declared_paths is not None else set(EDITABLE_PATHS)
        )
        if not admitted <= EDITABLE_PATHS or not changed <= admitted:
            raise RepairNeedsReview("Unowned file changes")
        source = await capture_source_identity(root)
        files = []
        for name in sorted(changed):
            path = root / name
            if path.is_symlink() or any(
                p.is_symlink()
                for p in path.parents
                if p.is_relative_to(root) and p != root
            ):
                raise RepairNeedsReview("Symlink in candidate")
            # The initial surface contains only existing tracked text modules.
            after = path.read_text() if path.exists() else None
            if after is not None and len(after.encode()) > 1024 * 1024:
                raise RepairNeedsReview("Candidate is oversized")
            # Read Git blob bytes without strip/newline loss.
            import asyncio

            process = await asyncio.create_subprocess_exec(
                "git",
                "show",
                f"{repair.last_accepted_commit}:{name}",
                cwd=root,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            before_raw, _ = await process.communicate()
            tree_entry = await self._git(
                repair, "ls-tree", repair.last_accepted_commit, "--", name
            )
            if process.returncode and tree_entry:
                raise RepairNeedsReview("Candidate baseline is unavailable")
            files.append(
                {
                    "path": name,
                    "before": base64.b64encode(before_raw).decode()
                    if tree_entry
                    else None,
                    "after": after,
                    "mode": (int(tree_entry.split()[0], 8) & 0o777)
                    if tree_entry
                    else None,
                }
            )
        artifact_id = await private_artifact(
            self.artifacts,
            repair.source_task_id,
            canonical_json(
                {
                    "repair_id": str(repair.id),
                    "parent": repair.last_accepted_commit,
                    "source_digest": source.tree_digest,
                    "files": files,
                }
            ),
        )
        return {
            "artifact_id": str(artifact_id),
            "source_digest": source.tree_digest,
            "parent": repair.last_accepted_commit,
            "changed_paths": sorted(changed),
        }

    async def restore(self, repair: RepairRun, snapshot: dict[str, Any]) -> None:
        await self.verify(repair)
        metadata, raw = await self.artifacts.read(UUID(snapshot["artifact_id"]))
        if (
            metadata.task_id != repair.source_task_id
            or metadata.sensitivity != "private"
            or hashlib.sha256(raw).hexdigest() != metadata.content_hash
        ):
            raise RepairNeedsReview("Patch archive integrity failure")
        saved = json.loads(raw)
        if (
            saved["repair_id"] != str(repair.id)
            or saved["parent"] != repair.last_accepted_commit
            or saved["source_digest"] != snapshot["source_digest"]
        ):
            raise RepairNeedsReview("Patch archive scope changed")
        root = Path(repair.worktree_path or "")
        if (await capture_source_identity(root)).tree_digest != saved["source_digest"]:
            raise RepairNeedsReview("Worktree changed after patch archival")
        paths = frozenset(item["path"] for item in saved["files"])
        if not paths <= EDITABLE_PATHS:
            raise RepairNeedsReview("Archive contains protected paths")
        authority = WorkspaceAuthority(root, paths, 1024 * 1024)
        for item in saved["files"]:
            path = authority.validate_target(item["path"])
            current = path.read_text() if path.exists() else None
            if current != item["after"]:
                raise RepairNeedsReview("Archived candidate differs")
        for item in saved["files"]:
            # Precisely restore known targets; no reset/clean or whole-tree writes.
            path = authority.validate_target(item["path"])
            if item["before"] is None:
                path.unlink(missing_ok=True)
            else:
                content = base64.b64decode(item["before"], validate=True)
                authority.write_text(item["path"], content.decode("utf-8"))
                path.chmod(item["mode"])
        if await self._git(repair, "status", "--porcelain", "--untracked-files=all"):
            raise RepairNeedsReview("Restoration found unrelated changes")
