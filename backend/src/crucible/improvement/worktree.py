"""Owned worktree intent and checkpoints; no resets of unknown user edits."""

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from crucible.improvement.contracts import EDITABLE_PATHS
from crucible.improvement.source import (
    ScopeRejected,
    SourceInspection,
    git_bytes,
    inspect_source,
    safe_path,
    stage_harness,
)
from crucible.repair.identity import canonical_json


@dataclass(frozen=True)
class OwnedWorktree:
    path: Path
    initial_commit: str
    branch: str
    root: Path

    async def inspect(self) -> SourceInspection:
        branch = (
            (
                await asyncio.to_thread(
                    git_bytes, self.path, "symbolic-ref", "--short", "HEAD"
                )
            )
            .decode()
            .strip()
        )
        parent = (
            (await asyncio.to_thread(git_bytes, self.path, "rev-parse", "HEAD"))
            .decode()
            .strip()
        )
        if branch != self.branch or parent != self.initial_commit:
            raise ScopeRejected("worktree ownership drift")
        return await asyncio.to_thread(inspect_source, self.path, self.initial_commit)

    async def reconcile_proposal_transfer(self) -> None:
        """Explicit recovery recognizes only journaled partial controller writes."""
        import hashlib

        from crucible.benchmarks.evidence import atomic_json
        from crucible.improvement.worker_supervisor import read_regular_artifact

        path = self.root / "proposal-transfer.json"
        if not path.exists():
            return
        journal = json.loads(read_regular_artifact(path, 2 * 1024 * 1024))
        if journal["state"] != "intent":
            return
        inspection = await self.inspect()
        before, after = journal["before"], journal["after"]
        if not set(after) <= set(EDITABLE_PATHS):
            raise ScopeRejected("invalid proposal transfer journal")
        current = {
            name: [mode, hashlib.sha256(raw).hexdigest()]
            for name, (mode, raw) in inspection.files.items()
        }
        for name in set(before) | set(current) | set(after):
            allowed = [before.get(name)]
            if name in after:
                allowed.append(after[name])
            if current.get(name) not in allowed:
                raise ScopeRejected("unexpected edit during proposal transfer", (name,))
        await self.checkpoint_owned_edits()
        atomic_json(path, {**journal, "state": "reconciled"})

    async def checkpoint_owned_edits(self) -> str:
        inspection = await self.inspect()
        # Only called after controller-directed proposal/checkpoint, never on resume.
        (self.root / "owned.json").write_bytes(
            canonical_json({"source_digest": inspection.source_digest})
        )
        return inspection.source_digest

    async def archive_interrupted(self, snapshot_id: UUID) -> Path:
        inspection = await self.inspect()
        checkpoint = json.loads((self.root / "owned.json").read_bytes())
        if inspection.source_digest != checkpoint["source_digest"]:
            raise ScopeRejected("unexpected user edits require recovery")
        output = self.root / "interrupted" / str(snapshot_id)
        await asyncio.to_thread(
            stage_harness, self.path, self.initial_commit, output, EDITABLE_PATHS
        )
        return output

    async def restore_last_accepted(self, expected_source_digest: str) -> None:
        inspection = await self.inspect()
        checkpoint = json.loads((self.root / "owned.json").read_bytes())
        if inspection.source_digest != checkpoint["source_digest"]:
            raise ScopeRejected("unexpected user edits require recovery")
        if inspection.source_digest == expected_source_digest:
            return
        # Caller must first archive this exact owned checkpoint.
        archives = list((self.root / "interrupted").glob("*/snapshot.json"))
        if not any(
            json.loads(p.read_bytes())["source_digest"] == inspection.source_digest
            for p in archives
        ):
            raise ScopeRejected("owned candidate must be archived before restoration")
        for name in inspection.changed_paths:
            if name not in EDITABLE_PATHS:
                raise ScopeRejected("protected edit on restore", (name,))
        from crucible.improvement.source import _digest

        if _digest(inspection.base_files) != expected_source_digest:
            raise ScopeRejected("expected baseline digest mismatch before restore")
        for name in inspection.changed_paths:
            path = self.path / name
            if name not in inspection.base_files:
                path.unlink(missing_ok=True)
                continue
            mode, data = inspection.base_files[name]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o755 if mode == 0o100755 else 0o644)
        restored = await self.inspect()
        if restored.source_digest != expected_source_digest:
            raise ScopeRejected("restored baseline digest mismatch")
        await self.checkpoint_owned_edits()


class ExperimentWorktree:
    def __init__(self, root: Path) -> None:
        safe_path(root)
        self.root = root

    async def create(
        self, repository: Path, experiment_id: UUID, initial_commit: str
    ) -> OwnedWorktree:
        safe_path(repository)
        root = self.root / "ownership" / str(experiment_id)
        path = self.root / "worktrees" / str(experiment_id)
        branch = "codex/improve/" + str(experiment_id)
        intent = {
            "repository": str(repository),
            "path": str(path),
            "branch": branch,
            "parent": initial_commit,
        }
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        intent_path = root / "intent.json"
        if intent_path.exists():
            if json.loads(intent_path.read_bytes()) != intent:
                raise ScopeRejected("worktree intent drift")
        else:
            with intent_path.open("xb") as target:
                target.write(canonical_json(intent))
        owned = OwnedWorktree(path, initial_commit, branch, root)
        if not path.exists():
            await asyncio.to_thread(
                git_bytes,
                repository,
                "worktree",
                "add",
                "-b",
                branch,
                str(path),
                initial_commit,
            )
        inspection = await owned.inspect()
        checkpoint = root / "owned.json"
        if not checkpoint.exists():
            if inspection.changed_paths:
                raise ScopeRejected("unexpected edits before ownership checkpoint")
            await owned.checkpoint_owned_edits()
        return owned
