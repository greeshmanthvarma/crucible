"""Explicit-path Git effects and durable acceptance recovery on the host."""

import asyncio
import os
import tempfile
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from crucible.application.ports import UnitOfWork
from crucible.domain.repair import RepairAcceptance, RepairFix, RepairRun
from crucible.repair.admission import LocalRepairAdmissionGit
from crucible.repair.cases import ApprovedCase
from crucible.repair.controller import RepairNeedsReview
from crucible.repair.events import append_repair_event
from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.roles import EDITABLE_PATHS


class LocalRepairGit(LocalRepairAdmissionGit):
    async def _command(self, root: Path, *args: str, index: str | None = None) -> str:
        env = dict(os.environ)
        if index:
            env["GIT_INDEX_FILE"] = index
        process = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=root,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            raw, _ = await asyncio.wait_for(process.communicate(), 30)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        if process.returncode:
            raise RepairNeedsReview("Git acceptance command failed")
        return raw.decode().strip()

    async def changed_paths(self, root: Path) -> tuple[str, ...]:
        changed = set(
            (
                await self._command(
                    root, "diff", "--no-renames", "--name-only", "-z", "HEAD"
                )
            ).split("\0")
        )
        changed.update(
            (
                await self._command(
                    root, "ls-files", "--others", "--exclude-standard", "-z"
                )
            ).split("\0")
        )
        changed.discard("")
        # Ignored files are never staged; tracked protected changes still appear.
        if not changed <= EDITABLE_PATHS:
            raise RepairNeedsReview("Protected or unknown changes")
        for name in changed:
            path = root / name
            if path.is_symlink() or any(
                p.is_symlink() for p in path.parents if p.is_relative_to(root)
            ):
                raise RepairNeedsReview("Symlink in candidate")
        return tuple(sorted(changed))

    async def _stage(
        self, root: Path, paths: tuple[str, ...], index: str | None = None
    ) -> None:
        for name in paths:
            path = root / name
            if not path.exists():
                await self._command(
                    root, "update-index", "--force-remove", "--", name, index=index
                )
            else:
                blob = await self._command(
                    root, "hash-object", "-w", "--no-filters", "--", name
                )
                mode = "100755" if path.stat().st_mode & 0o111 else "100644"
                await self._command(
                    root,
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"{mode},{blob},{name}",
                    index=index,
                )

    async def candidate_tree(self, repair: RepairRun) -> str:
        root = Path(repair.worktree_path or "")
        paths = await self.changed_paths(root)
        # A disposable index calculates the full commit tree without changing the
        # live index or executing candidate code, filters, or repository hooks.
        with tempfile.TemporaryDirectory(prefix="repair-index-") as directory:
            index = str(Path(directory) / "index")
            await self._command(
                root, "read-tree", repair.last_accepted_commit, index=index
            )
            if paths:
                await self._stage(root, paths, index)
            return await self._command(root, "write-tree", index=index)

    @staticmethod
    def marker(intent: RepairAcceptance) -> str:
        return f"Crucible repair {intent.repair_id} fix {intent.fix_id}"

    async def reconcile_acceptance(self, intent: RepairAcceptance) -> str | None:
        root = Path(str(intent.result_refs_json["worktree_path"]))
        branch = await self._command(root, "symbolic-ref", "--short", "HEAD")
        if branch != intent.result_refs_json["branch"]:
            raise RepairNeedsReview("Repair branch changed")
        head = await self._command(root, "rev-parse", "HEAD")
        if head == intent.expected_parent:
            return None
        if (
            await self._command(root, "show", "-s", "--format=%P", head)
            != intent.expected_parent
            or await self._command(root, "show", "-s", "--format=%T", head)
            != intent.candidate_tree_digest
            or await self._command(root, "show", "-s", "--format=%B", head)
            != self.marker(intent)
            or await self._command(
                root, "status", "--porcelain", "--untracked-files=all"
            )
        ):
            raise RepairNeedsReview("Acceptance effect differs from saved intent")
        return head

    async def abandon(self, intent: RepairAcceptance) -> None:
        root = Path(str(intent.result_refs_json["worktree_path"]))
        if await self.reconcile_acceptance(intent) is not None:
            raise RepairNeedsReview("Committed acceptance cannot be abandoned")
        if (
            await capture_source_identity(root)
        ).tree_digest != intent.evaluated_source_digest:
            raise RepairNeedsReview("Candidate changed before interruption")
        paths = await self.changed_paths(root)
        staged = set(
            (await self._command(root, "diff", "--cached", "--name-only", "-z")).split(
                "\0"
            )
        ) - {""}
        if not staged <= set(paths):
            raise RepairNeedsReview("Unknown index changes")
        for name in staged:
            current = await self._command(root, "ls-files", "--stage", "--", name)
            entry = await self._command(
                root, "ls-tree", intent.candidate_tree_digest, "--", name
            )
            # Compare the staged blob/mode to the saved candidate, including a
            # staged deletion. No arbitrary user index state is overwritten.
            expected = entry.replace(" blob ", " ").replace("\t", " 0\t", 1)
            if current != expected:
                raise RepairNeedsReview("Index differs from owned candidate")
        if staged:
            await self._command(
                root, "reset", intent.expected_parent, "--", *sorted(staged)
            )

    async def accept(self, intent: RepairAcceptance) -> str:
        existing = await self.reconcile_acceptance(intent)
        if existing:
            return existing
        root = Path(str(intent.result_refs_json["worktree_path"]))
        paths = await self.changed_paths(root)
        if (
            not paths
            or (await capture_source_identity(root)).tree_digest
            != intent.evaluated_source_digest
        ):
            raise RepairNeedsReview("Candidate differs from evaluated source")
        # Reject unrelated index state even if the working file was restored.
        staged = set(
            (await self._command(root, "diff", "--cached", "--name-only", "-z")).split(
                "\0"
            )
        ) - {""}
        if not staged <= set(paths):
            raise RepairNeedsReview("Unexpected staged changes")
        await self._stage(root, paths)
        if await self._command(root, "write-tree") != intent.candidate_tree_digest:
            raise RepairNeedsReview("Staged tree differs from evaluated tree")
        if (
            await capture_source_identity(root)
        ).tree_digest != intent.evaluated_source_digest:
            raise RepairNeedsReview("Source changed before commit")
        # Recheck branch and parent after staging, before the commit effect.
        if await self.reconcile_acceptance(intent) is not None:
            raise RepairNeedsReview("Concurrent acceptance changed the branch")
        await self._command(
            root,
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--no-verify",
            "-m",
            self.marker(intent),
        )
        commit = await self.reconcile_acceptance(intent)
        if commit is None:
            raise RepairNeedsReview("Commit was not applied")
        return commit


class RepairAcceptanceService:
    """Save intent, commit, register private regression, then finalize atomically."""

    def __init__(
        self,
        factory: Callable[[], UnitOfWork],
        git: LocalRepairGit,
        artifacts: Any,
        clock: Any,
        incidents: Any,
    ) -> None:
        self.factory, self.git, self.artifacts, self.clock, self.incidents = (
            factory,
            git,
            artifacts,
            clock,
            incidents,
        )

    async def accept(
        self,
        repair: RepairRun,
        fix: RepairFix,
        case: ApprovedCase,
        snapshot: dict[str, Any],
        evidence_ids: tuple[UUID, ...],
    ) -> str:
        from crucible.repair.trials import private_artifact

        if (
            snapshot["parent"] != repair.last_accepted_commit
            or (
                await capture_source_identity(Path(repair.worktree_path or ""))
            ).tree_digest
            != snapshot["source_digest"]
        ):
            raise RepairNeedsReview("Evaluated snapshot changed")
        case_id = await private_artifact(
            self.artifacts, repair.source_task_id, case.model_dump_json().encode()
        )
        intent = RepairAcceptance(
            uuid4(),
            repair.id,
            fix.id,
            repair.last_accepted_commit,
            await self.git.candidate_tree(repair),
            snapshot["source_digest"],
            {
                "worktree_path": repair.worktree_path,
                "branch": repair.branch,
                "case_artifact": str(case_id),
                "snapshot": snapshot,
                "evidence_ids": [str(i) for i in evidence_ids],
            },
            "intent",
        )
        async with self.factory() as uow:
            await uow.repairs.lock()
            current = await uow.repairs.get(repair.id)
            owner = await uow.repairs.owner()
            if (
                current is None
                or current.status != "running"
                or owner.repair_id != repair.id
                or current.last_accepted_commit != intent.expected_parent
                or current.current_fix_id != fix.id
                or current.stage != "commit_acceptance"
                or fix.repair_id != repair.id
                or fix.approved_case_digest != case.case_digest
            ):
                raise RepairNeedsReview("Acceptance ownership changed")
            await uow.repairs.add_acceptance(intent)
            await uow.commit()
        commit = await self.git.accept(intent)
        await self._finalize(intent, commit)
        return commit

    async def _finalize(self, intent: RepairAcceptance, commit: str) -> None:
        metadata, raw = await self.artifacts.read(
            UUID(str(intent.result_refs_json["case_artifact"]))
        )
        import hashlib

        if (
            metadata.sensitivity != "private"
            or hashlib.sha256(raw).hexdigest() != metadata.content_hash
        ):
            raise RepairNeedsReview("Acceptance case integrity changed")
        case = ApprovedCase.model_validate_json(raw)
        async with self.factory() as uow:
            source_repair = await uow.repairs.get(intent.repair_id)
        if (
            source_repair is None
            or metadata.task_id != source_repair.source_task_id
            or case.case.incident_ids.get("task") != source_repair.source_task_id
        ):
            raise RepairNeedsReview("Acceptance evidence scope changed")
        import re

        if not re.fullmatch(r"[0-9a-f]{64}", case.case.fixture_manifest_digest):
            raise RepairNeedsReview("Fixture identity is invalid")
        # Fixture directories were privately materialized and verified by the
        # evaluator. Re-register from the saved case, never candidate source.
        fixture = (
            self.incidents.root.parent
            / "repair-fixtures"
            / case.case.fixture_manifest_digest
        )
        self.incidents.fixtures = {case.case.fixture_manifest_digest: fixture}
        await self.incidents.register(case, commit)
        async with self.factory() as uow:
            await uow.repairs.lock()
            repair = await uow.repairs.get(intent.repair_id)
            fixes = await uow.repairs.list_fixes(intent.repair_id)
            fix = next(f for f in fixes if f.id == intent.fix_id)
            if repair is None or repair.last_accepted_commit not in {
                intent.expected_parent,
                commit,
            }:
                raise RepairNeedsReview("Acceptance parent changed")
            repair = replace(repair, last_accepted_commit=commit)
            await uow.repairs.update(repair)
            attempts = await uow.repairs.list_attempts(intent.repair_id)
            await uow.repairs.update_fix(
                replace(
                    fix,
                    status="accepted",
                    accepted_commit=commit,
                    attempts_used=max(
                        (
                            a.attempt_index
                            for a in attempts
                            if a.fix_id == fix.id and a.role == "proposal" and a.charged
                        ),
                        default=0,
                    ),
                )
            )
            await uow.repairs.update_acceptance(
                replace(intent, status="accepted", commit_sha=commit)
            )
            await append_repair_event(uow, self.clock, repair)
            await uow.commit()

    async def reconcile(
        self, repair: RepairRun, *, apply_pending: bool = False
    ) -> bool:
        async with self.factory() as uow:
            intents = await uow.repairs.list_acceptances(repair.id)
        reconciled = False
        for intent in intents:
            if intent.status == "abandoned":
                continue
            if intent.status == "accepted":
                if (
                    intent.fix_id == repair.current_fix_id
                    and intent.commit_sha == repair.last_accepted_commit
                ):
                    reconciled = True
                continue
            commit = await self.git.reconcile_acceptance(intent)
            if commit is None:
                if not apply_pending:
                    continue
                # Intent saved before staging/commit: retry this exact evaluated
                # candidate, never charge a new model attempt or overwrite edits.
                commit = await self.git.accept(intent)
            await self._finalize(intent, commit)
            reconciled = True
        return reconciled

    async def interrupt(self, repair: RepairRun) -> bool:
        async with self.factory() as uow:
            intents = await uow.repairs.list_acceptances(repair.id)
        committed = False
        for intent in intents:
            if intent.status == "abandoned":
                continue
            if intent.status == "accepted":
                committed |= (
                    intent.fix_id == repair.current_fix_id
                    and intent.commit_sha == repair.last_accepted_commit
                )
                continue
            commit = await self.git.reconcile_acceptance(intent)
            if commit is not None:
                await self._finalize(intent, commit)
                committed = True
            else:
                await self.git.abandon(intent)
                async with self.factory() as uow:
                    await uow.repairs.update_acceptance(
                        replace(intent, status="abandoned")
                    )
                    await uow.commit()
        return committed

    async def resume_pending(self, repair: RepairRun) -> bool:
        return await self.reconcile(repair, apply_pending=True)
