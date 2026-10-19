"""Intent-first accepted Git commits. Reconciliation never reruns a model."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from crucible.improvement.contracts import (
    EDITABLE_PATHS,
    ExperimentPaths,
    HarnessSnapshot,
)
from crucible.improvement.roles import SelectionDecision
from crucible.improvement.selection import decide_candidate
from crucible.improvement.source import (
    ScopeRejected,
    _patch,
    git_bytes,
    inspect_source,
    verify_harness_export,
)
from crucible.improvement.store import (
    ExperimentStore,
    PromotionRecord,
    RevisionConflict,
)
from crucible.improvement.trials import (
    build_evaluation_set,
    owned_artifact,
    verified_candidate_checks,
)
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.improvement.worktree import OwnedWorktree
from crucible.repair.identity import digest


class PromotionService:
    def __init__(
        self, store: ExperimentStore, paths: ExperimentPaths, protected_suite_root: Path
    ) -> None:
        self.store, self.paths, self.protected_suite_root = (
            store,
            paths,
            protected_suite_root,
        )

    async def accepted_snapshot(self, experiment_id: UUID) -> HarnessSnapshot:
        return await self._snapshot(
            experiment_id, await self.store.accepted_snapshot_id(experiment_id)
        )

    async def _snapshot(
        self, experiment_id: UUID, snapshot_id: UUID
    ) -> HarnessSnapshot:
        row = await self.store.snapshot(experiment_id, snapshot_id)
        path = owned_artifact(self.paths.root, str(row["artifact_ref"]))
        snapshot = HarnessSnapshot.model_validate_json(
            read_regular_artifact(path, 65536)
        )
        if (
            str(snapshot.snapshot_id),
            snapshot.source_digest,
            snapshot.export_digest,
            snapshot.parent_commit,
            snapshot.patch_digest,
            snapshot.kind,
        ) != (
            row["id"],
            row["source_digest"],
            row["export_digest"],
            row["parent_commit"],
            row["patch_digest"],
            row["kind"],
        ) or row["status"] != "verified":
            raise RevisionConflict("promotion snapshot drift")
        verify_harness_export(path.parent / "harness", snapshot)
        return snapshot

    def _owned(
        self, experiment_id: UUID, initial_commit: str, parent: str
    ) -> OwnedWorktree:
        root = self.paths.root / "ownership" / str(experiment_id)
        intent = json.loads(read_regular_artifact(root / "intent.json", 65536))
        path = self.paths.root / "worktrees" / str(experiment_id)
        branch = "codex/improve/" + str(experiment_id)
        if intent != {
            "repository": str(self.paths.repository),
            "path": str(path),
            "branch": branch,
            "parent": initial_commit,
        }:
            raise ScopeRejected("promotion worktree ownership drift")
        return OwnedWorktree(path, initial_commit, branch, root, parent)

    async def _decision(
        self, round_id: UUID, expected_digest: str
    ) -> tuple[SelectionDecision, HarnessSnapshot]:
        round = await self.store.round(round_id)
        if (
            round.status != "decided"
            or round.decision_ref is None
            or round.decision_digest != expected_digest
        ):
            raise RevisionConflict("promotion decision missing or changed")
        decision = SelectionDecision.model_validate_json(round.decision_ref)
        if digest(decision) != expected_digest or decision.verdict != "promote":
            raise RevisionConflict("complete promotion decision required")
        if round.candidate_snapshot_id is None:
            raise RevisionConflict("round candidate snapshot unavailable")
        candidate = await self._snapshot(
            round.experiment_id, round.candidate_snapshot_id
        )
        if (candidate.source_digest, candidate.export_digest) != (
            decision.candidate_source_digest,
            decision.candidate_export_digest,
        ):
            raise RevisionConflict("round candidate identity drift")
        run = await self.store.get(round.experiment_id)
        checks = await verified_candidate_checks(
            self.store,
            self.paths,
            run.id,
            candidate,
            run.manifest,
            self.protected_suite_root,
        )
        before = await build_evaluation_set(
            self.store, self.paths, run.id, round.baseline_snapshot_id
        )
        after = await build_evaluation_set(
            self.store, self.paths, run.id, candidate.snapshot_id
        )
        if decide_candidate(before, after, checks) != decision:
            raise RevisionConflict("promotion evidence or policy drift")
        return decision, candidate

    def _tree(
        self, owned: OwnedWorktree, parent: str, source_digest: str, patch_digest: str
    ) -> str:
        inspection = inspect_source(owned.path, parent)
        if inspection.source_digest != source_digest or any(
            p not in EDITABLE_PATHS for p in inspection.changed_paths
        ):
            raise ScopeRejected("promotion bytes changed or protected edit")
        checkpoint = json.loads(read_regular_artifact(owned.root / "owned.json", 65536))
        if checkpoint["source_digest"] != source_digest:
            raise ScopeRejected("unowned edits before promotion")
        import hashlib

        if hashlib.sha256(_patch(inspection)).hexdigest() != patch_digest:
            raise ScopeRejected("promotion patch digest drift")
        index = owned.root / ("promotion-" + str(uuid4()) + ".index")
        git_bytes(owned.path, "read-tree", parent, index_file=index)
        for name in inspection.changed_paths:
            if name not in inspection.files:
                git_bytes(
                    owned.path,
                    "update-index",
                    "--force-remove",
                    "--",
                    name,
                    index_file=index,
                )
            else:
                mode, raw = inspection.files[name]
                blob = (
                    git_bytes(owned.path, "hash-object", "-w", "--stdin", data=raw)
                    .decode()
                    .strip()
                )
                git_bytes(
                    owned.path,
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"{mode:o}",
                    blob,
                    name,
                    index_file=index,
                )
        return git_bytes(owned.path, "write-tree", index_file=index).decode().strip()

    @staticmethod
    def _message(record: PromotionRecord) -> str:
        return (
            f"Improve harness for experiment {record.experiment_id} "
            f"round {record.round_id}\n\nPromotion intent: {record.id}"
        )

    async def promote(self, round_id: UUID, decision_digest: str) -> PromotionRecord:
        round = await self.store.round(round_id)
        existing = next(
            (
                p
                for p in await self.store.promotions(round.experiment_id)
                if p.round_id == round_id
            ),
            None,
        )
        if existing:
            if existing.decision_digest != decision_digest:
                raise RevisionConflict("promotion digest changed")
            return await self.reconcile_promotion(existing.id)
        run = await self.store.require_search_open(round.experiment_id)
        _, candidate = await self._decision(round_id, decision_digest)
        if candidate.patch_digest is None:
            raise RevisionConflict("no candidate patch")
        owned = self._owned(run.id, run.initial_commit, run.last_accepted_commit)
        await owned.inspect()
        tree = await asyncio.to_thread(
            self._tree,
            owned,
            run.last_accepted_commit,
            candidate.source_digest,
            candidate.patch_digest,
        )
        if (
            git_bytes(owned.path, "write-tree").decode().strip()
            != git_bytes(owned.path, "rev-parse", run.last_accepted_commit + "^{tree}")
            .decode()
            .strip()
        ):
            raise ScopeRejected("unexpected staged user edits")
        record = await self.store.prepare_promotion(
            round_id,
            candidate.snapshot_id,
            tree,
            candidate.patch_digest,
            decision_digest,
            datetime.now(UTC).isoformat(),
        )
        return await self.reconcile_promotion(record.id)

    async def reconcile_promotion(
        self, promotion_id: UUID, *, allow_commit: bool = True
    ) -> PromotionRecord:
        record = await self.store.promotion(promotion_id)
        if record.state == "complete":
            return record
        run = await self.store.get(record.experiment_id)
        _, candidate = await self._decision(record.round_id, record.decision_digest)
        owned = self._owned(run.id, run.initial_commit, record.expected_parent)
        branch = (
            git_bytes(owned.path, "symbolic-ref", "--short", "HEAD").decode().strip()
        )
        if branch != owned.branch:
            raise ScopeRejected("promotion branch drift")
        tree = await asyncio.to_thread(
            self._tree,
            owned,
            record.expected_parent,
            candidate.source_digest,
            record.patch_digest,
        )
        if tree != record.expected_tree:
            raise ScopeRejected("promotion tree drift")
        head = git_bytes(owned.path, "rev-parse", "HEAD").decode().strip()
        message = self._message(record)
        current_index = git_bytes(owned.path, "write-tree").decode().strip()
        parent_tree = (
            git_bytes(owned.path, "rev-parse", record.expected_parent + "^{tree}")
            .decode()
            .strip()
        )
        if current_index not in (
            {parent_tree}
            if head == record.expected_parent
            else {parent_tree, record.expected_tree}
        ):
            raise ScopeRejected("unexpected staged user edits")
        if head == record.expected_parent:
            if not allow_commit:
                return record
            await self.store.require_search_open(run.id)
            events = await self.store.events(run.id, "promotion_intent")
            timestamp = next(
                str(e["timestamp"])
                for e in events
                if e["promotion_id"] == str(record.id)
            )
            commit = (
                git_bytes(
                    owned.path,
                    "-c",
                    "commit.gpgsign=false",
                    "commit-tree",
                    tree,
                    "-p",
                    record.expected_parent,
                    "-m",
                    message,
                    commit_date=timestamp,
                )
                .decode()
                .strip()
            )
            await self.store.require_search_open(run.id)
            if (
                inspect_source(owned.path, record.expected_parent).source_digest
                != candidate.source_digest
            ):
                raise ScopeRejected("source changed during promotion")
            git_bytes(
                owned.path,
                "update-ref",
                "refs/heads/" + owned.branch,
                commit,
                record.expected_parent,
            )
            head = commit
        actual = (
            git_bytes(owned.path, "show", "-s", "--format=%P%n%T%n%B", head)
            .decode()
            .strip()
        )
        if actual != f"{record.expected_parent}\n{record.expected_tree}\n{message}":
            raise ScopeRejected("ambiguous promotion commit requires recovery")
        if (
            inspect_source(owned.path, record.expected_parent).source_digest
            != candidate.source_digest
        ):
            raise ScopeRejected("source changed after promotion commit")
        if git_bytes(owned.path, "write-tree").decode().strip() not in {
            parent_tree,
            record.expected_tree,
        }:
            raise ScopeRejected("staged edits changed during promotion")
        # Updating only this owned index does not alter source or the main checkout.
        git_bytes(owned.path, "read-tree", head)
        return await self.store.complete_promotion(record.id, head)
