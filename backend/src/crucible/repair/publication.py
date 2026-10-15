"""Idempotent publication with durable intent before each external effect."""

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.repair import RepairPublication
from crucible.repair.admission import AdmissionProcess
from crucible.repair.controller import RepairNeedsReview
from crucible.repair.events import append_repair_event
from crucible.repair.ports import PRIdentity, RepairGitHubPort
from crucible.repair.reporting import RepairReporter


class PublicationService:
    def __init__(
        self,
        factory: Callable[[], UnitOfWork],
        clock: Clock,
        github: RepairGitHubPort,
        reporter: RepairReporter | None,
    ) -> None:
        self.factory, self.clock, self.github, self.reporter = (
            factory,
            clock,
            github,
            reporter,
        )

    async def _save(self, record: RepairPublication) -> None:
        async with self.factory() as uow:
            await uow.repairs.update_publication(record)
            repair = await uow.repairs.get(record.repair_id)
            if repair:
                await append_repair_event(uow, self.clock, repair)
            await uow.commit()

    async def publish(self, repair_id: UUID) -> PRIdentity | None:
        async with self.factory() as uow:
            repair = await uow.repairs.get(repair_id)
            fixes = await uow.repairs.list_fixes(repair_id)
            saved = await uow.repairs.get_publication(repair_id)
        accepted = [f for f in fixes if f.status == "accepted"]
        if not accepted:
            return None
        if (
            repair is None
            or repair.worktree_path is None
            or repair.branch != f"repair/{repair_id}"
            or accepted[-1].accepted_commit != repair.last_accepted_commit
        ):
            raise RepairNeedsReview("Publication identity differs")
        process = AdmissionProcess()
        root = Path(repair.worktree_path)
        if (
            await process.run(root, "git", "rev-parse", "HEAD")
            != repair.last_accepted_commit
            or await process.run(root, "git", "symbolic-ref", "--short", "HEAD")
            != repair.branch
            or await process.run(
                root, "git", "status", "--porcelain", "--untracked-files=all"
            )
        ):
            raise RepairNeedsReview("Publication worktree changed")
        record = saved or RepairPublication(
            repair_id, repair.branch, "main", repair.last_accepted_commit, "intent"
        )
        if (record.head_sha, record.head_branch, record.base_branch) != (
            repair.last_accepted_commit,
            repair.branch,
            "main",
        ):
            raise RepairNeedsReview("Publication intent changed")
        await self._save(record)
        try:
            await self.github.push_branch(record.head_branch, record.head_sha)
            record = replace(record, status="pushed", last_error=None)
            await self._save(record)
            pr = await self.github.find_pr(record.head_branch, record.base_branch)
            if pr is None:
                if self.reporter is None:
                    raise ValueError("Reporter missing")
                report = await self.reporter.build(repair_id)
                record = replace(record, status="creating")
                await self._save(record)
                pr = await self.github.create_pr(
                    record.head_branch,
                    record.base_branch,
                    "Verified harness repair",
                    report.public_body,
                )
            if pr.head_sha != record.head_sha or pr.state != "open":
                raise RepairNeedsReview("Remote PR differs from publication intent")
            record = replace(
                record, status="labeling", pr_number=pr.number, pr_url=pr.url
            )
            await self._save(record)
            await self.github.ensure_label(pr.number, "crucible-repair")
            await self._save(replace(record, status="published"))
            return pr
        except Exception:
            await self._save(
                replace(record, last_error="Publication failed; resume explicitly.")
            )
            raise
