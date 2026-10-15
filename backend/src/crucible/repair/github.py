"""GitHub CLI adapter; private bodies never enter shell commands or workers."""

import json
import re
import tempfile
from pathlib import Path

from crucible.repair.admission import LocalRepairAdmissionGitHub
from crucible.repair.ports import PRIdentity


class LocalRepairGitHub(LocalRepairAdmissionGitHub):
    async def _repo(self) -> str:
        return self._repository(
            await self.process.run(self.root, "git", "remote", "get-url", "origin")
        )

    async def push_branch(self, branch: str, head_sha: str) -> None:
        if not re.fullmatch(r"repair/[0-9a-f-]{36}", branch) or not re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}", head_sha
        ):
            raise ValueError("Invalid repair branch identity")
        # Push the exact accepted object, never an implicitly resolved local tip.
        await self.process.run(
            self.root, "git", "push", "origin", f"{head_sha}:refs/heads/{branch}"
        )

    async def find_pr(self, head: str, base: str) -> PRIdentity | None:
        repo = await self._repo()
        raw = await self.process.run(
            self.root,
            "gh",
            "pr",
            "list",
            "--repo",
            repo,
            "--head",
            head,
            "--base",
            base,
            "--state",
            "all",
            "--json",
            "number,url,state,headRefOid",
        )
        records = json.loads(raw)
        if len(records) > 1:
            raise ValueError("Ambiguous repair PR identity")
        return self._identity(records[0]) if records else None

    @staticmethod
    def _identity(record: dict[str, object]) -> PRIdentity:
        return PRIdentity(
            int(str(record["number"])),
            str(record["url"]),
            str(record["state"]).lower(),
            str(record["headRefOid"]),
        )

    async def create_pr(
        self, head: str, base: str, title: str, body: str
    ) -> PRIdentity:
        repo = await self._repo()
        with tempfile.TemporaryDirectory(prefix="repair-pr-") as directory:
            path = Path(directory) / "body.md"
            path.write_text(body)
            path.chmod(0o600)
            await self.process.run(
                self.root,
                "gh",
                "pr",
                "create",
                "--repo",
                repo,
                "--head",
                head,
                "--base",
                base,
                "--title",
                title,
                "--body-file",
                str(path),
            )
        pr = await self.find_pr(head, base)
        if pr is None:
            raise ValueError("Created PR identity unavailable")
        return pr

    async def ensure_label(self, pr_number: int, label: str) -> None:
        repo = await self._repo()
        # PATCH is idempotent and creates the repository label if missing.
        await self.process.run(
            self.root,
            "gh",
            "label",
            "create",
            label,
            "--repo",
            repo,
            "--color",
            "5319e7",
            "--force",
        )
        await self.process.run(
            self.root,
            "gh",
            "pr",
            "edit",
            str(pr_number),
            "--repo",
            repo,
            "--add-label",
            label,
        )
