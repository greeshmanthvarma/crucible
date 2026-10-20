"""Read-only remote admission and idempotent local worktree provisioning."""

import asyncio
import json
import string
from pathlib import Path
from uuid import UUID

from crucible.repair.identity import RuntimeIdentity, capture_runtime_identity
from crucible.repair.ports import AdmissionCheck, PRIdentity, WorktreeIdentity


class AdmissionProcess:
    async def run(self, root: Path, *argv: str) -> str:
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(process.communicate(), 30)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        if process.returncode:
            raise ValueError("Repair admission command failed")
        return stdout.decode().strip()


class LocalRepairAdmissionGit:
    def __init__(
        self, root: Path, data_dir: Path, process: AdmissionProcess | None = None
    ) -> None:
        self.root, self.data_dir = root.resolve(), data_dir.resolve()
        self.process = process or AdmissionProcess()

    async def _git(self, *args: str) -> str:
        return await self.process.run(self.root, "git", *args)

    async def main_identity(self) -> RuntimeIdentity:
        return await capture_runtime_identity(self.root)

    async def check_fresh_main(self) -> AdmissionCheck:
        if await self._git("symbolic-ref", "--quiet", "--short", "HEAD") != "main":
            return AdmissionCheck(
                False, "not_main", "Switch the harness checkout to main and restart."
            )
        if await self._git("status", "--porcelain", "--untracked-files=all"):
            return AdmissionCheck(
                False,
                "dirty_main",
                "Commit or remove harness checkout changes before repairing.",
            )
        # Fetch changes only refs/objects, never checkout files. No pull/restart.
        await self._git("fetch", "--no-tags", "origin", "refs/heads/main")
        remote = await self._git("rev-parse", "FETCH_HEAD")
        local = await self._git("rev-parse", "HEAD")
        if remote != local:
            return AdmissionCheck(
                False,
                "main_behind",
                "Update main to the remote main revision and restart.",
            )
        return AdmissionCheck(True, None, "Main is clean and current.")

    async def create_worktree(
        self, repair_id: UUID, baseline_commit: str
    ) -> WorktreeIdentity:
        branch = f"repair/{repair_id}"
        destination = self.data_dir / "repair-worktrees" / str(repair_id)
        if destination.exists():
            # A crash may leave the saved provision intent applied but unbound.
            # Validate its registered path, branch and parent before adopting it.
            entries = (await self._git("worktree", "list", "--porcelain")).split("\n\n")
            expected = {
                f"worktree {destination}",
                f"HEAD {baseline_commit}",
                f"branch refs/heads/{branch}",
            }
            if destination.is_symlink() or not any(
                expected.issubset(set(entry.splitlines())) for entry in entries
            ):
                raise ValueError("Existing repair worktree requires review")
            status = await self.process.run(
                destination, "git", "status", "--porcelain", "--untracked-files=all"
            )
            if status:
                raise ValueError("Existing repair worktree has unexpected changes")
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            await self._git(
                "worktree", "add", "-b", branch, str(destination), baseline_commit
            )
        return WorktreeIdentity(str(destination), branch)


class LocalRepairAdmissionGitHub:
    def __init__(self, root: Path, process: AdmissionProcess | None = None) -> None:
        self.root, self.process = root, process or AdmissionProcess()

    async def list_open_repairs(self) -> tuple[PRIdentity, ...]:
        origin = await self.process.run(self.root, "git", "remote", "get-url", "origin")
        # Paginate all open PRs, including a repair whose label application failed.
        raw = await self.process.run(
            self.root,
            "gh",
            "api",
            "--paginate",
            "--slurp",
            f"repos/{self._repository(origin)}/pulls?state=open&per_page=100",
        )
        pages = json.loads(raw)
        return tuple(
            PRIdentity(pr["number"], pr["html_url"], pr["state"], pr["head"]["sha"])
            for page in pages
            for pr in page
            if pr["head"]["ref"].startswith(
                ("repair/", "codex/improve/", "codex/improve-report/")
            )
            or any(
                label["name"] in {"crucible-repair", "crucible-improvement"}
                for label in pr["labels"]
            )
        )

    @staticmethod
    def _repository(origin: str) -> str:
        from urllib.parse import urlparse

        if origin.startswith("git@github.com:"):
            path = origin.removeprefix("git@github.com:")
        else:
            parsed = urlparse(origin)
            if parsed.hostname != "github.com":
                raise ValueError("Configured origin must be a GitHub repository")
            path = parsed.path.lstrip("/")
        path = path.removesuffix(".git")
        if len(path.split("/")) != 2 or any(
            not part
            or part in {".", ".."}
            or any(
                c not in (string.ascii_letters + string.digits + "_.-") for c in part
            )
            for part in path.split("/")
        ):
            raise ValueError("Invalid configured repository")
        return path


class LocalRepairWorkerRecovery:
    """Remove only resources bearing the exact persisted trial-job UUID label."""

    def __init__(self, root: Path, process: AdmissionProcess | None = None) -> None:
        self.root, self.process = root, process or AdmissionProcess()

    async def stop_jobs(self, job_ids: tuple[UUID, ...]) -> None:
        for job_id in job_ids:
            label = f"label=crucible.repair.job={job_id}"
            containers = (
                await self.process.run(
                    self.root, "docker", "ps", "-aq", "--filter", label
                )
            ).split()
            if containers:
                await self.process.run(
                    self.root, "docker", "rm", "--force", *containers
                )
            networks = (
                await self.process.run(
                    self.root, "docker", "network", "ls", "-q", "--filter", label
                )
            ).split()
            if networks:
                await self.process.run(self.root, "docker", "network", "rm", *networks)
            if await self.process.run(
                self.root, "docker", "ps", "-aq", "--filter", label
            ) or await self.process.run(
                self.root, "docker", "network", "ls", "-q", "--filter", label
            ):
                raise ValueError("Repair worker resources are still present")
