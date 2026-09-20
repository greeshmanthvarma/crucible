import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from crucible.application.errors import (
    BareRepositoryUnsupported,
    NotAGitRepository,
    RepositoryHasNoCommit,
    RepositoryPathNotFound,
)


@dataclass(frozen=True)
class ResolvedRepository:
    root: Path
    head_revision: str


@dataclass(frozen=True)
class GitResult:
    returncode: int
    stdout: str
    stderr: str


class GitClient(Protocol):
    async def resolve_repository(self, candidate: Path) -> ResolvedRepository: ...

    async def resolve_revision(self, root: Path, source_ref: str) -> str: ...

    async def add_worktree(
        self, root: Path, destination: Path, revision: str
    ) -> None: ...

    async def worktree_revision(self, workspace: Path) -> str: ...


class SubprocessGitClient:
    async def resolve_repository(self, candidate: Path) -> ResolvedRepository:
        if not candidate.exists():
            raise RepositoryPathNotFound(f"Repository path does not exist: {candidate}")
        candidate = candidate.resolve()
        bare = await self._run(candidate, "rev-parse", "--is-bare-repository", "--")
        if bare.returncode != 0:
            raise NotAGitRepository(f"Path is not inside a Git repository: {candidate}")
        if bare.stdout.splitlines()[0] == "true":
            raise BareRepositoryUnsupported(
                f"Bare repositories are unsupported: {candidate}"
            )
        root_result = await self._run(candidate, "rev-parse", "--show-toplevel", "--")
        if root_result.returncode != 0:
            raise NotAGitRepository(f"Path is not inside a Git repository: {candidate}")
        root = Path(root_result.stdout.splitlines()[0]).resolve()
        head = await self._run(root, "rev-parse", "--verify", "HEAD^{commit}", "--")
        if head.returncode != 0:
            raise RepositoryHasNoCommit(f"Repository has no commit: {root}")
        return ResolvedRepository(root=root, head_revision=head.stdout.splitlines()[0])

    async def resolve_revision(self, root: Path, source_ref: str) -> str:
        result = await self._run(
            root,
            "rev-parse",
            "--verify",
            "--end-of-options",
            f"{source_ref}^{{commit}}",
            "--",
        )
        if result.returncode != 0:
            raise RepositoryHasNoCommit(
                f"Revision does not resolve to a commit: {source_ref}"
            )
        return result.stdout.splitlines()[0]

    async def add_worktree(self, root: Path, destination: Path, revision: str) -> None:
        result = await self._run(
            root, "worktree", "add", "--detach", "--", str(destination), revision
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())

    async def worktree_revision(self, workspace: Path) -> str:
        result = await self._run(
            workspace, "rev-parse", "--verify", "HEAD^{commit}", "--"
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        return result.stdout.splitlines()[0]

    async def _run(self, cwd: Path, *arguments: str) -> GitResult:
        process = await asyncio.create_subprocess_exec(
            "git",
            *arguments,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return GitResult(
            returncode=process.returncode or 0,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
        )
