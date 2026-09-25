import asyncio
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from crucible.application.errors import (
    BareRepositoryUnsupported,
    NotAGitRepository,
    RepositoryHasNoCommit,
    RepositoryPathNotFound,
    RevisionNotFound,
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


@dataclass(frozen=True)
class AcceptanceEvidence:
    changed_files: tuple[str, ...]
    diff: bytes


@dataclass(frozen=True)
class AcceptanceCommit:
    commit_sha: str
    parent_revision: str


@dataclass(frozen=True)
class TargetSnapshot:
    head_revision: str
    current_ref: str | None
    status: str
    index_tree: str


class GitClient(Protocol):
    async def resolve_repository(self, candidate: Path) -> ResolvedRepository: ...

    async def resolve_revision(self, root: Path, source_ref: str) -> str: ...

    async def add_worktree(
        self, root: Path, destination: Path, revision: str
    ) -> None: ...

    async def worktree_revision(self, workspace: Path) -> str: ...
    async def owns_worktree(self, root: Path, workspace: Path) -> bool: ...
    async def status(self, workspace: Path) -> GitResult: ...
    async def diff(self, workspace: Path) -> GitResult: ...
    async def apply_patch(
        self, workspace: Path, patch: str, *, check: bool
    ) -> GitResult: ...
    async def acceptance_evidence(self, workspace: Path) -> AcceptanceEvidence: ...
    async def create_acceptance_commit(
        self, workspace: Path, task_id: str, idempotency_key: str, now: datetime
    ) -> AcceptanceCommit: ...
    async def find_owned_acceptance_commit(
        self, workspace: Path, task_id: str, idempotency_key: str
    ) -> AcceptanceCommit | None: ...
    async def commit_evidence(
        self, workspace: Path, commit_sha: str
    ) -> AcceptanceEvidence: ...
    async def target_snapshot(self, root: Path) -> TargetSnapshot: ...
    async def has_commit(self, root: Path, commit_sha: str) -> bool: ...
    async def is_ancestor(self, root: Path, ancestor: str, descendant: str) -> bool: ...
    async def cherry_pick(self, root: Path, commit_sha: str) -> GitResult: ...
    async def abort_cherry_pick(self, root: Path) -> GitResult: ...
    async def is_applied_cherry_pick(
        self, root: Path, head_revision: str, source_commit: str, parent_revision: str
    ) -> bool: ...


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
            raise RevisionNotFound(
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

    async def owns_worktree(self, root: Path, workspace: Path) -> bool:
        result = await self._run(root, "worktree", "list", "--porcelain")
        if result.returncode != 0:
            return False
        expected = workspace.resolve()
        return any(
            Path(line.removeprefix("worktree ")).resolve() == expected
            for line in result.stdout.splitlines()
            if line.startswith("worktree ")
        )

    async def status(self, workspace: Path) -> GitResult:
        return await self._run(workspace, "status", "--porcelain=v2", "--")

    async def diff(self, workspace: Path) -> GitResult:
        return await self._run(workspace, "diff", "--no-ext-diff", "--binary", "--")

    async def apply_patch(
        self, workspace: Path, patch: str, *, check: bool
    ) -> GitResult:
        arguments = ["apply", "--whitespace=nowarn"]
        if check:
            arguments.append("--check")
        arguments.append("-")
        return await self._run(workspace, *arguments, input_text=patch)

    async def acceptance_evidence(self, workspace: Path) -> AcceptanceEvidence:
        descriptor, temporary_name = tempfile.mkstemp(prefix="crucible-index-")
        os.close(descriptor)
        temporary = Path(temporary_name)
        temporary.unlink()
        environment = {"GIT_INDEX_FILE": str(temporary)}
        try:
            read_tree = await self._run(workspace, "read-tree", "HEAD", env=environment)
            if read_tree.returncode != 0:
                raise RuntimeError(read_tree.stderr.strip())
            added = await self._run(workspace, "add", "-A", "--", env=environment)
            if added.returncode != 0:
                raise RuntimeError(added.stderr.strip())
            names = await self._run(
                workspace,
                "diff",
                "--cached",
                "--name-only",
                "-z",
                "--",
                env=environment,
            )
            diff_code, diff_stdout, diff_stderr = await self._run_bytes(
                workspace,
                "diff",
                "--cached",
                "--no-ext-diff",
                "--binary",
                "--",
                env=environment,
            )
            if names.returncode != 0 or diff_code != 0:
                raise RuntimeError((names.stderr or diff_stderr.decode()).strip())
            return AcceptanceEvidence(
                tuple(name for name in names.stdout.split("\0") if name), diff_stdout
            )
        finally:
            temporary.unlink(missing_ok=True)

    async def create_acceptance_commit(
        self, workspace: Path, task_id: str, idempotency_key: str, now: datetime
    ) -> AcceptanceCommit:
        parent = await self.worktree_revision(workspace)
        added = await self._run(workspace, "add", "-A", "--")
        if added.returncode != 0:
            raise RuntimeError(added.stderr.strip())
        timestamp = now.isoformat()
        environment = {
            "GIT_AUTHOR_NAME": "Crucible",
            "GIT_AUTHOR_EMAIL": "crucible@local.invalid",
            "GIT_COMMITTER_NAME": "Crucible",
            "GIT_COMMITTER_EMAIL": "crucible@local.invalid",
            "GIT_AUTHOR_DATE": timestamp,
            "GIT_COMMITTER_DATE": timestamp,
        }
        committed = await self._run(
            workspace,
            "commit",
            "--no-gpg-sign",
            "-m",
            "Accept Crucible task result",
            "-m",
            f"Crucible-Task-Id: {task_id}\nCrucible-Acceptance-Key: {idempotency_key}",
            "--",
            env=environment,
        )
        if committed.returncode != 0:
            raise RuntimeError(committed.stderr.strip())
        return AcceptanceCommit(await self.worktree_revision(workspace), parent)

    async def find_owned_acceptance_commit(
        self, workspace: Path, task_id: str, idempotency_key: str
    ) -> AcceptanceCommit | None:
        shown = await self._run(
            workspace,
            "show",
            "-s",
            "--format=%H%x00%P%x00%an%x00%ae%x00%B",
            "HEAD",
            "--",
        )
        if shown.returncode != 0:
            return None
        commit_sha, parents, author, email, message = shown.stdout.split("\0", 4)
        lines = {line.strip() for line in message.splitlines()}
        if (
            author != "Crucible"
            or email != "crucible@local.invalid"
            or f"Crucible-Task-Id: {task_id}" not in lines
            or f"Crucible-Acceptance-Key: {idempotency_key}" not in lines
        ):
            return None
        parent_values = parents.split()
        if len(parent_values) != 1:
            return None
        return AcceptanceCommit(commit_sha, parent_values[0])

    async def commit_evidence(
        self, workspace: Path, commit_sha: str
    ) -> AcceptanceEvidence:
        names = await self._run(
            workspace,
            "diff-tree",
            "--no-commit-id",
            "--name-only",
            "-r",
            "-z",
            commit_sha,
            "--",
        )
        diff_code, diff_stdout, diff_stderr = await self._run_bytes(
            workspace,
            "show",
            "--format=",
            "--no-ext-diff",
            "--binary",
            commit_sha,
            "--",
        )
        if names.returncode != 0 or diff_code != 0:
            raise RuntimeError((names.stderr or diff_stderr.decode()).strip())
        return AcceptanceEvidence(
            tuple(name for name in names.stdout.split("\0") if name), diff_stdout
        )

    async def target_snapshot(self, root: Path) -> TargetSnapshot:
        head = await self.worktree_revision(root)
        branch = await self._run(root, "symbolic-ref", "--short", "HEAD", "--")
        status = await self._run(
            root, "status", "--porcelain=v2", "--untracked-files=all", "--"
        )
        index = await self._run(root, "write-tree")
        if status.returncode != 0 or index.returncode != 0:
            raise RuntimeError((status.stderr or index.stderr).strip())
        return TargetSnapshot(
            head,
            branch.stdout.strip() if branch.returncode == 0 else None,
            status.stdout,
            index.stdout.strip(),
        )

    async def has_commit(self, root: Path, commit_sha: str) -> bool:
        result = await self._run(
            root, "cat-file", "-e", f"{commit_sha}^{{commit}}", "--"
        )
        return result.returncode == 0

    async def is_ancestor(self, root: Path, ancestor: str, descendant: str) -> bool:
        result = await self._run(
            root, "merge-base", "--is-ancestor", ancestor, descendant, "--"
        )
        return result.returncode == 0

    async def cherry_pick(self, root: Path, commit_sha: str) -> GitResult:
        return await self._run(root, "cherry-pick", "-x", commit_sha, "--")

    async def abort_cherry_pick(self, root: Path) -> GitResult:
        return await self._run(root, "cherry-pick", "--abort")

    async def is_applied_cherry_pick(
        self, root: Path, head_revision: str, source_commit: str, parent_revision: str
    ) -> bool:
        shown = await self._run(
            root,
            "show",
            "-s",
            "--format=%H%x00%P%x00%B",
            head_revision,
            "--",
        )
        if shown.returncode != 0:
            return False
        commit_sha, parents, message = shown.stdout.split("\0", 2)
        return (
            commit_sha == head_revision
            and parents.split() == [parent_revision]
            and f"(cherry picked from commit {source_commit})"
            in {line.strip() for line in message.splitlines()}
        )

    async def _run(
        self,
        cwd: Path,
        *arguments: str,
        input_text: str | None = None,
        env: dict[str, str] | None = None,
    ) -> GitResult:
        process = await asyncio.create_subprocess_exec(
            "git",
            *arguments,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE if input_text is not None else None,
            env={**os.environ, **(env or {})},
        )
        stdout, stderr = await process.communicate(
            input_text.encode() if input_text is not None else None
        )
        return GitResult(
            returncode=process.returncode or 0,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
        )

    async def _run_bytes(
        self, cwd: Path, *arguments: str, env: dict[str, str] | None = None
    ) -> tuple[int, bytes, bytes]:
        process = await asyncio.create_subprocess_exec(
            "git",
            *arguments,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, **(env or {})},
        )
        stdout, stderr = await process.communicate()
        return process.returncode or 0, stdout, stderr
