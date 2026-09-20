import subprocess
from pathlib import Path

import pytest

from crucible.application.errors import (
    BareRepositoryUnsupported,
    NotAGitRepository,
    RepositoryHasNoCommit,
    RepositoryPathNotFound,
)
from crucible.workspaces.git import SubprocessGitClient


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def committed_repository(path: Path) -> str:
    path.mkdir()
    git("init", "-q", cwd=path)
    git("config", "user.email", "test@example.com", cwd=path)
    git("config", "user.name", "Test User", cwd=path)
    (path / "README.md").write_text("fixture\n")
    git("add", "README.md", cwd=path)
    git("commit", "-qm", "fixture", cwd=path)
    return git("rev-parse", "HEAD^{commit}", cwd=path)


async def test_resolves_nested_candidate_to_canonical_root_and_head(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repository"
    expected_head = committed_repository(root)
    nested = root / "nested"
    nested.mkdir()

    resolved = await SubprocessGitClient().resolve_repository(nested)

    assert resolved.root == root.resolve()
    assert resolved.head_revision == expected_head


@pytest.mark.parametrize("kind", ["missing", "not_git", "bare", "unborn"])
async def test_rejects_unsupported_repository_candidates(
    tmp_path: Path, kind: str
) -> None:
    candidate = tmp_path / kind
    if kind == "not_git":
        candidate.mkdir()
        expected = NotAGitRepository
    elif kind == "bare":
        subprocess.run(["git", "init", "--bare", "-q", str(candidate)], check=True)
        expected = BareRepositoryUnsupported
    elif kind == "unborn":
        candidate.mkdir()
        git("init", "-q", cwd=candidate)
        expected = RepositoryHasNoCommit
    else:
        expected = RepositoryPathNotFound

    with pytest.raises(expected):
        await SubprocessGitClient().resolve_repository(candidate)
