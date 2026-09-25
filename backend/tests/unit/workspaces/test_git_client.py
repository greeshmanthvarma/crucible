import subprocess
from datetime import UTC, datetime
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


async def test_acceptance_evidence_is_binary_aware_and_commit_is_owned(
    tmp_path: Path,
) -> None:
    root = tmp_path / "acceptance"
    parent = committed_repository(root)
    (root / "README.md").write_text("changed\n")
    (root / "binary.dat").write_bytes(b"\x00\x01\xff")
    status_before = git("status", "--porcelain=v2", cwd=root)
    client = SubprocessGitClient()

    evidence = await client.acceptance_evidence(root)

    assert evidence.changed_files == ("README.md", "binary.dat")
    assert b"GIT binary patch" in evidence.diff
    assert git("status", "--porcelain=v2", cwd=root) == status_before

    accepted = await client.create_acceptance_commit(
        root,
        "task-123",
        "accept-123",
        datetime(2026, 9, 20, tzinfo=UTC),
    )
    assert accepted.parent_revision == parent
    assert (
        await client.find_owned_acceptance_commit(root, "task-123", "accept-123")
        == accepted
    )
    assert await client.find_owned_acceptance_commit(root, "task-123", "wrong") is None
    assert git("show", "-s", "--format=%an <%ae>", "HEAD", cwd=root) == (
        "Crucible <crucible@local.invalid>"
    )


async def test_cherry_pick_is_traceable_to_selected_result(tmp_path: Path) -> None:
    root = tmp_path / "integration"
    base = committed_repository(root)
    target_ref = git("symbolic-ref", "--short", "HEAD", cwd=root)
    git("checkout", "-qb", "result", cwd=root)
    (root / "README.md").write_text("result\n")
    git("add", "README.md", cwd=root)
    git("commit", "-qm", "accepted result", cwd=root)
    result = git("rev-parse", "HEAD", cwd=root)
    git("checkout", "-q", target_ref, cwd=root)
    client = SubprocessGitClient()

    before = await client.target_snapshot(root)
    applied = await client.cherry_pick(root, result)
    after = await client.target_snapshot(root)

    assert applied.returncode == 0
    assert before.head_revision == base
    assert await client.has_commit(root, result)
    assert await client.is_ancestor(root, base, after.head_revision)
    assert await client.is_applied_cherry_pick(root, after.head_revision, result, base)
