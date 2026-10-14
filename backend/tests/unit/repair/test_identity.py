import subprocess
from pathlib import Path

from crucible.repair.identity import capture_runtime_identity


def git(root, *args):
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True
    ).stdout


def repository(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "user.email", "test@example.com")
    (tmp_path / "source.py").write_text("old")
    (tmp_path / "uv.lock").write_text("locked")
    (tmp_path / ".gitignore").write_text(".env\nprivate/\n")
    git(tmp_path, "add", "source.py", "uv.lock", ".gitignore")
    git(tmp_path, "commit", "-qm", "fixture")


async def test_runtime_identity_does_not_follow_later_edits(tmp_path: Path):
    repository(tmp_path)
    identity = await capture_runtime_identity(tmp_path)
    original = identity.source_digest
    (tmp_path / "source.py").write_text("changed")
    assert identity.source_digest == original
    later = await capture_runtime_identity(tmp_path)
    assert later.source_digest != original
    assert later.commit == identity.commit
    (tmp_path / "uv.lock").write_text("new lock")
    assert (
        await capture_runtime_identity(tmp_path)
    ).lock_digest != identity.lock_digest


async def test_identity_excludes_ignored_data_and_secrets(tmp_path: Path):
    repository(tmp_path)
    identity = await capture_runtime_identity(tmp_path)
    (tmp_path / ".env").write_text("private key")
    (tmp_path / "private").mkdir()
    (tmp_path / "private" / "data").write_text("private evidence")
    assert await capture_runtime_identity(tmp_path) == identity


async def test_identity_never_reads_tracked_secret_or_symlink_target(tmp_path: Path):
    repository(tmp_path)
    (tmp_path / ".env").write_text("secret one")
    git(tmp_path, "add", "-f", ".env")
    outside = tmp_path / "private"
    outside.mkdir()
    (outside / "key").write_text("private one")
    (tmp_path / "linked").symlink_to(outside / "key")
    git(tmp_path, "add", "linked")
    identity = await capture_runtime_identity(tmp_path)
    (tmp_path / ".env").write_text("secret two")
    git(tmp_path, "add", "-f", ".env")
    (outside / "key").write_text("private two")
    assert await capture_runtime_identity(tmp_path) == identity
