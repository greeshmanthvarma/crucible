from pathlib import Path

import pytest

from crucible.tools.filesystem import WorkspacePathResolver


def test_resolver_rejects_absolute_traversal_and_symlink_escapes(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    (workspace / "file-link").symlink_to(outside / "secret.txt")
    (workspace / "dir-link").symlink_to(outside, target_is_directory=True)
    (workspace / "dangling").symlink_to(outside / "missing")
    resolver = WorkspacePathResolver(workspace)

    for path in (
        "/etc/passwd",
        "../outside/secret.txt",
        "file-link",
        "dir-link/secret.txt",
    ):
        with pytest.raises(ValueError):
            resolver.resolve(path)
    with pytest.raises(ValueError, match="dangling"):
        resolver.resolve("dangling")


def test_resolver_allows_internal_symlinks_and_missing_targets(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    directory = workspace / "actual"
    directory.mkdir(parents=True)
    (directory / "file.txt").write_text("ok")
    (workspace / "internal").symlink_to(directory, target_is_directory=True)
    resolver = WorkspacePathResolver(workspace)

    assert resolver.resolve("internal/file.txt") == directory / "file.txt"
    assert (
        resolver.resolve("actual/new.txt", allow_missing=True) == directory / "new.txt"
    )
