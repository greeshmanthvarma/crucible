from pathlib import Path

import pytest

from crucible.tools.definitions import ToolContext
from crucible.tools.filesystem import SearchFilesTool
from crucible.tools.workspace_authority import WorkspacePathResolver


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


async def test_search_cap_reports_more_matches(tmp_path: Path) -> None:
    (tmp_path / "matches.txt").write_text("x\n" * 25)
    outcome = await SearchFilesTool().invoke(ToolContext(tmp_path), {"query": "x"})
    locations = outcome.data["locations"]
    assert isinstance(locations, list)
    assert [hit["line"] for hit in locations] == list(range(1, 21))
    assert outcome.truncated
    assert outcome.display_text.endswith(
        "[Showing the first 20 matches; more exist. "
        "Narrow the query or pass a subdirectory path.]"
    )


async def test_search_under_cap_has_no_notice(tmp_path: Path) -> None:
    (tmp_path / "matches.txt").write_text("needle\n" * 5)
    outcome = await SearchFilesTool().invoke(ToolContext(tmp_path), {"query": "needle"})
    matches = outcome.data["matches"]
    assert isinstance(matches, list) and len(matches) == 5
    assert not outcome.truncated
    assert "Narrow the query" not in outcome.display_text
