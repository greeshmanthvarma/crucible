from pathlib import Path

from crucible.tools.definitions import ToolContext
from crucible.tools.filesystem import ListFilesTool, ReadFileTool, SearchFilesTool


async def test_read_tools_are_bounded_stable_and_workspace_relative(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "b.txt").write_text("needle\nsecond\n")
    (workspace / "a.txt").write_text("first\nneedle\nthird\n")
    (workspace / ".git").write_text("gitdir: elsewhere")
    context = ToolContext(workspace)

    listed = await ListFilesTool(max_entries=1).invoke(context, {})
    read = await ReadFileTool(max_bytes=8).invoke(
        context, {"path": "a.txt", "start_line": 2, "end_line": 3}
    )
    searched = await SearchFilesTool(max_matches=10).invoke(
        context, {"query": "needle"}
    )

    assert listed.data["paths"] == ["a.txt"]
    assert listed.truncated is True
    assert read.data["content"] == "needle\nt"
    assert read.truncated is True
    assert searched.error_code is None, searched.display_text
    assert all(not str(path).startswith("/") for path in searched.data["matches"])
