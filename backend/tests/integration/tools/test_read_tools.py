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


async def test_list_files_is_breadth_first_and_explains_truncation(
    tmp_path: Path,
) -> None:
    from crucible.tools.filesystem import LIST_TRUNCATED_NOTICE

    workspace = tmp_path / "workspace"
    (workspace / "pkg" / "deep").mkdir(parents=True)
    (workspace / "README.md").write_text("x")
    (workspace / "pkg" / "mod.py").write_text("x")
    (workspace / "pkg" / "deep" / "leaf.py").write_text("x")
    context = ToolContext(workspace)
    full = await ListFilesTool().invoke(context, {})
    cut = await ListFilesTool(max_entries=3).invoke(context, {})
    assert full.data["paths"] == [
        "README.md",
        "pkg/",
        "pkg/deep/",
        "pkg/mod.py",
        "pkg/deep/leaf.py",
    ]
    assert full.truncated is False
    assert cut.data["paths"] == ["README.md", "pkg/", "pkg/deep/"]
    assert cut.truncated is True
    assert cut.display_text.endswith(LIST_TRUNCATED_NOTICE)
