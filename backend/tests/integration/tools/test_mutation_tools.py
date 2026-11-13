import subprocess
from pathlib import Path

from crucible.tools.definitions import ToolContext
from crucible.tools.git_tools import (
    ApplyPatchTool,
    WorkspaceDiffTool,
    WorkspaceStatusTool,
    WriteFileTool,
)


def git(*arguments: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


def repository(path: Path) -> None:
    path.mkdir()
    git("init", "-q", cwd=path)
    git("config", "user.name", "Test", cwd=path)
    git("config", "user.email", "test@example.com", cwd=path)
    (path / "file.txt").write_text("old\n")
    git("add", "file.txt", cwd=path)
    git("commit", "-qm", "fixture", cwd=path)


async def test_write_patch_status_and_diff_are_confined_and_structured(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    repository(workspace)
    context = ToolContext(workspace)

    written = await WriteFileTool().invoke(
        context, {"path": "unicode.txt", "content": "héllo\n"}
    )
    patch = """--- a/file.txt
+++ b/file.txt
@@ -1 +1 @@
-old
+new
"""
    applied = await ApplyPatchTool().invoke(context, {"patch": patch})
    status = await WorkspaceStatusTool().invoke(context, {})
    diff = await WorkspaceDiffTool().invoke(context, {})

    assert written.error_code is None
    assert (workspace / "unicode.txt").read_text() == "héllo\n"
    assert applied.error_code is None
    assert (workspace / "file.txt").read_text() == "new\n"
    assert "file.txt" in str(status.data["output"])
    assert "+new" in str(diff.data["output"])


async def test_malformed_patch_does_not_partially_mutate(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    repository(workspace)
    patch = """--- a/file.txt
+++ b/file.txt
@@ -1 +1 @@
-does-not-match
+new
"""

    outcome = await ApplyPatchTool().invoke(ToolContext(workspace), {"patch": patch})

    assert outcome.error_code == "patch_rejected"
    assert (workspace / "file.txt").read_text() == "old\n"


async def test_codex_style_patch_explains_required_format(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    repository(workspace)
    patch = (
        "*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch\n"
    )

    outcome = await ApplyPatchTool().invoke(ToolContext(workspace), {"patch": patch})

    assert outcome.error_code == "patch_format_unsupported"
    assert "--- a/file.txt" in outcome.display_text
    assert (workspace / "file.txt").read_text() == "old\n"


async def test_write_file_rejects_symlink_target(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    repository(workspace)
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")
    (workspace / "escape.txt").symlink_to(outside)

    try:
        await WriteFileTool().invoke(
            ToolContext(workspace), {"path": "escape.txt", "content": "changed"}
        )
    except ValueError as error:
        assert "symlink" in str(error)
    else:
        raise AssertionError("symlink target was accepted")
    assert outside.read_text() == "outside"


async def test_edit_file_replaces_unique_text(tmp_path: Path) -> None:
    from crucible.tools.git_tools import EditFileTool

    (tmp_path / "x").write_text("héllo\nworld\n")
    outcome = await EditFileTool().invoke(
        ToolContext(tmp_path), {"path": "x", "old_text": "héllo", "new_text": "hello"}
    )
    assert outcome.error_code is None
    assert outcome.data == {"path": "x", "replacements": 1}
    assert (tmp_path / "x").read_text() == "hello\nworld\n"


async def test_edit_file_rejects_missing_and_ambiguous_text(tmp_path: Path) -> None:
    from crucible.tools.git_tools import EditFileTool

    path = tmp_path / "x"
    path.write_text("a\na\n")
    for old, new, code in (
        ("a", "b", "edit_ambiguous"),
        ("zzz", "b", "edit_no_match"),
        ("a", "a", "edit_noop"),
    ):
        outcome = await EditFileTool().invoke(
            ToolContext(tmp_path), {"path": "x", "old_text": old, "new_text": new}
        )
        assert outcome.error_code == code
        assert path.read_text() == "a\na\n"
    missing = await EditFileTool().invoke(
        ToolContext(tmp_path), {"path": "missing", "old_text": "a", "new_text": "b"}
    )
    assert missing.error_code == "file_not_found"
    assert "write_file" in missing.display_text
    assert not (tmp_path / "missing").exists()


async def test_edit_file_preserves_crlf_line_endings(tmp_path: Path) -> None:
    from crucible.tools.git_tools import EditFileTool

    path = tmp_path / "x"
    path.write_bytes(b"one\r\ntwo\r\nthree\r\n")
    outcome = await EditFileTool().invoke(
        ToolContext(tmp_path),
        {"path": "x", "old_text": "one\ntwo", "new_text": "one\n2\ntwo"},
    )
    assert outcome.error_code is None
    assert path.read_bytes() == b"one\r\n2\r\ntwo\r\nthree\r\n"


async def test_edit_file_replace_all(tmp_path: Path) -> None:
    from crucible.tools.git_tools import EditFileTool

    (tmp_path / "x").write_text("a\na\n")
    outcome = await EditFileTool().invoke(
        ToolContext(tmp_path),
        {"path": "x", "old_text": "a", "new_text": "", "replace_all": True},
    )
    assert outcome.data == {"path": "x", "replacements": 2}
    assert (tmp_path / "x").read_text() == "\n\n"
