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
