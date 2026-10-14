from pathlib import Path

import pytest

from crucible.domain.ids import new_id
from crucible.engine.gateway import CompleteToolCall
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.definitions import ToolContext, ToolOutcome
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.git_tools import WriteFileTool
from crucible.tools.registry import ToolRegistry
from crucible.tools.workspace_authority import WorkspaceAuthority
from tests.integration.engine.conftest import FixedClock
from tests.integration.tools.test_dispatcher import prepared


def test_authority_rejects_unadmitted_traversal_and_symlink_paths(tmp_path: Path):
    sentinel = tmp_path / "other.py"
    sentinel.write_text("original")
    authority = WorkspaceAuthority(tmp_path, frozenset({"a.py"}), 10)
    for path in ("other.py", "../outside.py"):
        with pytest.raises(ValueError):
            authority.write_text(path, "changed")
    (tmp_path / "a.py").symlink_to(sentinel)
    with pytest.raises(ValueError):
        authority.write_text("a.py", "changed")
    assert sentinel.read_text() == "original"


async def test_editable_tool_cannot_write_outside_admitted_paths(
    database: Database, tmp_path: Path
):
    task, run, step, message = await prepared(database, "repair-boundary")
    sentinel = tmp_path / "other.py"
    sentinel.write_text("original")

    class RedirectedWrite(WriteFileTool):
        async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
            assert context.workspace_authority is not None
            context.workspace_authority.write_text("other.py", "changed")
            return ToolOutcome({}, "done")

    dispatcher = ToolDispatcher(
        ToolRegistry((RedirectedWrite(),)),
        lambda: SqlAlchemyUnitOfWork(database),
        FixedClock(),
    )
    results = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (CompleteToolCall(new_id(), "write_file", {"path": "a.py", "content": "ok"}),),
    )
    assert results[0].error_code == "tool_exception"
    assert sentinel.read_text() == "original"


async def test_protected_limits_survive_behavior_override(
    database: Database, tmp_path: Path
):
    task, run, step, message = await prepared(database, "repair-limits")

    class UnboundedWrite(WriteFileTool):
        async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
            assert context.workspace_authority is not None
            with pytest.raises(ValueError):
                context.workspace_authority.write_text("a.py", "é" * 600_000)
            return ToolOutcome({"content": "x" * 200_000}, "x" * 200_000)

    dispatcher = ToolDispatcher(
        ToolRegistry((UnboundedWrite(),)),
        lambda: SqlAlchemyUnitOfWork(database),
        FixedClock(),
    )
    results = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (CompleteToolCall(new_id(), "write_file", {"path": "a.py", "content": "ok"}),),
    )
    assert results[0].result["truncated"] is True
    assert len(results[0].display_text.encode()) <= 100_000
    assert len(str(results[0].result).encode()) < 110_000
    assert not (tmp_path / "a.py").exists()


@pytest.mark.parametrize(
    "header",
    [
        "--- a/../outside.txt\n+++ b/a.py",
        "--- a/a.py\n+++ b/../outside.txt",
        '--- "a/a.py"\n+++ "b/a.py"',
        "--- nested/a.py\n+++ nested/a.py",
        "--- a/a.py ambiguous\n+++ b/a.py ambiguous",
    ],
)
async def test_patch_admission_checks_old_and_new_paths(tmp_path: Path, header: str):
    from crucible.tools.workspace_authority import patch_paths

    patch = header + "\n@@ -1 +1 @@\n-old\n+new\n"
    with pytest.raises(ValueError):
        patch_paths(patch)
    assert list(tmp_path.iterdir()) == []


async def test_patch_additions_deletions_and_header_like_content(tmp_path: Path):
    from crucible.tools.workspace_authority import patch_paths
    from tests.integration.tools.test_mutation_tools import repository

    workspace = tmp_path / "workspace"
    repository(workspace)
    patch = (
        "--- a/file.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n"
        "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+new\n"
    )
    assert patch_paths(patch) == ("file.txt", "new.txt")
    authority = WorkspaceAuthority(
        workspace, frozenset({"file.txt", "new.txt"}), 1_000_000
    )
    result = await authority.apply_patch(patch)
    assert result.returncode == 0
    assert not (workspace / "file.txt").exists()
    assert (workspace / "new.txt").read_text() == "new\n"
    assert patch_paths(
        "--- a/new.txt\n+++ b/new.txt\n@@ -1 +1 @@\n"
        "--- a/looks-like-header\n+++ b/looks-like-header\n"
    ) == ("new.txt",)


async def test_patch_rechecks_symlinks_after_git_check(tmp_path: Path):
    from crucible.workspaces.git import GitResult, SubprocessGitClient

    target = tmp_path / "a.py"
    target.write_text("old\n")
    outside = tmp_path.parent / f"{tmp_path.name}-sentinel.txt"
    outside.write_text("original")

    class SwapAfterCheck(SubprocessGitClient):
        async def apply_patch(
            self, workspace: Path, patch: str, *, check: bool
        ) -> GitResult:
            assert check, "mutation should never run after path changes"
            target.unlink()
            target.symlink_to(outside)
            return GitResult(0, "", "")

    authority = WorkspaceAuthority(
        tmp_path, frozenset({"a.py"}), 1_000_000, git=SwapAfterCheck()
    )
    with pytest.raises(ValueError, match="symlink"):
        await authority.apply_patch("--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n-old\n+new\n")
    assert outside.read_text() == "original"


def test_registry_keeps_registered_identity_and_maximum_schema():
    from crucible.engine.gateway import ModelToolDefinition
    from crucible.tools.definitions import ToolArguments

    tool = WriteFileTool()
    tool.argument_model = ToolArguments
    tool.definition = ModelToolDefinition("write_file", "editable description", {})
    registry = ToolRegistry((tool,))
    assert (
        registry.definitions[0].input_schema["properties"]["content"]["maxLength"]
        == 1_000_000
    )
    tool.definition = ModelToolDefinition("renamed_write", "description", {})
    with pytest.raises(ValueError, match="identity"):
        ToolRegistry((tool,))


async def test_argument_adapter_cannot_expand_mutation_admission(
    database: Database, tmp_path: Path, monkeypatch
):
    from crucible.tools import behavior_contracts

    task, run, step, message = await prepared(database, "repair-adapter")
    sentinel = tmp_path / "other.py"
    sentinel.write_text("original")
    monkeypatch.setattr(
        behavior_contracts,
        "adapt_arguments",
        lambda name, args: {**args, "path": "other.py"},
    )
    dispatcher = ToolDispatcher(
        ToolRegistry((WriteFileTool(),)),
        lambda: SqlAlchemyUnitOfWork(database),
        FixedClock(),
    )
    result = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (
            CompleteToolCall(
                new_id(), "write_file", {"path": "a.py", "content": "changed"}
            ),
        ),
    )
    assert result[0].error_code == "tool_exception"
    assert sentinel.read_text() == "original"


def test_output_cap_preserves_valid_structured_text_and_bounds_nested_content():
    from crucible.tools.execution_policy import bound_outcome, policy_for

    policy = policy_for(WriteFileTool())
    text = '\\"\n' * 30_000
    original = ToolOutcome({"content": text, "path": "a.py"}, text)
    assert bound_outcome(original, policy) == original
    oversized = bound_outcome(
        ToolOutcome({"nested": {"content": "é" * 100_000}}, "é" * 100_000), policy
    )
    assert oversized.truncated is True
    assert len(oversized.display_text.encode()) <= policy.output_bytes
    assert len(oversized.data["nested"]["content"].encode()) <= policy.output_bytes
