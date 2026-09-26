import os
import tempfile
from pathlib import Path

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools.definitions import (
    ApplyPatchArguments,
    ToolContext,
    ToolOutcome,
    WorkspaceDiffArguments,
    WorkspaceStatusArguments,
    WriteFileArguments,
)
from crucible.tools.filesystem import WorkspacePathResolver
from crucible.workspaces.git import GitClient, SubprocessGitClient


class WriteFileTool:
    parallel_safe = False
    argument_model = WriteFileArguments

    def __init__(self, *, max_bytes: int = 1_000_000) -> None:
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "write_file",
            "Atomically write complete UTF-8 content inside the workspace.",
            WriteFileArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = WriteFileArguments.model_validate(arguments)
        encoded = args.content.encode()
        if len(encoded) > self.max_bytes:
            return ToolOutcome({}, "Content exceeds limit", error_code="size_limit")
        resolver = WorkspacePathResolver(context.workspace)
        logical = context.workspace / args.path
        if logical.is_symlink():
            raise ValueError("write_file rejects symlink targets")
        target = resolver.resolve(args.path, allow_missing=True)
        parent = target.parent.resolve(strict=True)
        if not parent.is_relative_to(resolver.root):
            raise ValueError("Write parent escapes the Task Workspace")
        file_descriptor, temporary_name = tempfile.mkstemp(
            dir=parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(file_descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            if target.parent.resolve(strict=True) != parent:
                raise ValueError("Write parent changed during operation")
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return ToolOutcome(
            {"path": args.path, "bytes_written": len(encoded)},
            f"Wrote {len(encoded)} bytes to {args.path}",
        )


class ApplyPatchTool:
    parallel_safe = False
    argument_model = ApplyPatchArguments

    def __init__(self, git: GitClient | None = None) -> None:
        self._git = git or SubprocessGitClient()
        self.definition = ModelToolDefinition(
            "apply_patch",
            "Apply a standard unified diff inside the workspace. Include --- a/path, "
            "+++ b/path, and numbered @@ -old +new @@ hunk headers. "
            "The *** Begin Patch / *** Update File format is not supported.",
            ApplyPatchArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = ApplyPatchArguments.model_validate(arguments)
        if args.patch.startswith("*** Begin Patch"):
            return ToolOutcome(
                {},
                "This tool requires a standard unified diff with headers such as "
                "--- a/file.txt and +++ b/file.txt and numbered @@ hunks; "
                "*** Begin Patch format is unsupported.",
                error_code="patch_format_unsupported",
            )
        resolver = WorkspacePathResolver(context.workspace)
        paths = _patch_paths(args.patch)
        for path in paths:
            resolver.resolve(path, allow_missing=True)
        checked = await self._git.apply_patch(context.workspace, args.patch, check=True)
        if checked.returncode != 0:
            return ToolOutcome(
                {"paths": paths},
                checked.stderr[:4000],
                error_code="patch_rejected",
            )
        applied = await self._git.apply_patch(
            context.workspace, args.patch, check=False
        )
        if applied.returncode != 0:
            return ToolOutcome(
                {"paths": paths},
                applied.stderr[:4000],
                error_code="patch_failed",
            )
        return ToolOutcome({"paths": paths}, f"Changed {len(paths)} path(s)")


class WorkspaceStatusTool:
    parallel_safe = True
    argument_model = WorkspaceStatusArguments

    def __init__(
        self, git: GitClient | None = None, *, max_bytes: int = 100_000
    ) -> None:
        self._git = git or SubprocessGitClient()
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "workspace_status",
            "Inspect Git workspace status.",
            WorkspaceStatusArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        WorkspaceStatusArguments.model_validate(arguments)
        result = await self._git.status(context.workspace)
        return _bounded_git_result(result.stdout, result.stderr, self.max_bytes)


class WorkspaceDiffTool:
    parallel_safe = True
    argument_model = WorkspaceDiffArguments

    def __init__(
        self, git: GitClient | None = None, *, max_bytes: int = 200_000
    ) -> None:
        self._git = git or SubprocessGitClient()
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "workspace_diff",
            "Inspect the current Git workspace diff.",
            WorkspaceDiffArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        WorkspaceDiffArguments.model_validate(arguments)
        result = await self._git.diff(context.workspace)
        return _bounded_git_result(result.stdout, result.stderr, self.max_bytes)


def _bounded_git_result(stdout: str, stderr: str, limit: int) -> ToolOutcome:
    encoded = stdout.encode()
    truncated = len(encoded) > limit
    bounded = encoded[:limit].decode(errors="replace") if truncated else stdout
    code = "git_error" if stderr else None
    return ToolOutcome(
        {"output": bounded, "truncated": truncated},
        bounded or stderr[:limit],
        truncated,
        code,
    )


def _patch_paths(patch: str) -> list[str]:
    paths: list[str] = []
    for line in patch.splitlines():
        if not line.startswith(("--- ", "+++ ")):
            continue
        value = line[4:].split("\t", 1)[0]
        if value == "/dev/null":
            continue
        if value.startswith(("a/", "b/")):
            value = value[2:]
        if value not in paths:
            paths.append(value)
    if not paths:
        raise ValueError("Patch does not contain file headers")
    return paths
