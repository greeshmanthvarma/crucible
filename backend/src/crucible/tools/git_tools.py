from crucible.engine.gateway import ModelToolDefinition
from crucible.tools import behavior_contracts
from crucible.tools.definitions import (
    ApplyPatchArguments,
    ToolContext,
    ToolOutcome,
    WorkspaceDiffArguments,
    WorkspaceStatusArguments,
    WriteFileArguments,
)
from crucible.tools.execution_policy import admit_mutation
from crucible.tools.workspace_authority import PatchApplicationFailed, patch_paths
from crucible.workspaces.git import GitClient, SubprocessGitClient


class WriteFileTool:
    parallel_safe = False
    argument_model = WriteFileArguments

    def __init__(self, *, max_bytes: int = 1_000_000) -> None:
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "write_file",
            behavior_contracts.DESCRIPTIONS["write_file"],
            WriteFileArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = WriteFileArguments.model_validate(arguments)
        encoded = args.content.encode()
        if len(encoded) > self.max_bytes:
            return ToolOutcome({}, "Content exceeds limit", error_code="size_limit")
        authority = (
            context.workspace_authority
            or admit_mutation(
                "write_file", args.model_dump(), context.workspace
            ).authority
        )
        written = authority.write_text(args.path, args.content)
        return ToolOutcome(
            {"path": args.path, "bytes_written": written},
            f"Wrote {written} bytes to {args.path}",
        )


class ApplyPatchTool:
    parallel_safe = False
    argument_model = ApplyPatchArguments

    def __init__(self, git: GitClient | None = None) -> None:
        self._git = git or SubprocessGitClient()
        self.definition = ModelToolDefinition(
            "apply_patch",
            behavior_contracts.DESCRIPTIONS["apply_patch"],
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
        paths = list(patch_paths(args.patch))
        authority = (
            context.workspace_authority
            or admit_mutation(
                "apply_patch", args.model_dump(), context.workspace, git=self._git
            ).authority
        )
        try:
            applied = await authority.apply_patch(args.patch)
        except PatchApplicationFailed as error:
            return ToolOutcome({"paths": paths}, str(error), error_code="patch_failed")
        if applied.returncode != 0:
            return ToolOutcome(
                {"paths": paths}, applied.stderr[:4000], error_code="patch_rejected"
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
            behavior_contracts.DESCRIPTIONS["workspace_status"],
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
            behavior_contracts.DESCRIPTIONS["workspace_diff"],
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


# Compatibility import for existing callers; admission uses the protected parser.
_patch_paths = patch_paths
