"""Protected registered contracts and final bounds, independent of tool behavior."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools.definitions import (
    ApplyPatchArguments,
    ListFilesArguments,
    ReadFileArguments,
    SearchFilesArguments,
    Tool,
    ToolArguments,
    ToolOutcome,
    WorkspaceDiffArguments,
    WorkspaceStatusArguments,
    WriteFileArguments,
)
from crucible.tools.workspace_authority import (
    WorkspaceAuthority,
    normalize_path,
    patch_paths,
)
from crucible.workspaces.git import GitClient

_MODELS: dict[str, type[ToolArguments]] = {
    "list_files": ListFilesArguments,
    "search_files": SearchFilesArguments,
    "read_file": ReadFileArguments,
    "write_file": WriteFileArguments,
    "apply_patch": ApplyPatchArguments,
    "workspace_status": WorkspaceStatusArguments,
    "workspace_diff": WorkspaceDiffArguments,
}


@dataclass(frozen=True)
class ToolPolicy:
    name: str
    argument_model: type[ToolArguments]
    output_bytes: int = 100_000
    max_write_bytes: int = 1_000_000
    mutation: bool = False
    permitted_role: str = "coding"
    parallel_safe: bool = False
    schema_version: int = 1


def policy_for(tool: Tool) -> ToolPolicy:
    from crucible.tools.command import ExecuteCommandTool
    from crucible.tools.filesystem import ListFilesTool, ReadFileTool, SearchFilesTool
    from crucible.tools.git_tools import (
        ApplyPatchTool,
        WorkspaceDiffTool,
        WorkspaceStatusTool,
        WriteFileTool,
    )

    registered = (
        (ListFilesTool, "list_files"),
        (ReadFileTool, "read_file"),
        (SearchFilesTool, "search_files"),
        (WriteFileTool, "write_file"),
        (ApplyPatchTool, "apply_patch"),
        (WorkspaceDiffTool, "workspace_diff"),
        (WorkspaceStatusTool, "workspace_status"),
        (ExecuteCommandTool, "execute_command"),
    )
    name = next(
        (
            identity
            for implementation, identity in registered
            if isinstance(tool, implementation)
        ),
        tool.definition.name,
    )
    if tool.definition.name != name:
        raise ValueError("Editable tool cannot change its registered identity")
    model = _MODELS.get(name, tool.argument_model)
    if name == "execute_command":
        # Command arguments and execution remain entirely protected.
        from crucible.tools.command import ExecuteCommandArguments

        model = ExecuteCommandArguments
    return ToolPolicy(
        name,
        model,
        output_bytes=200_000 if name == "workspace_diff" else 100_000,
        mutation=name in {"write_file", "apply_patch"},
        parallel_safe=(
            name not in {"write_file", "apply_patch", "execute_command"}
            and tool.parallel_safe
        ),
    )


def protected_definition(tool: Tool, policy: ToolPolicy) -> ModelToolDefinition:
    if tool.definition.schema_version != policy.schema_version:
        raise ValueError("Unsupported tool schema version")
    schema = policy.argument_model.model_json_schema()
    if policy.name == "execute_command":
        schema["properties"].pop("image", None)
        schema["required"] = [name for name in schema["required"] if name != "image"]
    # Editable descriptions cannot replace controller-owned identities/constraints.
    return ModelToolDefinition(
        policy.name, tool.definition.description, schema, policy.schema_version
    )


@dataclass(frozen=True)
class MutationAdmission:
    paths: tuple[str, ...]
    authority: WorkspaceAuthority


def admit_mutation(
    name: str,
    arguments: Mapping[str, object],
    workspace: Path,
    *,
    git: GitClient | None = None,
) -> MutationAdmission:
    if name == "write_file":
        args = WriteFileArguments.model_validate(arguments)
        paths: tuple[str, ...] = (normalize_path(args.path),)
    elif name == "apply_patch":
        patch = ApplyPatchArguments.model_validate(arguments).patch
        paths = () if patch.startswith("*** Begin Patch") else patch_paths(patch)
    else:
        raise ValueError("Tool is not a registered mutation")
    authority = WorkspaceAuthority(workspace, frozenset(paths), 1_000_000, git=git)
    for path in paths:
        authority.validate_target(path)
    return MutationAdmission(paths, authority)


def bound_outcome(outcome: ToolOutcome, policy: ToolPolicy) -> ToolOutcome:
    display = outcome.display_text.encode()
    display_limit = min(100_000, policy.output_bytes)
    truncated = len(display) > display_limit
    bounded_display = display[:display_limit].decode(errors="ignore")
    remaining = policy.output_bytes + 8192  # bounded metadata allowance
    nodes = 10_000

    def bound(value: object) -> object:
        nonlocal remaining, nodes, truncated
        nodes -= 1
        if nodes < 0 or remaining <= 0:
            truncated = True
            return None
        if isinstance(value, str):
            encoded = value.encode()
            limit = min(remaining, policy.output_bytes)
            if len(encoded) > limit:
                truncated = True
            bounded = encoded[:limit].decode(errors="ignore")
            remaining -= len(bounded.encode())
            return bounded
        if isinstance(value, Mapping):
            mapped: dict[str, object] = {}
            for key, item in value.items():
                if nodes <= 0 or remaining <= 0:
                    truncated = True
                    break
                bounded_key = str(bound(str(key)))
                mapped[bounded_key] = bound(item)
            return mapped
        if isinstance(value, (list, tuple)):
            items: list[object] = []
            for item in value:
                if nodes <= 0 or remaining <= 0:
                    truncated = True
                    break
                items.append(bound(item))
            return tuple(items) if isinstance(value, tuple) else items
        return value

    # Bound actual UTF-8 strings, rather than JSON escaping overhead, preserving
    # normal structured results containing newlines/backslashes/non-ASCII text.
    data = bound(dict(outcome.data))
    assert isinstance(data, dict)
    return replace(
        outcome,
        data=data,
        display_text=bounded_display,
        truncated=outcome.truncated or truncated,
    )
