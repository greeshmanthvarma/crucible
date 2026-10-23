import os
from pathlib import Path

from pydantic import ValidationError

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools import behavior_contracts
from crucible.tools.definitions import (
    ListFilesArguments,
    ReadFileArguments,
    SearchFilesArguments,
    ToolContext,
    ToolOutcome,
)
from crucible.tools.search_io import search_workspace
from crucible.tools.workspace_authority import WorkspacePathResolver


class ListFilesTool:
    parallel_safe = True
    argument_model = ListFilesArguments

    def __init__(self, *, max_entries: int = 1000, max_bytes: int = 100_000) -> None:
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "list_files",
            behavior_contracts.DESCRIPTIONS["list_files"],
            ListFilesArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = ListFilesArguments.model_validate(arguments)
        resolver = WorkspacePathResolver(context.workspace)
        root = resolver.resolve(args.path)
        if not root.is_dir():
            raise ValueError("list_files path must be a directory")
        paths: list[str] = []
        size = 0
        truncated = False
        for current, directories, files in os.walk(root, followlinks=False):
            directories[:] = sorted(
                name for name in directories if name not in {".git", ".crucible"}
            )
            for name in sorted(files):
                path = Path(current) / name
                relative = path.relative_to(resolver.root).as_posix()
                if relative == ".git" or relative.startswith(".git/"):
                    continue
                added = len(relative.encode())
                if len(paths) >= self.max_entries or size + added > self.max_bytes:
                    truncated = True
                    break
                paths.append(relative)
                size += added
            if truncated:
                break
        paths.sort()
        return ToolOutcome(
            {"paths": paths, "truncated": truncated},
            "\n".join(paths),
            truncated,
        )


class ReadFileTool:
    parallel_safe = True
    argument_model = ReadFileArguments

    def __init__(self, *, max_bytes: int = 100_000) -> None:
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "read_file",
            behavior_contracts.DESCRIPTIONS["read_file"],
            ReadFileArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = ReadFileArguments.model_validate(arguments)
        path = WorkspacePathResolver(context.workspace).resolve(args.path)
        selected_bytes = bytearray()
        line_number = 1
        with path.open("rb") as source:
            while chunk := source.readline(64 * 1024):
                if b"\x00" in chunk:
                    return ToolOutcome(
                        {"path": args.path, "binary": True},
                        "Binary file",
                        error_code="binary_file",
                    )
                if line_number >= args.start_line and (
                    args.end_line is None or line_number <= args.end_line
                ):
                    remaining = self.max_bytes + 1 - len(selected_bytes)
                    selected_bytes.extend(chunk[:remaining])
                    if len(selected_bytes) > self.max_bytes:
                        break
                if chunk.endswith(b"\n"):
                    line_number += 1
                    if args.end_line is not None and line_number > args.end_line:
                        break
        truncated = len(selected_bytes) > self.max_bytes
        selected = bytes(selected_bytes[: self.max_bytes]).decode(
            "utf-8", errors="ignore"
        )
        return ToolOutcome(
            {
                "path": args.path,
                "content": selected,
                "start_line": args.start_line,
                "truncated": truncated,
            },
            selected,
            truncated,
        )


class SearchFilesTool:
    parallel_safe = True
    argument_model = SearchFilesArguments

    def __init__(self, *, max_matches: int = 200, max_bytes: int = 100_000) -> None:
        self.max_matches = max_matches
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "search_files",
            behavior_contracts.DESCRIPTIONS["search_files"],
            SearchFilesArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = SearchFilesArguments.model_validate(arguments)
        return await search_workspace(
            context.workspace,
            args,
            max_matches=self.max_matches,
            max_bytes=self.max_bytes,
        )


def invalid_arguments(error: ValidationError) -> ToolOutcome:
    return ToolOutcome({}, str(error), error_code="invalid_arguments")
