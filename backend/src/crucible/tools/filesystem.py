import asyncio
import os
from pathlib import Path, PurePosixPath

from pydantic import ValidationError

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools.definitions import (
    ListFilesArguments,
    ReadFileArguments,
    SearchFilesArguments,
    ToolContext,
    ToolOutcome,
)


class WorkspacePathResolver:
    def __init__(self, workspace: Path) -> None:
        self.root = workspace.resolve(strict=True)

    def resolve(self, value: str, *, allow_missing: bool = False) -> Path:
        logical = PurePosixPath(value)
        if logical.is_absolute() or ".." in logical.parts:
            raise ValueError("Workspace path must be relative and cannot traverse")
        candidate = self.root.joinpath(*logical.parts)
        if candidate.is_symlink() and not candidate.exists():
            raise ValueError("Workspace path contains a dangling symlink")
        if candidate.exists():
            resolved = candidate.resolve(strict=True)
            self._require_contained(resolved)
            return resolved
        if not allow_missing:
            raise ValueError(f"Workspace path does not exist: {value}")
        parent = candidate.parent
        missing: list[str] = [candidate.name]
        while not parent.exists():
            if parent.is_symlink():
                raise ValueError("Workspace path contains a dangling symlink")
            missing.append(parent.name)
            parent = parent.parent
        resolved_parent = parent.resolve(strict=True)
        self._require_contained(resolved_parent)
        return resolved_parent.joinpath(*reversed(missing))

    def _require_contained(self, path: Path) -> None:
        if not path.is_relative_to(self.root):
            raise ValueError("Workspace path escapes the Task Workspace")


class ListFilesTool:
    parallel_safe = True
    argument_model = ListFilesArguments

    def __init__(self, *, max_entries: int = 1000, max_bytes: int = 100_000) -> None:
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.definition = ModelToolDefinition(
            "list_files",
            "List stable workspace-relative file paths.",
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
            "Read a bounded UTF-8 workspace file range.",
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
            "Search workspace text with ripgrep.",
            SearchFilesArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = SearchFilesArguments.model_validate(arguments)
        resolver = WorkspacePathResolver(context.workspace)
        target = resolver.resolve(args.path)
        try:
            process = await asyncio.create_subprocess_exec(
                "rg",
                "--line-number",
                "--no-heading",
                "--color=never",
                "--glob=!.git",
                "--",
                args.query,
                str(target),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            return ToolOutcome(
                {}, "ripgrep is unavailable", error_code="tool_unavailable"
            )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
        except TimeoutError:
            process.kill()
            await process.wait()
            return ToolOutcome({}, "Search timed out", error_code="timeout")
        if process.returncode not in (0, 1):
            return ToolOutcome(
                {},
                stderr[:1000].decode(errors="replace"),
                error_code="search_failed",
            )
        raw_lines = stdout.decode(errors="replace").splitlines()
        matches: list[str] = []
        used = 0
        truncated = False
        for line in raw_lines:
            normalized = line.replace(str(resolver.root) + os.sep, "", 1)
            size = len(normalized.encode())
            if len(matches) >= self.max_matches or used + size > self.max_bytes:
                truncated = True
                break
            matches.append(normalized)
            used += size
        return ToolOutcome(
            {"matches": matches, "truncated": truncated},
            "\n".join(matches),
            truncated,
        )


def invalid_arguments(error: ValidationError) -> ToolOutcome:
    return ToolOutcome({}, str(error), error_code="invalid_arguments")
