from typing import cast

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools.definitions import Tool
from crucible.tools.filesystem import ListFilesTool, ReadFileTool, SearchFilesTool
from crucible.tools.git_tools import (
    ApplyPatchTool,
    WorkspaceDiffTool,
    WorkspaceStatusTool,
    WriteFileTool,
)


class ToolRegistry:
    def __init__(self, tools: tuple[Tool, ...]) -> None:
        self._tools = {tool.definition.name: tool for tool in tools}

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    @property
    def definitions(self) -> tuple[ModelToolDefinition, ...]:
        return tuple(tool.definition for tool in self._tools.values())


def default_registry(command_tool: Tool | None = None) -> ToolRegistry:
    return ToolRegistry(
        cast(
            tuple[Tool, ...],
            tuple(
                item
                for item in (
                    ListFilesTool(),
                    SearchFilesTool(),
                    ReadFileTool(),
                    WorkspaceStatusTool(),
                    WorkspaceDiffTool(),
                    ApplyPatchTool(),
                    WriteFileTool(),
                    command_tool,
                )
                if item is not None
            ),
        )
    )
