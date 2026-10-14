from typing import cast

from crucible.engine.gateway import ModelToolDefinition
from crucible.tools.definitions import Tool
from crucible.tools.execution_policy import ToolPolicy, policy_for, protected_definition
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
        if len(self._tools) != len(tools):
            raise ValueError("Duplicate tool identity")
        self._policies = {name: policy_for(tool) for name, tool in self._tools.items()}
        self._definitions = tuple(
            protected_definition(tool, self._policies[name])
            for name, tool in self._tools.items()
        )

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def policy(self, name: str) -> ToolPolicy:
        return self._policies[name]

    @property
    def definitions(self) -> tuple[ModelToolDefinition, ...]:
        return self._definitions


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
