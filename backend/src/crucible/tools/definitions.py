from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field

from crucible.engine.gateway import ModelToolDefinition


class ToolArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ListFilesArguments(ToolArguments):
    path: str = "."


class SearchFilesArguments(ToolArguments):
    query: str = Field(min_length=1, max_length=500)
    path: str = "."


class ReadFileArguments(ToolArguments):
    path: str
    start_line: int = Field(default=1, ge=1)
    end_line: int | None = Field(default=None, ge=1)


@dataclass(frozen=True)
class ToolContext:
    workspace: Path


@dataclass(frozen=True)
class ToolOutcome:
    data: Mapping[str, object]
    display_text: str
    truncated: bool = False
    error_code: str | None = None


class Tool(Protocol):
    definition: ModelToolDefinition
    parallel_safe: bool

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome: ...
