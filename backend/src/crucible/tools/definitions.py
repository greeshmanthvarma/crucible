from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field

from crucible.domain.ids import ArtifactId, RunId, StepId, TaskId, ToolCallId
from crucible.domain.tools import ToolResultStatus
from crucible.engine.active_time import ActiveTimeBudget
from crucible.engine.gateway import ModelToolDefinition


class ToolArguments(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ListFilesArguments(ToolArguments):
    path: str = "."


class SearchFilesArguments(ToolArguments):
    query: str = Field(min_length=1, max_length=500)
    path: str = "."


class ReadFileArguments(ToolArguments):
    path: str
    start_line: int = Field(default=1, ge=1)
    end_line: int | None = Field(default=None, ge=1)


class WriteFileArguments(ToolArguments):
    path: str
    content: str = Field(max_length=1_000_000)


class ApplyPatchArguments(ToolArguments):
    patch: str = Field(min_length=1, max_length=1_000_000)


class WorkspaceStatusArguments(ToolArguments):
    pass


class WorkspaceDiffArguments(ToolArguments):
    pass


@dataclass(frozen=True)
class ToolContext:
    workspace: Path
    task_id: TaskId | None = None
    run_id: RunId | None = None
    step_id: StepId | None = None
    tool_call_id: ToolCallId | None = None
    active_time: ActiveTimeBudget | None = None
    sandbox_image: str | None = None


@dataclass(frozen=True)
class ToolOutcome:
    data: Mapping[str, object]
    display_text: str
    truncated: bool = False
    error_code: str | None = None
    status: ToolResultStatus | None = None
    artifact_id: ArtifactId | None = None


class Tool(Protocol):
    definition: ModelToolDefinition
    parallel_safe: bool
    argument_model: type[ToolArguments]

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome: ...
