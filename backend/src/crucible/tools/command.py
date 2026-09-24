import base64
import json
from collections.abc import AsyncIterator
from typing import Literal

from pydantic import Field

from crucible.application.artifact_service import ArtifactService
from crucible.application.command_authority import CommandAuthority
from crucible.application.ports import UnitOfWork
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.events import EventType
from crucible.domain.tools import ToolResultStatus
from crucible.engine.gateway import ModelToolDefinition
from crucible.engine.journal import EventSpec, JournalMutation, RunJournal
from crucible.sandbox.output import OutputCapture
from crucible.sandbox.protocol import (
    OutputChunk,
    SandboxBackend,
    SandboxRequest,
    SandboxTermination,
)
from crucible.sandbox.resources import TaskResourceManager
from crucible.tools.definitions import ToolArguments, ToolContext, ToolOutcome
from crucible.tools.filesystem import WorkspacePathResolver


class CommandLimitArguments(ToolArguments):
    cpus: float = Field(default=1, gt=0, le=16)
    memory_bytes: int = Field(
        default=1024**3, gt=0, le=32 * 1024**3, alias="memoryBytes"
    )
    pids: int = Field(default=256, gt=0, le=4096)
    output_bytes: int = Field(default=100_000, gt=0, le=10_000_000, alias="outputBytes")


class ExecuteCommandArguments(ToolArguments):
    executable: str = Field(min_length=1, max_length=1000)
    arguments: list[str] = Field(default_factory=list, max_length=1000)
    cwd: str = "."
    timeout_seconds: int = Field(default=300, ge=1, le=3600, alias="timeoutSeconds")
    network: Literal["none", "outbound"] = "none"
    environment: dict[str, str] = Field(default_factory=dict)
    image: str = Field(min_length=1, max_length=1000)
    reason: str = Field(min_length=1, max_length=2000)
    limits: CommandLimitArguments = Field(default_factory=CommandLimitArguments)


class ExecuteCommandTool:
    parallel_safe = False
    argument_model = ExecuteCommandArguments

    def __init__(
        self,
        authority: CommandAuthority,
        sandbox: SandboxBackend,
        resources: TaskResourceManager,
        artifacts: ArtifactService,
        *,
        live_limit: int = 16_000,
        model_limit: int = 32_000,
        journal: RunJournal | None = None,
    ) -> None:
        self._authority = authority
        self._sandbox = sandbox
        self._resources = resources
        self._artifacts = artifacts
        self._live_limit = live_limit
        self._model_limit = model_limit
        self._journal = journal
        self.definition = ModelToolDefinition(
            "execute_command",
            "Request approval, then execute one structured command in Docker.",
            ExecuteCommandArguments.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        if any(
            value is None
            for value in (
                context.task_id,
                context.run_id,
                context.step_id,
                context.tool_call_id,
            )
        ):
            raise ValueError("execute_command requires durable Tool Call identity")
        assert context.task_id is not None
        assert context.run_id is not None
        assert context.step_id is not None
        assert context.tool_call_id is not None
        args = ExecuteCommandArguments.model_validate(arguments)
        cwd = WorkspacePathResolver(context.workspace).resolve(args.cwd)
        if not cwd.is_dir():
            raise ValueError("Command cwd must be a Workspace directory")
        spec = CommandSpec(
            args.executable,
            tuple(args.arguments),
            args.cwd,
            args.timeout_seconds,
            CommandNetwork(args.network),
            args.environment,
            args.image,
            args.reason,
            CommandLimits(
                args.limits.cpus,
                args.limits.memory_bytes,
                args.limits.pids,
                args.limits.output_bytes,
            ),
        )
        if context.active_time is None:
            approval = await self._authority.request_and_wait(
                context.task_id,
                context.run_id,
                context.step_id,
                context.tool_call_id,
                spec,
            )
        else:
            async with context.active_time.pause():
                approval = await self._authority.request_and_wait(
                    context.task_id,
                    context.run_id,
                    context.step_id,
                    context.tool_call_id,
                    spec,
                )
        if approval.status is not ApprovalStatus.APPROVED:
            return ToolOutcome(
                {
                    "approval_id": str(approval.id),
                    "decision_reason": approval.decision_reason,
                },
                "Command approval denied",
                error_code="approval_denied",
                status=ToolResultStatus.DENIED,
            )

        mounts = await self._resources.verified_mounts(context.task_id)
        capture = OutputCapture(
            live_limit=self._live_limit,
            model_limit=self._model_limit,
            artifact_limit=approval.spec.limits.output_bytes,
        )
        outcome = await self._sandbox.execute(
            SandboxRequest(
                context.task_id,
                context.run_id,
                context.tool_call_id,
                context.workspace,
                approval.spec,
                mounts,
            ),
            lambda chunk: self._accept_chunk(context, capture, chunk),
        )
        artifact = await self._artifacts.put(
            context.task_id,
            "application/x-ndjson",
            "private",
            self._artifact_stream(
                capture.artifact_ndjson(),
                original_bytes=outcome.original_bytes,
                retained_bytes=outcome.retained_bytes,
                truncated=outcome.truncated,
            ),
        )
        status, error_code = self._result_status(outcome.termination, outcome.exit_code)
        metadata = capture.metadata
        return ToolOutcome(
            {
                "approval_id": str(approval.id),
                "artifact_id": str(artifact.id),
                "container_id": outcome.container_id,
                "image_digest": outcome.image_digest,
                "exit_code": outcome.exit_code,
                "termination": outcome.termination.value,
                "original_bytes": outcome.original_bytes,
                "model_retained_bytes": metadata.model.retained_bytes,
                "artifact_retained_bytes": metadata.artifact.retained_bytes,
            },
            capture.model.decode(errors="replace"),
            outcome.truncated or metadata.model.truncated,
            error_code,
            status,
            artifact.id,
        )

    async def _accept_chunk(
        self, context: ToolContext, capture: OutputCapture, chunk: OutputChunk
    ) -> None:
        before = len(capture.live)
        await capture.accept(chunk)
        retained = capture.live[before:]
        if not retained or self._journal is None:
            return
        assert context.task_id is not None and context.run_id is not None

        async def apply(_uow: UnitOfWork) -> None:
            return None

        await self._journal.record(
            JournalMutation(
                context.task_id,
                context.run_id,
                apply,
                (
                    EventSpec(
                        EventType.COMMAND_OUTPUT,
                        {
                            "tool_call_id": str(context.tool_call_id),
                            "sequence": chunk.sequence,
                            "stream": chunk.stream.value,
                            "data_base64": base64.b64encode(retained).decode("ascii"),
                        },
                    ),
                ),
            )
        )

    @staticmethod
    async def _artifact_stream(
        content: bytes,
        *,
        original_bytes: int,
        retained_bytes: int,
        truncated: bool,
    ) -> AsyncIterator[bytes]:
        yield content
        yield (
            json.dumps(
                {
                    "type": "summary",
                    "original_bytes": original_bytes,
                    "retained_bytes": retained_bytes,
                    "truncated": truncated,
                },
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @staticmethod
    def _result_status(
        termination: SandboxTermination, exit_code: int | None
    ) -> tuple[ToolResultStatus, str | None]:
        if termination is SandboxTermination.TIMED_OUT:
            return ToolResultStatus.TIMED_OUT, "command_timed_out"
        if termination is SandboxTermination.CANCELLED:
            return ToolResultStatus.CANCELLED, "cancelled"
        if termination is not SandboxTermination.COMPLETED:
            return ToolResultStatus.FAILED, f"command_{termination.value}"
        if exit_code != 0:
            return ToolResultStatus.FAILED, "command_exit_nonzero"
        return ToolResultStatus.SUCCEEDED, None
