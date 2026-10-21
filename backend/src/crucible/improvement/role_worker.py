"""Ordinary role execution confined to disposable workers."""

import asyncio
import codecs
import hashlib
from pathlib import Path
from typing import Literal, cast
from uuid import uuid5

from pydantic import Field

from crucible.application.worker_container import create_worker_engine
from crucible.domain.conversation import MessagePartKind, MessageRole
from crucible.domain.run import RunStatus
from crucible.engine.gateway import (
    ModelError,
    ModelToolDefinition,
    PreparedModelRequest,
)
from crucible.improvement.worker import seed_task
from crucible.improvement.worker_gateway import WorkerModelGateway
from crucible.improvement.worker_protocol import RoleJob, RoleObservation
from crucible.repair.gateway import WorkerCredential
from crucible.repair.identity import canonical_json
from crucible.storage.database import Database
from crucible.storage.models import metadata
from crucible.tools.definitions import Tool, ToolArguments, ToolContext, ToolOutcome
from crucible.tools.filesystem import ListFilesTool, ReadFileTool, SearchFilesTool
from crucible.tools.registry import ToolRegistry, default_registry
from crucible.tools.workspace_authority import WorkspacePathResolver

ROLE_READ_BYTES = 8192


class FileChunkArguments(ToolArguments):
    path: str
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=ROLE_READ_BYTES, ge=4, le=ROLE_READ_BYTES)


class ReadFileChunkTool:
    """Bounded UTF-8 pages, including within a single long JSON line."""

    parallel_safe = True
    argument_model = FileChunkArguments

    def __init__(self) -> None:
        self.definition = ModelToolDefinition(
            "read_file_chunk",
            "Read a UTF-8 workspace file in pages of at most 8192 bytes. "
            "Start at offset=0; use next_offset to continue, null means EOF. "
            "Prefer targeted evidence reads; do not fetch every page by default.",
            self.argument_model.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = FileChunkArguments.model_validate(arguments)
        path = WorkspacePathResolver(context.workspace).resolve(args.path)
        with path.open("rb") as source:
            source.seek(args.offset)
            raw = source.read(args.limit + 1)
        if b"\x00" in raw:
            return ToolOutcome({}, "Binary file", error_code="binary_file")
        decoder = codecs.getincrementaldecoder("utf-8")()
        # Keep an incomplete trailing code point for the next page, not a lossy
        # replacement. An invalid caller-supplied offset fails rather than skips.
        content = decoder.decode(raw[: args.limit], final=len(raw) <= args.limit)
        pending, _ = decoder.getstate()
        consumed = min(len(raw), args.limit) - len(pending)
        truncated = len(raw) > consumed
        next_offset = args.offset + consumed if truncated else None
        return ToolOutcome(
            {
                "path": args.path,
                "offset": args.offset,
                "content": content,
                "next_offset": next_offset,
                "truncated": truncated,
            },
            content,
            truncated,
        )


def role_registry(
    kind: Literal["mining", "format_correction", "proposal"],
    command: Tool | None = None,
) -> ToolRegistry:
    readers = cast(
        tuple[Tool, ...],
        (
            ListFilesTool(max_entries=200, max_bytes=ROLE_READ_BYTES),
            SearchFilesTool(max_matches=40, max_bytes=ROLE_READ_BYTES),
            ReadFileTool(max_bytes=ROLE_READ_BYTES),
            ReadFileChunkTool(),
        ),
    )
    if kind != "proposal":
        return ToolRegistry(readers)
    ordinary = default_registry(command)
    reader_names = {tool.definition.name for tool in readers}
    return ToolRegistry(
        readers
        + tuple(
            tool
            for definition in ordinary.definitions
            if definition.name not in reader_names
            and (tool := ordinary.get(definition.name)) is not None
        )
    )


async def run_role_worker(job: RoleJob, token: str) -> None:
    with Path("/workspace/input.json").open("rb") as stream:
        raw = stream.read(1_048_577)
    if len(raw) > 1_048_576 or hashlib.sha256(raw).hexdigest() != job.input_digest:
        raise ValueError("role input identity drift")
    prompt = (
        "Read input.json and the supplied development index and source. "
        "Evidence supports hypotheses, not causal proof. Return one strict JSON object "
        "as your final assistant text, without markdown fences. "
        "Begin with index.json for trial outcomes and evidence paths. Use "
        "read_file_chunk for bounded evidence and source reads; follow its byte "
        "next_offset only when more detail is needed. A truncated read_file result "
        "is not the whole file; switch to read_file_chunk to inspect further. "
        + (
            "Edit source/ for one supplied weakness. Preserve passing behavior. "
            if job.job_kind == "proposal"
            else "Read only. Do not edit files or run commands. "
        )
        + raw.decode("utf-8")
    )
    gateway = WorkerModelGateway(
        WorkerCredential(token, "http://relay:8080/internal/improvement/model"),
        job.job_id,
    )
    provider_failed = False

    class CapturingGateway:
        async def stream(self, request: PreparedModelRequest):  # type: ignore[no-untyped-def]
            nonlocal provider_failed
            async for event in gateway.stream(request):
                if isinstance(event, ModelError):
                    provider_failed = True
                yield event

    database = await Database.create("sqlite+aiosqlite:////state/worker.sqlite3")
    async with database.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    try:
        engine, uow = create_worker_engine(database, job, CapturingGateway(), token)
        run_id = await seed_task(
            job.job_id, prompt, job.harness_digest, job.command_image, uow
        )
        async with asyncio.timeout(job.wall_deadline_seconds):
            await engine.execute(run_id)
        async with uow() as unit:
            terminal = await unit.runs.get(run_id)
            messages = await unit.messages.list_for_task(uuid5(job.job_id, "task"))
        termination = "infrastructure_error"
        if terminal and terminal.status == RunStatus.COMPLETED:
            termination = "completed"
        elif terminal and terminal.outcome_code == "budget_exhausted":
            termination = "budget_exhausted"
        if provider_failed:
            termination = "provider_error"
        trace = canonical_json(messages)
        assistant = [m for m in messages if m.role == MessageRole.ASSISTANT]
        output = (
            "".join(
                p.text_content or ""
                for p in assistant[-1].parts
                if p.kind == MessagePartKind.TEXT
            )
            if assistant
            else ""
        )
        if (
            len(trace) > job.output_limit_bytes
            or len(output.encode()) > job.output_limit_bytes
        ):
            raise ValueError("role output exceeds cap")
        Path("/state/trace.json").write_bytes(trace)
        Path("/state/assistant.txt").write_text(output)
        observation = RoleObservation.model_validate(
            dict(
                schema_version=2,
                job_id=job.job_id,
                role_id=job.role_id,
                round_id=job.round_id,
                experiment_id=job.experiment_id,
                export_digest=job.export_digest,
                termination=termination,
                assistant_output_ref="assistant.txt",
                trace_ref="trace.json",
                detail_code=terminal.outcome_code if terminal else "missing_run",
            )
        )
        temporary = Path("/state/observation.tmp")
        temporary.write_text(observation.model_dump_json())
        temporary.replace("/state/observation.json")
    finally:
        await database.dispose()
