"""Protected read-only tools and bounded successful-read receipts for workers."""

import hashlib
import os
import stat
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Literal, cast
from uuid import UUID

from pydantic import TypeAdapter

from crucible.domain.tools import ToolResultStatus
from crucible.improvement.mining_contracts import InspectionReceipt
from crucible.improvement.role_worker import ReadFileChunkTool
from crucible.repair.identity import canonical_json
from crucible.tools.definitions import (
    ReadFileArguments,
    Tool,
    ToolContext,
    ToolOutcome,
)
from crucible.tools.execution_policy import bound_outcome, policy_for
from crucible.tools.filesystem import ListFilesTool, ReadFileTool, SearchFilesTool
from crucible.tools.registry import ToolRegistry

RECEIPTS_BYTES = 1_048_576
MAX_INSPECTION_FILE = 2 * 1024 * 1024
DiagnosticKind = Literal["trial_analysis", "mining_synthesis", "format_correction"]


_READ_ROOTS = {
    "trial_analysis": ("trial", "source"),
    "mining_synthesis": ("trials", "source", "reports"),
}


def inspection_path(kind: str, value: str) -> str:
    logical = PurePosixPath(value)
    if (
        not value
        or logical.is_absolute()
        or ".." in logical.parts
        or "\\" in value
        or ":" in value
        or "\x00" in value
    ):
        raise ValueError("diagnostic read outside positive view")
    name = logical.as_posix()
    roots = _READ_ROOTS.get(kind)
    if (
        roots is None
        or (name != "input.json" and logical.parts[0] not in roots)
        or any(part in (".git", ".crucible") for part in logical.parts)
    ):
        raise ValueError("diagnostic read outside positive view")
    return name


def _path(context: ToolContext, kind: str, value: str) -> Path:
    name = inspection_path(kind, value)
    root = context.workspace.resolve(strict=True)
    path = root / name
    if any(p.is_symlink() for p in (path, *path.parents) if p.is_relative_to(root)):
        raise ValueError("diagnostic views cannot contain symlinks")
    if not path.resolve(strict=True).is_relative_to(root):
        raise ValueError("diagnostic read outside positive view")
    return path


def _bytes(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("inspection requires a regular file")
        raw = source.read(MAX_INSPECTION_FILE + 1)
    if len(raw) > MAX_INSPECTION_FILE:
        raise ValueError("inspection file exceeds frozen bounds")
    return raw


def line_span(
    raw: bytes, start_line: int, end_line: int | None = None
) -> tuple[int, int]:
    """Line boundaries use LF, matching the ordinary read_file reader."""
    ends = []
    cursor = 0
    while (offset := raw.find(b"\n", cursor)) >= 0:
        cursor = offset + 1
        ends.append(cursor)
    if cursor < len(raw):
        ends.append(len(raw))
    if start_line < 1 or start_line > len(ends):
        return len(raw), len(raw)
    start = ends[start_line - 2] if start_line > 1 else 0
    if end_line is not None and end_line < start_line:
        return start, start
    return start, ends[min(end_line or len(ends), len(ends)) - 1]


class AuditedReadTool:
    def __init__(
        self, tool: Tool, kind: str, receipts: list[InspectionReceipt], job_id: UUID
    ) -> None:
        self.tool, self.kind, self.receipts, self.job_id = tool, kind, receipts, job_id
        self.definition, self.argument_model = tool.definition, tool.argument_model
        self.parallel_safe = tool.parallel_safe
        self.policy = replace(policy_for(tool), output_bytes=8192)

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = self.argument_model.model_validate(arguments)
        value = str(getattr(args, "path"))
        if value in (".", "") and self.definition.name == "list_files":
            roots = _READ_ROOTS[self.kind]
            names = [
                n for n in ("input.json", *roots) if (context.workspace / n).exists()
            ]
            return ToolOutcome({"paths": names, "truncated": False}, "\n".join(names))
        path = _path(context, self.kind, value)
        if self.definition.name not in ("read_file", "read_file_chunk"):
            return bound_outcome(
                await self.tool.invoke(context, arguments), self.policy
            )
        before = _bytes(path)
        # Apply protected bounds before crediting bytes. The dispatcher may bound
        # again, but cannot then remove any of this already bounded read output.
        outcome = bound_outcome(await self.tool.invoke(context, arguments), self.policy)
        if outcome.error_code is not None or outcome.status not in (
            None,
            ToolResultStatus.SUCCEEDED,
        ):
            return outcome
        after = _bytes(_path(context, self.kind, value))
        if before != after:
            raise ValueError("inspection file changed during read")
        content = outcome.data.get("content")
        if not isinstance(content, str):
            raise ValueError("read tool omitted returned content")
        start = (
            line_span(before, args.start_line, args.end_line)[0]
            if isinstance(args, ReadFileArguments)
            else int(getattr(args, "offset"))
        )
        returned = content.encode("utf-8")
        # The ordinary line reader ignores invalid UTF-8. Such a noncontiguous
        # rendering cannot prove inspection of a byte interval; issue no receipt.
        if (
            returned
            and content == outcome.display_text
            and before[start : start + len(returned)] == returned
        ):
            if context.tool_call_id is None:
                raise ValueError("inspection requires an admitted tool call identity")
            receipt = InspectionReceipt(
                job_id=self.job_id,
                path=inspection_path(self.kind, value),
                sha256=hashlib.sha256(before).hexdigest(),
                start_byte=start,
                end_byte=start + len(returned),
                tool_call_id=context.tool_call_id,
            )
            if len(canonical_json((*self.receipts, receipt))) > RECEIPTS_BYTES:
                raise ValueError("inspection receipts exceed cap")
            self.receipts.append(receipt)
        return outcome


def audited_role_registry(
    kind: str, receipts: list[InspectionReceipt], job_id: UUID
) -> ToolRegistry:
    if kind == "format_correction":
        return ToolRegistry(())
    if kind not in ("trial_analysis", "mining_synthesis"):
        raise ValueError("unknown diagnostic role")
    readers = cast(
        tuple[Tool, ...],
        (
            ListFilesTool(max_entries=200, max_bytes=8192),
            SearchFilesTool(max_matches=40, max_bytes=8192),
            ReadFileTool(max_bytes=8192),
            ReadFileChunkTool(),
        ),
    )
    return ToolRegistry(
        tuple(AuditedReadTool(tool, kind, receipts, job_id) for tool in readers)
    )


def read_inspection_receipts(
    raw: bytes, *, job_id: UUID, expected_sha256: str, kind: str
) -> tuple[InspectionReceipt, ...]:
    if kind not in (*_READ_ROOTS, "format_correction"):
        raise ValueError("unknown diagnostic receipt role")
    if len(raw) > RECEIPTS_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("inspection receipt artifact limit or digest mismatch")
    receipts = TypeAdapter(tuple[InspectionReceipt, ...]).validate_json(
        raw, strict=True
    )
    if kind == "format_correction" and receipts:
        raise ValueError("format correction cannot produce inspection receipts")
    for receipt in receipts:
        if receipt.job_id != job_id:
            raise ValueError("inspection receipt job identity mismatch")
        inspection_path(kind, receipt.path)
    return receipts
