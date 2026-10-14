"""Bounded private evidence. Recording failures never authorize or block edits."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import stat
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from crucible.domain.artifacts import Artifact
from crucible.domain.clock import Clock
from crucible.domain.ids import new_id
from crucible.engine.gateway import PreparedModelRequest
from crucible.repair.identity import RuntimeIdentity, canonical_json, digest
from crucible.tools.execution_policy import MutationAdmission

if TYPE_CHECKING:
    from crucible.application.ports import UnitOfWork

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CaptureLimits:
    file_bytes: int = 1_048_576
    call_bytes: int = 8_388_608
    paths: int = 32
    compaction_bytes: int = 8_388_608


@dataclass(frozen=True)
class EvidenceCapture:
    id: UUID
    task_id: UUID
    run_id: UUID
    kind: str
    artifact_id: UUID | None
    complete: bool
    unsupported_code: str | None
    source_ids: Mapping[str, str]
    manifest_digest: str
    created_at: datetime
    source_key: str
    attempt_index: int = 0
    step_id: UUID | None = None
    tool_call_id: UUID | None = None

    def __post_init__(self) -> None:
        if self.complete and (
            self.artifact_id is None or self.unsupported_code is not None
        ):
            raise ValueError("Complete evidence requires an unaltered artifact")
        if self.attempt_index < 0:
            raise ValueError("Evidence attempt index cannot be negative")
        object.__setattr__(self, "source_ids", MappingProxyType(dict(self.source_ids)))


class EvidenceArtifacts(Protocol):
    async def put(
        self,
        task_id: UUID,
        media_type: str,
        sensitivity: str,
        stream: AsyncIterable[bytes],
        *,
        hard_limit: int | None = None,
    ) -> Artifact: ...
    async def read(self, artifact_id: UUID) -> tuple[Artifact, bytes]: ...


class EvidenceRecorder:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        artifacts: EvidenceArtifacts,
        clock: Clock,
        *,
        limits: CaptureLimits = CaptureLimits(),
        secret_values: tuple[str, ...] = (),
        runtime_identity: RuntimeIdentity | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._artifacts = artifacts
        self._clock = clock
        self.limits = limits
        self._secrets = tuple(
            sorted((value for value in secret_values if value), key=len, reverse=True)
        )
        self.runtime_identity = runtime_identity

    def _redact(self, value: object) -> tuple[object, bool]:
        altered = False

        def redact(item: object) -> object:
            nonlocal altered
            if isinstance(item, str):
                for secret in self._secrets:
                    if secret in item:
                        item = item.replace(secret, "[REDACTED]")
                        altered = True
                return item
            if isinstance(item, Mapping):
                return {str(redact(key)): redact(child) for key, child in item.items()}
            if isinstance(item, (tuple, list)):
                return [redact(child) for child in item]
            return item

        return redact(value), altered

    async def _authorize(
        self,
        task_id: UUID,
        run_id: UUID,
        *,
        call_id: UUID | None = None,
        step_id: UUID | None = None,
    ) -> None:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None or run.task_id != task_id:
                raise ValueError("Evidence Run does not belong to Task")
            steps = await uow.steps.list_for_run(run_id)
            if step_id is not None and step_id not in {step.id for step in steps}:
                raise ValueError("Evidence Step does not belong to Run")
            if call_id is not None:
                calls = [
                    call
                    for step in steps
                    for call in await uow.tool_calls.list_for_step(step.id)
                ]
                if call_id not in {call.id for call in calls}:
                    raise ValueError("Evidence Tool Call does not belong to Run")

    async def _existing(
        self,
        task_id: UUID,
        run_id: UUID,
        kind: str,
        source_key: str,
        attempt_index: int,
    ) -> EvidenceCapture | None:
        async with self._unit_of_work() as uow:
            return await uow.repair_evidence.get_by_source(
                run_id, kind, source_key, attempt_index, task_id=task_id
            )

    async def _save(
        self,
        task_id: UUID,
        run_id: UUID,
        kind: str,
        source_key: str,
        attempt_index: int,
        payload: object,
        *,
        complete: bool,
        code: str | None = None,
        step_id: UUID | None = None,
        call_id: UUID | None = None,
    ) -> EvidenceCapture:
        source_ids = {
            "task_id": str(task_id),
            "run_id": str(run_id),
            "source_key": source_key,
        }
        if step_id is not None:
            source_ids["step_id"] = str(step_id)
        if call_id is not None:
            source_ids["tool_call_id"] = str(call_id)
        safe, altered = self._redact(json.loads(canonical_json(payload)))
        if altered:
            complete, code = False, "secret_redacted"
        body = (
            {
                "schema_version": 1,
                "kind": kind,
                "source_ids": source_ids,
                "attempt_index": attempt_index,
                "complete": complete,
                "unsupported_code": code,
                "capture_limits": asdict(self.limits),
                "runtime_identity": asdict(self.runtime_identity)
                if self.runtime_identity
                else None,
                **dict(safe),
            }
            if isinstance(safe, Mapping)
            else {"value": safe}
        )
        manifest_digest = digest(body)
        artifact_id = None
        try:
            encoded = canonical_json(body)

            async def chunks() -> AsyncIterator[bytes]:
                yield encoded

            artifact = await self._artifacts.put(
                task_id,
                "application/json",
                "private",
                chunks(),
                hard_limit=len(encoded),
            )
            if (
                artifact.metadata.get("truncated")
                or artifact.byte_length != len(encoded)
                or artifact.content_hash != manifest_digest
            ):
                complete, code = False, "artifact_incomplete"
            else:
                artifact_id = artifact.id
        except Exception:
            complete, code = False, "artifact_write_failed"
            logger.warning(
                "Private evidence artifact capture failed",
                extra={"run_id": str(run_id), "kind": kind},
            )
        record = EvidenceCapture(
            new_id(),
            task_id,
            run_id,
            kind,
            artifact_id,
            complete,
            code,
            source_ids,
            manifest_digest,
            self._clock.now(),
            source_key,
            attempt_index,
            step_id,
            call_id,
        )
        try:
            async with self._unit_of_work() as uow:
                record = await uow.repair_evidence.add(record)
                await uow.commit()
            return record
        except Exception:
            logger.warning(
                "Private evidence metadata persistence failed",
                extra={"run_id": str(run_id), "kind": kind},
            )
            return replace(
                record, complete=False, unsupported_code="evidence_persistence_failed"
            )

    def _file(
        self, admission: MutationAdmission, path: str, remaining: int
    ) -> tuple[dict[str, object], int, str | None]:
        item: dict[str, object] = {"path": path, "exists": True}
        code: str | None = None
        try:
            target = admission.authority.validate_target(path)
        except ValueError as error:
            code = "symlink" if "symlink" in str(error) else "invalid_path"
            item["unsupported_code"] = code
            return item, 0, code
        try:
            descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            item["exists"] = False
            return item, 0, None
        except OSError:
            item["unsupported_code"] = "unreadable_file"
            return item, 0, "unreadable_file"
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode):
                item["unsupported_code"] = "not_regular_file"
                return item, 0, "not_regular_file"
            if target.name == ".env" or target.name.startswith(".env."):
                item["unsupported_code"] = "secret_redacted"
                return item, 0, "secret_redacted"
            raw = source.read(min(self.limits.file_bytes, remaining) + 1)
            after = os.fstat(source.fileno())
        code = None
        if before.st_size > self.limits.file_bytes or len(raw) > self.limits.file_bytes:
            code = "file_size_limit"
        elif before.st_size > remaining or len(raw) > remaining:
            code = "call_size_limit"
        elif (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            code = "file_changed_during_capture"
        elif b"\0" in raw:
            code = "binary_file"
        else:
            try:
                item["content"] = raw.decode("utf-8")
            except UnicodeDecodeError:
                code = "binary_file"
        if code:
            item["unsupported_code"] = code
            return item, 0, code
        item["content_digest"] = hashlib.sha256(raw).hexdigest()
        item["byte_length"] = len(raw)
        return item, len(raw), None

    async def capture_preedit(
        self, task_id: UUID, run_id: UUID, call_id: UUID, admission: MutationAdmission
    ) -> EvidenceCapture:
        await self._authorize(task_id, run_id, call_id=call_id)
        existing = await self._existing(task_id, run_id, "preedit", str(call_id), 0)
        if existing is not None:
            return existing
        files: list[dict[str, object]] = []
        remaining = self.limits.call_bytes
        code = "path_count_limit" if len(admission.paths) > self.limits.paths else None
        if code is None:
            for path in admission.paths:
                try:
                    item, size, unsupported = await asyncio.to_thread(
                        self._file, admission, path, remaining
                    )
                except OSError:
                    item, size, unsupported = (
                        {"path": path, "unsupported_code": "unreadable_file"},
                        0,
                        "unreadable_file",
                    )
                files.append(item)
                remaining -= size
                code = code or unsupported
        return await self._save(
            task_id,
            run_id,
            "preedit",
            str(call_id),
            0,
            {"files": files, "target_count": len(admission.paths)},
            complete=code is None,
            code=code,
            call_id=call_id,
        )

    async def capture_request(
        self,
        task_id: UUID,
        run_id: UUID,
        attempt_index: int,
        request: PreparedModelRequest,
        *,
        kind: str = "compaction_request",
        source_key: str | None = None,
        step_id: UUID | None = None,
        context: Mapping[str, object] | None = None,
    ) -> EvidenceCapture:
        await self._authorize(task_id, run_id, step_id=step_id)
        key = source_key or str(request.step_id)
        existing = await self._existing(task_id, run_id, kind, key, attempt_index)
        if existing is not None:
            return existing
        normalized = {
            "run_id": str(request.run_id),
            "step_id": str(request.step_id),
            "model": request.model,
            "max_output_tokens": request.max_output_tokens,
            "messages": [
                {
                    "role": message.role.value,
                    "parts": [
                        {
                            "kind": part.kind,
                            "text_content": part.text_content,
                            "tool_call_id": str(part.tool_call_id)
                            if part.tool_call_id
                            else None,
                            "tool_name": part.tool_name,
                            "arguments": dict(part.arguments)
                            if part.arguments is not None
                            else None,
                            "provider_correlation_id": part.provider_correlation_id,
                        }
                        for part in message.parts
                    ],
                }
                for message in request.messages
            ],
            "tools": [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": dict(tool.input_schema),
                    "schema_version": tool.schema_version,
                }
                for tool in request.tools
            ],
        }
        payload: dict[str, object] = {
            "request": normalized,
            "context": dict(context or {}),
        }
        oversized = len(canonical_json(payload)) > self.limits.compaction_bytes
        if oversized:
            payload = {"request_digest": digest(payload)}
        return await self._save(
            task_id,
            run_id,
            kind,
            key,
            attempt_index,
            payload,
            complete=not oversized,
            code="compaction_size_limit" if oversized else None,
            step_id=step_id,
        )

    async def capture_compaction(
        self,
        task_id: UUID,
        run_id: UUID,
        attempt_index: int,
        request: PreparedModelRequest,
        raw_text: str,
        complete: bool,
        *,
        source_key: str | None = None,
        step_id: UUID | None = None,
    ) -> EvidenceCapture:
        await self._authorize(task_id, run_id, step_id=step_id)
        key = source_key or str(request.step_id)
        existing = await self._existing(
            task_id, run_id, "compaction_response", key, attempt_index
        )
        if existing is not None:
            return existing
        oversized = len(raw_text.encode()) > self.limits.compaction_bytes
        payload = (
            {
                "raw_digest": hashlib.sha256(raw_text.encode()).hexdigest(),
                "raw_text": raw_text,
            }
            if not oversized
            else {"raw_digest": hashlib.sha256(raw_text.encode()).hexdigest()}
        )
        code = (
            "compaction_size_limit"
            if oversized
            else None
            if complete
            else "partial_response"
        )
        return await self._save(
            task_id,
            run_id,
            "compaction_response",
            key,
            attempt_index,
            payload,
            complete=complete and not oversized,
            code=code,
            step_id=step_id,
        )

    async def read(
        self, evidence_id: UUID, *, task_id: UUID
    ) -> tuple[EvidenceCapture, bytes]:
        async with self._unit_of_work() as uow:
            capture = await uow.repair_evidence.get(evidence_id, task_id=task_id)
        if capture is None or capture.artifact_id is None:
            raise ValueError("Evidence is not available for Task")
        artifact, raw = await self._artifacts.read(capture.artifact_id)
        if artifact.task_id != task_id:
            raise ValueError("Evidence artifact does not belong to Task")
        if hashlib.sha256(raw).hexdigest() != capture.manifest_digest:
            raise ValueError("Evidence artifact digest does not match its manifest")
        return capture, raw
