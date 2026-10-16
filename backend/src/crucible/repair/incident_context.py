"""Source-chat-only, bounded read access; never a general artifact browser."""

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import UnitOfWork
from crucible.repair.identity import canonical_json


@dataclass(frozen=True)
class IncidentIndex:
    source_task_id: UUID
    runs: tuple[dict[str, Any], ...]
    message_count: int


class IncidentContext:
    def __init__(
        self, unit_of_work: Callable[[], UnitOfWork], artifacts: ArtifactService
    ) -> None:
        self.factory, self.artifacts = unit_of_work, artifacts
        self._secrets = tuple(
            value
            for name, value in os.environ.items()
            if value and name.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD"))
        )

    def safe(self, value: Any) -> Any:
        if isinstance(value, str):
            for secret in self._secrets:
                value = value.replace(secret, "[redacted]")
            return value
        if isinstance(value, dict):
            return {key: self.safe(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [self.safe(item) for item in value]
        return value

    async def _source(self, repair_id: UUID) -> UUID:
        async with self.factory() as uow:
            repair = await uow.repairs.get(repair_id)
        if repair is None:
            raise ValueError("Repair is unavailable")
        return repair.source_task_id

    async def index(self, source_task_id: UUID) -> IncidentIndex:
        async with self.factory() as uow:
            task = await uow.tasks.get(source_task_id)
            if task is None or task.internal_repair_id is not None:
                raise ValueError("Source chat is unavailable")
            runs = await uow.runs.list_for_task(source_task_id)
            records = []
            for run in runs:
                captures = await uow.repair_evidence.list_for_run(
                    run.id, task_id=source_task_id
                )
                step_index = []
                for step in await uow.steps.list_for_run(run.id):
                    results = await uow.tool_results.list_for_step(step.id)
                    calls = await uow.tool_calls.list_for_step(step.id)
                    step_index.append(
                        {
                            "step_id": str(step.id),
                            "calls": [
                                {
                                    "tool_call_id": str(call.id),
                                    "name": call.name,
                                    "status": call.status,
                                    "errors": [
                                        result.error_code
                                        for result in results
                                        if result.tool_call_id == call.id
                                        and result.error_code
                                    ],
                                }
                                for call in calls
                            ],
                        }
                    )
                records.append(
                    self.safe(
                        {
                            "run_id": str(run.id),
                            "status": run.status,
                            "outcome_code": run.outcome_code,
                            "detail": (run.outcome_detail or "")[:1000],
                            "steps": step_index,
                            "evidence": [
                                {
                                    "id": str(c.id),
                                    "artifact_id": str(c.artifact_id)
                                    if c.artifact_id
                                    else None,
                                    "kind": c.kind,
                                    "complete": c.complete,
                                    "unsupported_code": c.unsupported_code,
                                }
                                for c in captures
                            ],
                        }
                    )
                )
            messages = await uow.messages.list_for_task(source_task_id)
        return IncidentIndex(source_task_id, tuple(records), len(messages))

    async def read_evidence(self, repair_id: UUID, evidence_id: UUID) -> dict[str, Any]:
        task_id = await self._source(repair_id)
        async with self.factory() as uow:
            capture = await uow.repair_evidence.get(evidence_id, task_id=task_id)
        if capture is None or capture.artifact_id is None:
            raise ValueError("Evidence is not available for this repair")
        metadata, raw = await self.artifacts.read(capture.artifact_id)
        if (
            metadata.task_id != task_id
            or metadata.sensitivity != "private"
            or hashlib.sha256(raw).hexdigest() != capture.manifest_digest
            or len(raw) > 8 * 1024 * 1024
        ):
            raise ValueError("Evidence integrity check failed")
        value = self.safe(json.loads(raw))
        if not isinstance(value, dict):
            raise ValueError("Invalid evidence envelope")
        return value

    async def read_messages(
        self, repair_id: UUID, start: int = 0, limit: int = 10
    ) -> dict[str, Any]:
        if start < 0 or not 1 <= limit <= 50:
            raise ValueError("Invalid message range")
        task_id = await self._source(repair_id)
        async with self.factory() as uow:
            messages = await uow.messages.list_for_task(task_id)
        selected: list[dict[str, Any]] = []
        for message in messages[start : start + limit]:
            item = self.safe(json.loads(canonical_json(message)))
            # Navigation is deliberately not an exact replay input. Keep stable
            # part/call/result IDs; full bodies are available through read_detail.
            parts = item.pop("parts")
            item["parts"] = []
            for part in parts:
                text = (
                    part.pop("text_content", None)
                    or part.pop("reasoning_content", None)
                    or ""
                )
                part.pop("reasoning_content", None)
                part.update(preview=text[:512], truncated=len(text) > 512)
                candidate = {**item, "parts": [*item["parts"], part]}
                if len(canonical_json({"messages": [*selected, candidate]})) > 15000:
                    # Even a message with many parts remains navigable. Page its
                    # full representation using the message detail identity.
                    item["parts_truncated"] = True
                    break
                item["parts"].append(part)
            if len(canonical_json({"messages": [*selected, item]})) > 15000:
                break
            selected.append(item)
        return {
            "messages": selected,
            "next": start + len(selected),
            "total": len(messages),
            "content_mode": "preview; use read_incident_detail for full source text",
        }

    async def read_detail(
        self,
        repair_id: UUID,
        kind: Literal[
            "message", "message_part", "tool_call", "tool_result", "evidence"
        ],
        identity: UUID,
        offset: int = 0,
        limit: int = 4000,
    ) -> dict[str, Any]:
        """Character-paged original records, authorized through source ownership."""
        if offset < 0 or not 1 <= limit <= 4000:
            raise ValueError("Invalid detail range")
        task_id = await self._source(repair_id)
        value: Any = (
            await self.read_evidence(repair_id, identity)
            if kind == "evidence"
            else None
        )
        async with self.factory() as uow:
            messages = await uow.messages.list_for_task(task_id)
            if kind in {"message", "message_part"}:
                for message in messages:
                    if kind == "message" and message.id == identity:
                        value = json.loads(canonical_json(message))
                    elif kind == "message_part":
                        for part in message.parts:
                            if part.id == identity:
                                value = (
                                    part.text_content or part.reasoning_content or ""
                                )
            elif kind in {"tool_call", "tool_result"}:
                for run in await uow.runs.list_for_task(task_id):
                    for step in await uow.steps.list_for_run(run.id):
                        records = (
                            await uow.tool_calls.list_for_step(step.id)
                            if kind == "tool_call"
                            else await uow.tool_results.list_for_step(step.id)
                        )
                        for record in records:
                            if record.id == identity and record.task_id == task_id:
                                value = json.loads(canonical_json(record))
        if value is None:
            raise ValueError("Detail is not available for this repair")
        value = self.safe(value)
        text = value if isinstance(value, str) else canonical_json(value).decode()
        if offset > len(text):
            raise ValueError("Detail offset exceeds content")
        end = min(len(text), offset + limit)
        return {
            "kind": kind,
            "id": str(identity),
            "text": text[offset:end],
            "next": end if end < len(text) else None,
            "total_characters": len(text),
        }

    async def replay_settings(self, repair_id: UUID, case: Any) -> dict[str, Any]:
        if case.kind == "tool":
            settings = dict(case.settings)
            if case.tool_name == "write_file":
                from crucible.tools.workspace_authority import normalize_path

                settings["admitted_mutation_paths"] = [
                    normalize_path(str(case.arguments.get("path", "")))
                ]
            elif case.tool_name == "apply_patch":
                from crucible.tools.workspace_authority import patch_paths

                settings["admitted_mutation_paths"] = list(
                    patch_paths(str(case.arguments.get("patch", "")))
                )
            return settings
        references = set(case.source_artifact_ids)
        if not references:
            raise ValueError("Context replay requires captured source inputs")
        if case.kind == "compaction":
            references.add(case.request_artifact_id)
        index = await self.index(await self._source(repair_id))
        captures = []
        for run in index.runs:
            for record in run["evidence"]:
                if record["artifact_id"] and UUID(record["artifact_id"]) in references:
                    if not record["complete"]:
                        raise ValueError("Context capture is incomplete")
                    captures.append(
                        await self.read_evidence(repair_id, UUID(record["id"]))
                    )
        selected = next(
            (
                record
                for record in captures
                if isinstance(record.get("context"), dict)
                and "input_limit" in record["context"]
                and isinstance(record.get("request"), dict)
            ),
            None,
        )
        if selected is None:
            raise ValueError("Captured context settings are missing")
        context, request = selected["context"], selected["request"]
        messages = request.get("messages", [])
        systems = [
            "\n".join(part.get("text_content") or "" for part in message["parts"])
            for message in messages
            if message.get("role") == "system"
        ]
        if len(systems) < 2:
            raise ValueError("Captured instruction inputs are missing")
        settings = {
            key: context[key]
            for key in ("input_limit", "output_reserve", "threshold")
            if key in context
        }
        settings.update(
            harness_policy=systems[0],
            tool_contract=systems[1],
            source_messages=[
                {**message, "conversation_sequence": number}
                for number, message in enumerate(messages, 1)
            ],
            source_order_basis="captured_model_request_projection",
        )
        # The diagnosis may select a smaller complete compaction prefix, but it
        # cannot invent messages or modify the recorded capacity/instructions.
        retain = case.settings.get("retain_complete_units", 0)
        if type(retain) is not int or not 0 <= retain <= len(messages):
            raise ValueError("Invalid compaction prefix")
        settings["retain_complete_units"] = retain
        return settings

    async def authorize_case(self, repair_id: UUID, case: Any) -> None:
        task_id = await self._source(repair_id)
        if case.incident_ids.get("task") != task_id:
            raise ValueError("Incident belongs to another chat")
        index = await self.index(task_id)
        source_runs = {UUID(row["run_id"]): row for row in index.runs}
        run_id = case.incident_ids.get("run")
        if run_id not in source_runs or source_runs[run_id]["status"] != "failed":
            raise ValueError("Incident must reference a failed source run")
        steps = source_runs[run_id].get("steps", [])
        if case.incident_ids.get("step") and str(case.incident_ids["step"]) not in {
            step["step_id"] for step in steps
        }:
            raise ValueError("Incident step belongs to another run")
        if case.kind == "tool":
            await self._authorize_tool_case(task_id, case)
        if case.incident_ids.get("tool_call"):
            matched = [
                call
                for step in steps
                for call in step["calls"]
                if call["tool_call_id"] == str(case.incident_ids["tool_call"])
            ]
            if (
                len(matched) != 1
                or case.kind == "tool"
                and matched[0]["name"] != case.tool_name
            ):
                raise ValueError("Incident tool call differs from the source")
        allowed_artifacts, allowed_evidence = set(), set()
        for row in index.runs:
            for capture in row["evidence"]:
                allowed_evidence.add(UUID(capture["id"]))
                if capture["artifact_id"]:
                    allowed_artifacts.add(UUID(capture["artifact_id"]))
        if not set(case.evidence_ids) <= allowed_evidence:
            raise ValueError("Foreign incident evidence")
        references = list(getattr(case, "source_artifact_ids", ()))
        for name in (
            "request_artifact_id",
            "raw_response_artifact_id",
            "prior_compaction_artifact_id",
        ):
            value = getattr(case, name, None)
            if value:
                references.append(value)
        if not set(references) <= allowed_artifacts:
            raise ValueError("Foreign replay artifact")

    async def _authorize_tool_case(self, task_id: UUID, case: Any) -> None:
        from crucible.domain.tools import ToolResultStatus

        run_id = case.incident_ids["run"]
        call_id = case.incident_ids.get("tool_call")
        if call_id is None:
            raise ValueError("Tool replay requires the original call identity")
        async with self.factory() as uow:
            matches = []
            for step in await uow.steps.list_for_run(run_id):
                for call in await uow.tool_calls.list_for_step(step.id):
                    if call.id == call_id:
                        matches.append(call)
            if len(matches) != 1:
                raise ValueError("Original tool call is unavailable")
            call = matches[0]
            results = [
                result
                for result in await uow.tool_results.list_for_step(call.step_id)
                if result.tool_call_id == call.id
            ]
            captures = [
                await uow.repair_evidence.get(evidence_id, task_id=task_id)
                for evidence_id in case.evidence_ids
            ]
            task = await uow.tasks.get(task_id)
        if (
            call.task_id != task_id
            or call.run_id != run_id
            or call.name != case.tool_name
            or case.incident_ids.get("step", call.step_id) != call.step_id
            or canonical_json(dict(call.arguments)) != canonical_json(case.arguments)
            or self.safe(json.loads(canonical_json(call.arguments)))
            != json.loads(canonical_json(call.arguments))
        ):
            raise ValueError("Replay inputs differ from the original tool call")
        if len(results) != 1:
            raise ValueError("Original failure result is unavailable or ambiguous")
        result = results[0]
        matcher = case.expected_baseline_failure
        detail = result.display_text.replace("/workspace/", "{workspace}/")
        if task is not None and task.workspace_path:
            detail = detail.replace(
                str(task.workspace_path).rstrip("/") + "/", "{workspace}/"
            )
        if (
            result.task_id != task_id
            or result.run_id != run_id
            or result.status
            not in {
                ToolResultStatus.FAILED,
                ToolResultStatus.REJECTED,
                ToolResultStatus.TIMED_OUT,
            }
            or not result.error_code
            or matcher.status != "error"
            or matcher.error_code != result.error_code
            or not all(fragment in detail for fragment in matcher.detail_contains)
        ):
            raise ValueError("Replay failure differs from the recorded tool outcome")
        preedits = []
        for capture in captures:
            if capture is None or capture.run_id != run_id:
                raise ValueError("Replay evidence belongs to another incident")
            if capture.kind == "preedit":
                if not capture.complete or capture.tool_call_id != call.id:
                    raise ValueError("Pre-edit evidence belongs to another tool call")
                body = await self._preedit_for_call(capture, task_id, call)
                preedits.append(body)
        if call.name in {"write_file", "apply_patch"} and len(preedits) != 1:
            raise ValueError(
                "Mutation replay requires one complete original pre-edit capture"
            )

    async def _preedit_for_call(
        self, capture: Any, task_id: UUID, call: Any
    ) -> dict[str, Any]:
        if capture.artifact_id is None:
            raise ValueError("Pre-edit artifact is unavailable")
        metadata, raw = await self.artifacts.read(capture.artifact_id)
        if (
            metadata.task_id != task_id
            or metadata.sensitivity != "private"
            or hashlib.sha256(raw).hexdigest() != capture.manifest_digest
            or len(raw) > 8 * 1024 * 1024
        ):
            raise ValueError("Pre-edit artifact integrity failure")
        body = json.loads(raw)
        if not isinstance(body, dict):
            raise ValueError("Invalid pre-edit artifact")
        source = body.get("source_ids", {})
        if not isinstance(source, dict):
            raise ValueError("Invalid pre-edit source identity")
        if (
            body.get("kind") != "preedit"
            or body.get("complete") is not True
            or source.get("task_id") != str(task_id)
            or source.get("run_id") != str(call.run_id)
            or source.get("tool_call_id") != str(call.id)
        ):
            raise ValueError("Pre-edit artifact provenance differs from the tool call")
        if self.safe(body) != body:
            raise ValueError("Pre-edit evidence cannot be replayed without redaction")
        return body
