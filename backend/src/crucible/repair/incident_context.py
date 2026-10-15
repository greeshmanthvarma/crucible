"""Source-chat-only, bounded read access; never a general artifact browser."""

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
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
        size = 0
        for message in messages[start : start + limit]:
            item = self.safe(json.loads(canonical_json(message)))
            if message.step_id is not None:
                async with self.factory() as uow:
                    calls = await uow.tool_calls.list_for_step(message.step_id)
                    results = await uow.tool_results.list_for_step(message.step_id)
                # Step identity comes from the source message.
                item["tool_calls"] = self.safe(json.loads(canonical_json(calls)))
                item["tool_results"] = self.safe(json.loads(canonical_json(results)))
            encoded = canonical_json(item)
            if size + len(encoded) > 80000:
                if not selected:
                    # Retrieval is bounded but can page a large message by parts
                    # through the original capture; do not invent truncated replay.
                    item = {
                        "id": str(message.id),
                        "unsupported_code": "message_size_limit",
                    }
                    selected.append(item)
                break
            selected.append(item)
            size += len(encoded)
        return {
            "messages": selected,
            "next": start + len(selected),
            "total": len(messages),
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
