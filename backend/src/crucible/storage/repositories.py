from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.application.idempotency import IdempotencyRecord
from crucible.context.manifests import ContextManifest
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.artifacts import Artifact
from crucible.domain.commands import CommandSpec
from crucible.domain.compaction import Compaction
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import Event, EventType
from crucible.domain.repository import Repository, RepositorySettings
from crucible.domain.resources import (
    ExternalResource,
    ExternalResourceKind,
    ExternalResourceStatus,
)
from crucible.domain.results import Integration, IntegrationStatus, ResultRevision
from crucible.domain.run import Run, RunStatus
from crucible.domain.steps import Step, StepStatus
from crucible.domain.task import Task, TaskStatus
from crucible.domain.tools import (
    ToolCall,
    ToolCallStatus,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.domain.validation import (
    ValidationAttempt,
    ValidationCommandResult,
    ValidationStatus,
)
from crucible.storage import models


def _settings_json(settings: RepositorySettings) -> dict[str, object]:
    return {
        "validation_commands": [
            command.as_dict() for command in settings.validation_commands
        ],
        "sandbox_image": settings.sandbox_image,
        "sandbox_network": settings.sandbox_network,
        "validation_repair_limit": settings.validation_repair_limit,
        "default_cwd": settings.default_cwd,
        "compaction_threshold": settings.compaction_threshold,
        "compaction_model": settings.compaction_model,
        "compaction_prompt_version": settings.compaction_prompt_version,
        "compaction_attempt_limit": settings.compaction_attempt_limit,
        "model_input_limit": settings.model_input_limit,
        "model_output_reserve": settings.model_output_reserve,
        "schema_version": settings.schema_version,
    }


def _settings_from_json(value: object) -> RepositorySettings:
    data = cast(dict[str, object], value or {})
    defaults = RepositorySettings()
    return RepositorySettings(
        validation_commands=tuple(
            CommandSpec.from_dict(item)
            for item in cast(
                list[dict[str, object]], data.get("validation_commands", [])
            )
        ),
        sandbox_image=str(data.get("sandbox_image", defaults.sandbox_image)),
        sandbox_network=str(data.get("sandbox_network", defaults.sandbox_network)),
        validation_repair_limit=int(
            cast(
                int,
                data.get("validation_repair_limit", defaults.validation_repair_limit),
            )
        ),
        default_cwd=str(data.get("default_cwd", defaults.default_cwd)),
        compaction_threshold=float(
            cast(float, data.get("compaction_threshold", defaults.compaction_threshold))
        ),
        compaction_model=cast(
            str | None, data.get("compaction_model", defaults.compaction_model)
        ),
        compaction_prompt_version=str(
            data.get("compaction_prompt_version", defaults.compaction_prompt_version)
        ),
        compaction_attempt_limit=int(
            cast(
                int,
                data.get("compaction_attempt_limit", defaults.compaction_attempt_limit),
            )
        ),
        model_input_limit=int(
            cast(int, data.get("model_input_limit", defaults.model_input_limit))
        ),
        model_output_reserve=int(
            cast(int, data.get("model_output_reserve", defaults.model_output_reserve))
        ),
        schema_version=int(
            cast(int, data.get("schema_version", defaults.schema_version))
        ),
    )


class RepositoryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, repository: Repository) -> None:
        await self._session.execute(
            insert(models.repositories).values(
                id=str(repository.id),
                root_path=str(repository.root_path),
                created_at=repository.created_at,
                settings_json=_settings_json(repository.settings),
            )
        )
        await self._session.flush()

    async def add_if_absent(self, repository: Repository) -> bool:
        result = await self._session.execute(
            sqlite_insert(models.repositories)
            .values(
                id=str(repository.id),
                root_path=str(repository.root_path),
                created_at=repository.created_at,
                settings_json=_settings_json(repository.settings),
            )
            .on_conflict_do_nothing(index_elements=["root_path"])
        )
        await self._session.flush()
        return cast(int, result.rowcount) == 1  # type: ignore[attr-defined]

    async def get_by_root(self, root: Path) -> Repository | None:
        row = (
            (
                await self._session.execute(
                    select(models.repositories).where(
                        models.repositories.c.root_path == str(root)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Repository(
            id=UUID(row["id"]),
            root_path=Path(row["root_path"]),
            created_at=row["created_at"],
            settings=_settings_from_json(row["settings_json"]),
        )

    async def get(self, repository_id: UUID) -> Repository | None:
        row = (
            (
                await self._session.execute(
                    select(models.repositories).where(
                        models.repositories.c.id == str(repository_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Repository(
            id=UUID(row["id"]),
            root_path=Path(row["root_path"]),
            created_at=row["created_at"],
            settings=_settings_from_json(row["settings_json"]),
        )

    async def list(self) -> tuple[Repository, ...]:
        rows = (
            await self._session.execute(
                select(models.repositories).order_by(
                    models.repositories.c.created_at, models.repositories.c.id
                )
            )
        ).mappings()
        return tuple(
            Repository(
                id=UUID(row["id"]),
                root_path=Path(row["root_path"]),
                created_at=row["created_at"],
                settings=_settings_from_json(row["settings_json"]),
            )
            for row in rows
        )


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, task: Task) -> None:
        await self._session.execute(
            insert(models.tasks).values(
                id=str(task.id),
                repository_id=str(task.repository_id),
                source_ref=task.source_ref,
                base_revision=task.base_revision,
                workspace_path=str(task.workspace_path),
                status=task.status,
                failure_code=task.failure_code,
                failure_detail=task.failure_detail,
                created_at=task.created_at,
                updated_at=task.updated_at,
            )
        )
        await self._session.flush()

    async def get(self, task_id: UUID) -> Task | None:
        row = (
            (
                await self._session.execute(
                    select(models.tasks).where(models.tasks.c.id == str(task_id))
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Task(
            id=UUID(row["id"]),
            repository_id=UUID(row["repository_id"]),
            source_ref=row["source_ref"],
            base_revision=row["base_revision"],
            workspace_path=Path(row["workspace_path"]),
            status=TaskStatus(row["status"]),
            failure_code=row["failure_code"],
            failure_detail=row["failure_detail"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def update(self, task: Task) -> None:
        await self._session.execute(
            update(models.tasks)
            .where(models.tasks.c.id == str(task.id))
            .values(
                workspace_path=str(task.workspace_path),
                status=task.status,
                failure_code=task.failure_code,
                failure_detail=task.failure_detail,
                updated_at=task.updated_at,
            )
        )
        await self._session.flush()

    async def list_provisioning(self) -> tuple[Task, ...]:
        rows = (
            await self._session.execute(
                select(models.tasks)
                .where(models.tasks.c.status == TaskStatus.PROVISIONING)
                .order_by(models.tasks.c.created_at, models.tasks.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: object) -> Task:
        values = cast(dict[str, object], row)
        return Task(
            id=UUID(cast(str, values["id"])),
            repository_id=UUID(cast(str, values["repository_id"])),
            source_ref=cast(str, values["source_ref"]),
            base_revision=cast(str | None, values["base_revision"]),
            workspace_path=Path(cast(str, values["workspace_path"])),
            status=TaskStatus(cast(str, values["status"])),
            failure_code=cast(str | None, values["failure_code"]),
            failure_detail=cast(str | None, values["failure_detail"]),
            created_at=cast(datetime, values["created_at"]),
            updated_at=cast(datetime, values["updated_at"]),
        )


class RunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, run: Run) -> None:
        if run.triggering_message_id is not None:
            message_task = await self._session.scalar(
                select(models.messages.c.task_id).where(
                    models.messages.c.id == str(run.triggering_message_id)
                )
            )
            if message_task != str(run.task_id):
                raise ValueError(
                    "Run and triggering Message must belong to the same Task"
                )
        await self._session.execute(
            insert(models.runs).values(
                id=str(run.id),
                task_id=str(run.task_id),
                triggering_message_id=str(run.triggering_message_id)
                if run.triggering_message_id
                else None,
                status=run.status,
                execution_id=str(run.execution_id) if run.execution_id else None,
                lease_expires_at=run.lease_expires_at,
                heartbeat_at=run.heartbeat_at,
                outcome_code=run.outcome_code,
                outcome_detail=run.outcome_detail,
                created_at=run.created_at,
                started_at=run.started_at,
                completed_at=run.completed_at,
                cancel_requested_at=run.cancel_requested_at,
                cancel_code=run.cancel_code,
                settings_snapshot_json=_settings_json(run.settings_snapshot),
            )
        )
        await self._session.flush()

    async def get(self, run_id: UUID) -> Run | None:
        row = (
            (
                await self._session.execute(
                    select(models.runs).where(models.runs.c.id == str(run_id))
                )
            )
            .mappings()
            .one_or_none()
        )
        return self._from_row(row) if row is not None else None

    async def update(self, run: Run) -> None:
        await self._session.execute(
            update(models.runs)
            .where(models.runs.c.id == str(run.id))
            .values(
                status=run.status,
                execution_id=str(run.execution_id) if run.execution_id else None,
                lease_expires_at=run.lease_expires_at,
                heartbeat_at=run.heartbeat_at,
                outcome_code=run.outcome_code,
                outcome_detail=run.outcome_detail,
                started_at=run.started_at,
                completed_at=run.completed_at,
                cancel_requested_at=run.cancel_requested_at,
                cancel_code=run.cancel_code,
                settings_snapshot_json=_settings_json(run.settings_snapshot),
            )
        )
        await self._session.flush()

    async def list_queued(self) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(models.runs.c.status == RunStatus.QUEUED)
                .order_by(models.runs.c.created_at, models.runs.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    async def list_running(self) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(models.runs.c.status == RunStatus.RUNNING)
                .order_by(models.runs.c.created_at, models.runs.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    async def list_for_task(self, task_id: UUID) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(models.runs.c.task_id == str(task_id))
                .order_by(models.runs.c.created_at, models.runs.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    async def get_nonterminal_for_task(self, task_id: UUID) -> Run | None:
        rows = (
            (
                await self._session.execute(
                    select(models.runs)
                    .where(
                        models.runs.c.task_id == str(task_id),
                        models.runs.c.status.in_((RunStatus.QUEUED, RunStatus.RUNNING)),
                    )
                    .order_by(models.runs.c.created_at.desc(), models.runs.c.id.desc())
                    .limit(1)
                )
            )
            .mappings()
            .one_or_none()
        )
        return self._from_row(rows) if rows is not None else None

    async def list_running_not_owned_by(
        self, process_execution_id: UUID
    ) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(
                    models.runs.c.status == RunStatus.RUNNING,
                    models.runs.c.execution_id != str(process_execution_id),
                )
                .order_by(models.runs.c.created_at, models.runs.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: object) -> Run:
        values = cast(dict[str, object], row)
        return Run(
            id=UUID(cast(str, values["id"])),
            task_id=UUID(cast(str, values["task_id"])),
            triggering_message_id=UUID(cast(str, values["triggering_message_id"]))
            if values["triggering_message_id"]
            else None,
            status=RunStatus(cast(str, values["status"])),
            execution_id=UUID(cast(str, values["execution_id"]))
            if values["execution_id"]
            else None,
            lease_expires_at=cast(datetime | None, values["lease_expires_at"]),
            heartbeat_at=cast(datetime | None, values["heartbeat_at"]),
            outcome_code=cast(str | None, values["outcome_code"]),
            outcome_detail=cast(str | None, values["outcome_detail"]),
            created_at=cast(datetime, values["created_at"]),
            started_at=cast(datetime | None, values["started_at"]),
            completed_at=cast(datetime | None, values["completed_at"]),
            cancel_requested_at=cast(datetime | None, values["cancel_requested_at"]),
            cancel_code=cast(str | None, values["cancel_code"]),
            settings_snapshot=_settings_from_json(values["settings_snapshot_json"]),
        )

    async def claim_queued(
        self,
        run_id: UUID,
        execution_id: UUID,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        result = await self._session.execute(
            update(models.runs)
            .where(
                models.runs.c.id == str(run_id),
                models.runs.c.status == RunStatus.QUEUED,
            )
            .values(
                status=RunStatus.RUNNING,
                execution_id=str(execution_id),
                started_at=now,
                heartbeat_at=now,
                lease_expires_at=lease_expires_at,
            )
        )
        return cast(int, result.rowcount) == 1  # type: ignore[attr-defined]

    async def set_triggering_message(self, run_id: UUID, message_id: UUID) -> None:
        row = (
            await self._session.execute(
                select(models.runs.c.task_id, models.messages.c.task_id)
                .select_from(
                    models.runs.join(
                        models.messages, models.messages.c.id == str(message_id)
                    )
                )
                .where(models.runs.c.id == str(run_id))
            )
        ).one_or_none()
        if row is None or row[0] != row[1]:
            raise ValueError("Run and triggering Message must belong to the same Task")
        await self._session.execute(
            update(models.runs)
            .where(models.runs.c.id == str(run_id))
            .values(triggering_message_id=str(message_id))
        )
        await self._session.flush()


class StepRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, step: Step) -> None:
        run_task = await self._session.scalar(
            select(models.runs.c.task_id).where(models.runs.c.id == str(step.run_id))
        )
        if run_task != str(step.task_id):
            raise ValueError("Step and Run must belong to the same Task")
        await self._session.execute(
            insert(models.steps).values(
                id=str(step.id),
                task_id=str(step.task_id),
                run_id=str(step.run_id),
                step_sequence=step.step_sequence,
                status=step.status,
                created_at=step.created_at,
                started_at=step.started_at,
                completed_at=step.completed_at,
            )
        )
        await self._session.flush()

    async def update(self, step: Step) -> None:
        await self._session.execute(
            update(models.steps)
            .where(models.steps.c.id == str(step.id))
            .values(
                status=step.status,
                started_at=step.started_at,
                completed_at=step.completed_at,
            )
        )
        await self._session.flush()

    async def list_for_run(self, run_id: UUID) -> tuple[Step, ...]:
        rows = (
            await self._session.execute(
                select(models.steps)
                .where(models.steps.c.run_id == str(run_id))
                .order_by(models.steps.c.step_sequence)
            )
        ).mappings()
        return tuple(
            Step(
                id=UUID(row["id"]),
                task_id=UUID(row["task_id"]),
                run_id=UUID(row["run_id"]),
                step_sequence=row["step_sequence"],
                status=StepStatus(row["status"]),
                created_at=row["created_at"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
            )
            for row in rows
        )


class ContextManifestRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, manifest: ContextManifest) -> None:
        await self._require_same_task(
            manifest.task_id, manifest.run_id, manifest.step_id
        )
        await self._session.execute(
            insert(models.context_manifests).values(
                id=str(manifest.id),
                task_id=str(manifest.task_id),
                run_id=str(manifest.run_id),
                step_id=str(manifest.step_id),
                model=manifest.model,
                parameters_json=dict(manifest.parameters),
                input_limit=manifest.input_limit,
                output_reserve=manifest.output_reserve,
                threshold=str(manifest.threshold),
                estimated_tokens=manifest.estimated_tokens,
                message_ids_json=[str(value) for value in manifest.message_ids],
                part_ids_json=[str(value) for value in manifest.part_ids],
                instruction_digests_json=dict(manifest.instruction_digests),
                tool_schema_digest=manifest.tool_schema_digest,
                created_at=manifest.created_at,
            )
        )
        await self._session.flush()

    async def get_for_step(self, step_id: UUID) -> ContextManifest | None:
        row = (
            (
                await self._session.execute(
                    select(models.context_manifests).where(
                        models.context_manifests.c.step_id == str(step_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return ContextManifest(
            id=UUID(row["id"]),
            task_id=UUID(row["task_id"]),
            run_id=UUID(row["run_id"]),
            step_id=UUID(row["step_id"]),
            model=row["model"],
            parameters=row["parameters_json"],
            input_limit=row["input_limit"],
            output_reserve=row["output_reserve"],
            threshold=float(row["threshold"]),
            estimated_tokens=row["estimated_tokens"],
            message_ids=[UUID(value) for value in row["message_ids_json"]],
            part_ids=[UUID(value) for value in row["part_ids_json"]],
            instruction_digests=row["instruction_digests_json"],
            tool_schema_digest=row["tool_schema_digest"],
            created_at=row["created_at"],
        )

    async def _require_same_task(
        self, task_id: UUID, run_id: UUID, step_id: UUID
    ) -> None:
        row = (
            await self._session.execute(
                select(models.runs.c.task_id, models.steps.c.task_id)
                .select_from(
                    models.runs.join(
                        models.steps, models.steps.c.run_id == models.runs.c.id
                    )
                )
                .where(
                    models.runs.c.id == str(run_id),
                    models.steps.c.id == str(step_id),
                )
            )
        ).one_or_none()
        if row is None or row[0] != str(task_id) or row[1] != str(task_id):
            raise ValueError("Context Manifest parents must belong to the same Task")


class ToolCallRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, call: ToolCall) -> None:
        parent_tasks = (
            await self._session.execute(
                select(
                    models.runs.c.task_id,
                    models.steps.c.task_id,
                    models.messages.c.task_id,
                )
                .select_from(
                    models.runs.join(
                        models.steps, models.steps.c.run_id == models.runs.c.id
                    ).join(
                        models.messages,
                        models.messages.c.id == str(call.assistant_message_id),
                    )
                )
                .where(
                    models.runs.c.id == str(call.run_id),
                    models.steps.c.id == str(call.step_id),
                )
            )
        ).one_or_none()
        if parent_tasks is None or any(
            task != str(call.task_id) for task in parent_tasks
        ):
            raise ValueError("Tool Call parents must belong to the same Task")
        await self._session.execute(
            insert(models.tool_calls).values(
                id=str(call.id),
                task_id=str(call.task_id),
                run_id=str(call.run_id),
                step_id=str(call.step_id),
                assistant_message_id=str(call.assistant_message_id),
                call_sequence=call.call_sequence,
                name=call.name,
                arguments_json=dict(call.arguments),
                schema_version=call.schema_version,
                provider_correlation_id=call.provider_correlation_id,
                execution_mode=call.execution_mode,
                status=call.status,
                created_at=call.created_at,
            )
        )
        await self._session.flush()

    async def list_for_step(self, step_id: UUID) -> tuple[ToolCall, ...]:
        rows = (
            await self._session.execute(
                select(models.tool_calls)
                .where(models.tool_calls.c.step_id == str(step_id))
                .order_by(models.tool_calls.c.call_sequence)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    async def update(self, call: ToolCall) -> None:
        result = await self._session.execute(
            update(models.tool_calls)
            .where(models.tool_calls.c.id == str(call.id))
            .values(status=call.status)
        )
        if cast(int, result.rowcount) != 1:  # type: ignore[attr-defined]
            raise ValueError(f"Tool Call not found: {call.id}")

    async def list_without_result_for_run(self, run_id: UUID) -> tuple[ToolCall, ...]:
        rows = (
            await self._session.execute(
                select(models.tool_calls)
                .outerjoin(
                    models.tool_results,
                    models.tool_results.c.tool_call_id == models.tool_calls.c.id,
                )
                .where(
                    models.tool_calls.c.run_id == str(run_id),
                    models.tool_results.c.id.is_(None),
                )
                .order_by(models.tool_calls.c.call_sequence)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: object) -> ToolCall:
        values = cast(dict[str, object], row)
        return ToolCall(
            id=UUID(cast(str, values["id"])),
            task_id=UUID(cast(str, values["task_id"])),
            run_id=UUID(cast(str, values["run_id"])),
            step_id=UUID(cast(str, values["step_id"])),
            assistant_message_id=UUID(cast(str, values["assistant_message_id"])),
            call_sequence=cast(int, values["call_sequence"]),
            name=cast(str, values["name"]),
            arguments=cast(dict[str, object], values["arguments_json"]),
            schema_version=cast(int, values["schema_version"]),
            provider_correlation_id=cast(str | None, values["provider_correlation_id"]),
            execution_mode=ToolExecutionMode(cast(str, values["execution_mode"])),
            created_at=cast(datetime, values["created_at"]),
            status=ToolCallStatus(cast(str, values["status"])),
        )


class ToolResultRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, result: ToolResult) -> None:
        call = (
            await self._session.execute(
                select(
                    models.tool_calls.c.task_id,
                    models.tool_calls.c.run_id,
                    models.tool_calls.c.step_id,
                ).where(models.tool_calls.c.id == str(result.tool_call_id))
            )
        ).one_or_none()
        if call != (
            str(result.task_id),
            str(result.run_id),
            str(result.step_id),
        ):
            raise ValueError("Tool Result must belong to its Tool Call")
        await self._session.execute(
            insert(models.tool_results).values(
                id=str(result.id),
                task_id=str(result.task_id),
                run_id=str(result.run_id),
                step_id=str(result.step_id),
                tool_call_id=str(result.tool_call_id),
                status=result.status,
                result_json=dict(result.result),
                schema_version=result.schema_version,
                display_text=result.display_text,
                error_code=result.error_code,
                completion_sequence=result.completion_sequence,
                created_at=result.created_at,
                completed_at=result.completed_at,
                artifact_id=str(result.artifact_id) if result.artifact_id else None,
            )
        )
        await self._session.flush()

    async def list_for_step(self, step_id: UUID) -> tuple[ToolResult, ...]:
        rows = (
            await self._session.execute(
                select(models.tool_results)
                .where(models.tool_results.c.step_id == str(step_id))
                .order_by(models.tool_results.c.completion_sequence)
            )
        ).mappings()
        return tuple(
            ToolResult(
                id=UUID(row["id"]),
                task_id=UUID(row["task_id"]),
                run_id=UUID(row["run_id"]),
                step_id=UUID(row["step_id"]),
                tool_call_id=UUID(row["tool_call_id"]),
                status=ToolResultStatus(row["status"]),
                result=row["result_json"],
                schema_version=row["schema_version"],
                display_text=row["display_text"],
                error_code=row["error_code"],
                completion_sequence=row["completion_sequence"],
                created_at=row["created_at"],
                completed_at=row["completed_at"],
                artifact_id=UUID(row["artifact_id"]) if row["artifact_id"] else None,
            )
            for row in rows
        )


class MessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: Message) -> Message:
        if message.run_id is not None:
            run_task = await self._session.scalar(
                select(models.runs.c.task_id).where(
                    models.runs.c.id == str(message.run_id)
                )
            )
            if run_task != str(message.task_id):
                raise ValueError("Message and Run must belong to the same Task")
        if message.step_id is not None:
            step_task = await self._session.scalar(
                select(models.steps.c.task_id).where(
                    models.steps.c.id == str(message.step_id)
                )
            )
            if step_task != str(message.task_id):
                raise ValueError("Message and Step must belong to the same Task")
        sequence = await self._session.scalar(
            update(models.tasks)
            .where(models.tasks.c.id == str(message.task_id))
            .values(
                next_conversation_sequence=models.tasks.c.next_conversation_sequence + 1
            )
            .returning(models.tasks.c.next_conversation_sequence - 1)
        )
        if sequence is None:
            raise ValueError("Task not found")
        stored = replace(message, conversation_sequence=sequence)
        await self._session.execute(
            insert(models.messages).values(
                id=str(stored.id),
                task_id=str(stored.task_id),
                run_id=str(stored.run_id) if stored.run_id else None,
                step_id=str(stored.step_id) if stored.step_id else None,
                conversation_sequence=stored.conversation_sequence,
                role=stored.role,
                status=stored.status,
                created_at=stored.created_at,
                completed_at=stored.completed_at,
            )
        )
        for part in stored.parts:
            await self._session.execute(
                insert(models.message_parts).values(
                    id=str(part.id),
                    message_id=str(stored.id),
                    part_sequence=part.part_sequence,
                    kind=part.kind,
                    text_content=part.text_content,
                    reasoning_content=part.reasoning_content,
                    tool_call_id=str(part.tool_call_id) if part.tool_call_id else None,
                    tool_result_id=str(part.tool_result_id)
                    if part.tool_result_id
                    else None,
                )
            )
        await self._session.flush()
        return stored

    async def list_for_task(self, task_id: UUID) -> tuple[Message, ...]:
        message_rows = (
            (
                await self._session.execute(
                    select(models.messages)
                    .where(models.messages.c.task_id == str(task_id))
                    .order_by(models.messages.c.conversation_sequence)
                )
            )
            .mappings()
            .all()
        )
        result = []
        for row in message_rows:
            part_rows = (
                (
                    await self._session.execute(
                        select(models.message_parts)
                        .where(models.message_parts.c.message_id == row["id"])
                        .order_by(models.message_parts.c.part_sequence)
                    )
                )
                .mappings()
                .all()
            )
            result.append(
                Message(
                    id=UUID(row["id"]),
                    task_id=UUID(row["task_id"]),
                    run_id=UUID(row["run_id"]) if row["run_id"] else None,
                    step_id=UUID(row["step_id"]) if row["step_id"] else None,
                    conversation_sequence=row["conversation_sequence"],
                    role=MessageRole(row["role"]),
                    status=MessageStatus(row["status"]),
                    parts=tuple(
                        MessagePart(
                            id=UUID(part["id"]),
                            part_sequence=part["part_sequence"],
                            kind=MessagePartKind(part["kind"]),
                            text_content=part["text_content"],
                            reasoning_content=part["reasoning_content"],
                            tool_call_id=UUID(part["tool_call_id"])
                            if part["tool_call_id"]
                            else None,
                            tool_result_id=UUID(part["tool_result_id"])
                            if part["tool_result_id"]
                            else None,
                        )
                        for part in part_rows
                    ),
                    created_at=row["created_at"],
                    completed_at=row["completed_at"],
                )
            )
        return tuple(result)


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, event: Event) -> Event:
        run_sequence = None
        if event.run_id is not None:
            run_task = await self._session.scalar(
                select(models.runs.c.task_id).where(
                    models.runs.c.id == str(event.run_id)
                )
            )
            if run_task != str(event.task_id):
                raise ValueError("Event and Run must belong to the same Task")
            run_sequence = await self._session.scalar(
                update(models.runs)
                .where(models.runs.c.id == str(event.run_id))
                .values(next_run_sequence=models.runs.c.next_run_sequence + 1)
                .returning(models.runs.c.next_run_sequence - 1)
            )
        task_sequence = await self._session.scalar(
            update(models.tasks)
            .where(models.tasks.c.id == str(event.task_id))
            .values(next_task_sequence=models.tasks.c.next_task_sequence + 1)
            .returning(models.tasks.c.next_task_sequence - 1)
        )
        if task_sequence is None or (event.run_id is not None and run_sequence is None):
            raise ValueError("Event parent not found")
        stored = replace(event, task_sequence=task_sequence, run_sequence=run_sequence)
        await self._session.execute(
            insert(models.task_events).values(
                id=str(stored.id),
                task_id=str(stored.task_id),
                run_id=str(stored.run_id) if stored.run_id else None,
                task_sequence=stored.task_sequence,
                run_sequence=stored.run_sequence,
                type=stored.type,
                schema_version=stored.schema_version,
                payload_json=stored.payload_json(),
                created_at=stored.created_at,
            )
        )
        await self._session.flush()
        return stored

    async def get(self, event_id: UUID) -> Event | None:
        row = (
            (
                await self._session.execute(
                    select(models.task_events).where(
                        models.task_events.c.id == str(event_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return self._from_row(row) if row is not None else None

    async def list_after(
        self, task_id: UUID, sequence: int, limit: int = 100
    ) -> tuple[Event, ...]:
        rows = (
            await self._session.execute(
                select(models.task_events)
                .where(
                    models.task_events.c.task_id == str(task_id),
                    models.task_events.c.task_sequence > sequence,
                )
                .order_by(models.task_events.c.task_sequence)
                .limit(limit)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: object) -> Event:
        values = cast(dict[str, object], row)
        return Event(
            id=UUID(cast(str, values["id"])),
            task_id=UUID(cast(str, values["task_id"])),
            run_id=UUID(cast(str, values["run_id"])) if values["run_id"] else None,
            task_sequence=cast(int, values["task_sequence"]),
            run_sequence=cast(int | None, values["run_sequence"]),
            type=EventType(cast(str, values["type"])),
            schema_version=cast(int, values["schema_version"]),
            payload=cast(dict[str, object], values["payload_json"]),
            created_at=cast(datetime, values["created_at"]),
        )


class ApprovalRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, approval: Approval) -> None:
        await self._require_same_task(approval)
        await self._session.execute(
            insert(models.approvals).values(
                id=str(approval.id),
                task_id=str(approval.task_id),
                run_id=str(approval.run_id),
                step_id=str(approval.step_id),
                tool_call_id=str(approval.tool_call_id),
                spec_json=approval.spec.as_dict(),
                spec_digest=approval.spec_digest,
                status=approval.status,
                decision_reason=approval.decision_reason,
                decided_by=approval.decided_by,
                created_at=approval.created_at,
                decided_at=approval.decided_at,
            )
        )
        await self._session.flush()

    async def get(self, approval_id: UUID) -> Approval | None:
        return await self._one(models.approvals.c.id == str(approval_id))

    async def get_for_tool_call(self, tool_call_id: UUID) -> Approval | None:
        return await self._one(models.approvals.c.tool_call_id == str(tool_call_id))

    async def update(self, approval: Approval) -> None:
        result = await self._session.execute(
            update(models.approvals)
            .where(models.approvals.c.id == str(approval.id))
            .values(
                status=approval.status,
                decision_reason=approval.decision_reason,
                decided_by=approval.decided_by,
                decided_at=approval.decided_at,
            )
        )
        if cast(int, result.rowcount) != 1:  # type: ignore[attr-defined]
            raise ValueError(f"Approval not found: {approval.id}")

    async def decide_pending(self, approval: Approval) -> bool:
        result = await self._session.execute(
            update(models.approvals)
            .where(
                models.approvals.c.id == str(approval.id),
                models.approvals.c.status == ApprovalStatus.PENDING,
                models.approvals.c.spec_digest == approval.spec_digest,
            )
            .values(
                status=approval.status,
                decision_reason=approval.decision_reason,
                decided_by=approval.decided_by,
                decided_at=approval.decided_at,
            )
        )
        return cast(int, result.rowcount) == 1  # type: ignore[attr-defined]

    async def list_for_task(self, task_id: UUID) -> tuple[Approval, ...]:
        return await self._many(models.approvals.c.task_id == str(task_id))

    async def list_pending_for_run(self, run_id: UUID) -> tuple[Approval, ...]:
        return await self._many(
            (models.approvals.c.run_id == str(run_id))
            & (models.approvals.c.status == ApprovalStatus.PENDING)
        )

    async def list_for_run(self, run_id: UUID) -> tuple[Approval, ...]:
        return await self._many(models.approvals.c.run_id == str(run_id))

    async def _one(self, criterion: object) -> Approval | None:
        row = (
            (
                await self._session.execute(
                    select(models.approvals).where(criterion)  # type: ignore[arg-type]
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else self._from_row(row)

    async def _many(self, criterion: object) -> tuple[Approval, ...]:
        rows = (
            await self._session.execute(
                select(models.approvals)
                .where(criterion)  # type: ignore[arg-type]
                .order_by(models.approvals.c.created_at)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: object) -> Approval:
        value = cast(dict[str, object], row)
        return Approval(
            UUID(cast(str, value["id"])),
            UUID(cast(str, value["task_id"])),
            UUID(cast(str, value["run_id"])),
            UUID(cast(str, value["step_id"])),
            UUID(cast(str, value["tool_call_id"])),
            CommandSpec.from_dict(cast(dict[str, object], value["spec_json"])),
            cast(str, value["spec_digest"]),
            ApprovalStatus(cast(str, value["status"])),
            cast(str | None, value["decision_reason"]),
            cast(str | None, value["decided_by"]),
            cast(datetime, value["created_at"]),
            cast(datetime | None, value["decided_at"]),
        )

    async def _require_same_task(self, approval: Approval) -> None:
        row = (
            await self._session.execute(
                select(
                    models.runs.c.task_id,
                    models.steps.c.task_id,
                    models.tool_calls.c.task_id,
                )
                .select_from(
                    models.runs.join(
                        models.steps, models.steps.c.run_id == models.runs.c.id
                    ).join(
                        models.tool_calls,
                        models.tool_calls.c.id == str(approval.tool_call_id),
                    )
                )
                .where(
                    models.runs.c.id == str(approval.run_id),
                    models.steps.c.id == str(approval.step_id),
                    models.tool_calls.c.run_id == str(approval.run_id),
                    models.tool_calls.c.step_id == str(approval.step_id),
                )
            )
        ).one_or_none()
        if row is None or any(value != str(approval.task_id) for value in row):
            raise ValueError("Approval parents must belong to the same Task")


class ArtifactRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, artifact: Artifact) -> None:
        await self._session.execute(
            insert(models.artifacts).values(
                id=str(artifact.id),
                task_id=str(artifact.task_id),
                content_hash=artifact.content_hash,
                media_type=artifact.media_type,
                byte_length=artifact.byte_length,
                storage_identity=artifact.storage_identity,
                sensitivity=artifact.sensitivity,
                metadata_json=dict(artifact.metadata),
                created_at=artifact.created_at,
            )
        )
        await self._session.flush()

    async def get(self, artifact_id: UUID) -> Artifact | None:
        return await self._one(models.artifacts.c.id == str(artifact_id))

    async def get_by_content(
        self,
        task_id: UUID,
        content_hash: str,
        media_type: str,
        sensitivity: str,
    ) -> Artifact | None:
        return await self._one(
            (models.artifacts.c.task_id == str(task_id))
            & (models.artifacts.c.content_hash == content_hash)
            & (models.artifacts.c.media_type == media_type)
            & (models.artifacts.c.sensitivity == sensitivity)
        )

    async def _one(self, criterion: object) -> Artifact | None:
        row = (
            (
                await self._session.execute(
                    select(models.artifacts).where(criterion)  # type: ignore[arg-type]
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Artifact(
            UUID(row["id"]),
            UUID(row["task_id"]),
            row["content_hash"],
            row["media_type"],
            row["byte_length"],
            row["storage_identity"],
            row["sensitivity"],
            row["metadata_json"],
            row["created_at"],
        )


class ExternalResourceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, resource: ExternalResource) -> None:
        await self._session.execute(
            insert(models.external_resources).values(**self._values(resource))
        )
        await self._session.flush()

    async def get(self, resource_id: UUID) -> ExternalResource | None:
        rows = await self._query(models.external_resources.c.id == str(resource_id))
        return rows[0] if rows else None

    async def update(self, resource: ExternalResource) -> None:
        values = self._values(resource)
        values.pop("id")
        result = await self._session.execute(
            update(models.external_resources)
            .where(models.external_resources.c.id == str(resource.id))
            .values(**values)
        )
        if cast(int, result.rowcount) != 1:  # type: ignore[attr-defined]
            raise ValueError(f"External resource not found: {resource.id}")

    async def list_for_task(self, task_id: UUID) -> tuple[ExternalResource, ...]:
        return await self._query(models.external_resources.c.task_id == str(task_id))

    async def list_managed(self) -> tuple[ExternalResource, ...]:
        return await self._query()

    async def _query(
        self, criterion: object | None = None
    ) -> tuple[ExternalResource, ...]:
        statement = select(models.external_resources)
        if criterion is not None:
            statement = statement.where(criterion)  # type: ignore[arg-type]
        rows = (
            await self._session.execute(
                statement.order_by(models.external_resources.c.created_at)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _values(resource: ExternalResource) -> dict[str, object]:
        return {
            "id": str(resource.id),
            "task_id": str(resource.task_id),
            "run_id": str(resource.run_id) if resource.run_id else None,
            "tool_call_id": str(resource.tool_call_id)
            if resource.tool_call_id
            else None,
            "kind": resource.kind,
            "external_identity": resource.external_identity,
            "mount_target": resource.mount_target,
            "status": resource.status,
            "labels_json": dict(resource.labels),
            "metadata_json": dict(resource.metadata),
            "created_at": resource.created_at,
            "updated_at": resource.updated_at,
        }

    @staticmethod
    def _from_row(row: object) -> ExternalResource:
        value = cast(dict[str, object], row)
        return ExternalResource(
            UUID(cast(str, value["id"])),
            UUID(cast(str, value["task_id"])),
            UUID(cast(str, value["run_id"])) if value["run_id"] else None,
            UUID(cast(str, value["tool_call_id"])) if value["tool_call_id"] else None,
            ExternalResourceKind(cast(str, value["kind"])),
            cast(str, value["external_identity"]),
            cast(str | None, value["mount_target"]),
            ExternalResourceStatus(cast(str, value["status"])),
            cast(dict[str, str], value["labels_json"]),
            cast(dict[str, object], value["metadata_json"]),
            cast(datetime, value["created_at"]),
            cast(datetime, value["updated_at"]),
        )


class IdempotencyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, record: IdempotencyRecord) -> None:
        await self._session.execute(
            insert(models.idempotency_records).values(
                id=str(record.id),
                scope=record.scope,
                key=record.key,
                request_hash=record.request_hash,
                response_status=record.response_status,
                response_json=record.response_json,
                created_at=record.created_at,
            )
        )
        await self._session.flush()

    async def get(self, scope: str, key: str) -> IdempotencyRecord | None:
        row = (
            (
                await self._session.execute(
                    select(models.idempotency_records).where(
                        models.idempotency_records.c.scope == scope,
                        models.idempotency_records.c.key == key,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return IdempotencyRecord(
            UUID(row["id"]),
            row["scope"],
            row["key"],
            row["request_hash"],
            row["response_status"],
            row["response_json"],
            row["created_at"],
        )


class CompactionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, value: Compaction) -> None:
        await self._session.execute(
            insert(models.compactions).values(
                id=str(value.id),
                task_id=str(value.task_id),
                source_start_sequence=value.source_start_sequence,
                source_end_sequence=value.source_end_sequence,
                retained_tail_start_sequence=value.retained_tail_start_sequence,
                summary_artifact_id=str(value.summary_artifact_id),
                rendered_summary=value.rendered_summary,
                previous_compaction_id=str(value.previous_compaction_id)
                if value.previous_compaction_id
                else None,
                model=value.model,
                parameters_json=value.parameters,
                prompt_version=value.prompt_version,
                input_tokens=value.input_tokens,
                output_tokens=value.output_tokens,
                resulting_context_estimate=value.resulting_context_estimate,
                created_at=value.created_at,
            )
        )
        await self._session.flush()

    async def get(self, value_id: UUID) -> Compaction | None:
        row = (
            (
                await self._session.execute(
                    select(models.compactions).where(
                        models.compactions.c.id == str(value_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Compaction(
            UUID(row["id"]),
            UUID(row["task_id"]),
            row["source_start_sequence"],
            row["source_end_sequence"],
            row["retained_tail_start_sequence"],
            UUID(row["summary_artifact_id"]),
            row["rendered_summary"],
            UUID(row["previous_compaction_id"])
            if row["previous_compaction_id"]
            else None,
            row["model"],
            row["parameters_json"],
            row["prompt_version"],
            row["input_tokens"],
            row["output_tokens"],
            row["resulting_context_estimate"],
            row["created_at"],
        )


class ValidationAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, value: ValidationAttempt) -> None:
        await self._session.execute(
            insert(models.validation_attempts).values(
                id=str(value.id),
                run_id=str(value.run_id),
                attempt_number=value.attempt_number,
                status=value.status,
                created_at=value.created_at,
                completed_at=value.completed_at,
            )
        )
        await self._session.flush()

    async def get(self, value_id: UUID) -> ValidationAttempt | None:
        row = (
            (
                await self._session.execute(
                    select(models.validation_attempts).where(
                        models.validation_attempts.c.id == str(value_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return (
            None
            if row is None
            else ValidationAttempt(
                UUID(row["id"]),
                UUID(row["run_id"]),
                row["attempt_number"],
                ValidationStatus(row["status"]),
                row["created_at"],
                row["completed_at"],
            )
        )


class ValidationCommandResultRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, value: ValidationCommandResult) -> None:
        await self._session.execute(
            insert(models.validation_command_results).values(
                id=str(value.id),
                validation_attempt_id=str(value.validation_attempt_id),
                command_sequence=value.command_sequence,
                status=value.status,
                approval_id=str(value.approval_id) if value.approval_id else None,
                tool_call_id=str(value.tool_call_id) if value.tool_call_id else None,
                artifact_id=str(value.artifact_id) if value.artifact_id else None,
                exit_code=value.exit_code,
                summary=value.summary,
                created_at=value.created_at,
                completed_at=value.completed_at,
            )
        )
        await self._session.flush()


class ResultRevisionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, value: ResultRevision) -> None:
        await self._session.execute(
            insert(models.result_revisions).values(
                id=str(value.id),
                task_id=str(value.task_id),
                commit_sha=value.commit_sha,
                parent_revision=value.parent_revision,
                previous_result_revision_id=str(value.previous_result_revision_id)
                if value.previous_result_revision_id
                else None,
                diff_artifact_id=str(value.diff_artifact_id),
                validation_snapshot_json=value.validation_snapshot,
                summary=value.summary,
                created_by=value.created_by,
                created_at=value.created_at,
            )
        )
        await self._session.flush()

    async def get(self, value_id: UUID) -> ResultRevision | None:
        row = (
            (
                await self._session.execute(
                    select(models.result_revisions).where(
                        models.result_revisions.c.id == str(value_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return (
            None
            if row is None
            else ResultRevision(
                UUID(row["id"]),
                UUID(row["task_id"]),
                row["commit_sha"],
                row["parent_revision"],
                UUID(row["previous_result_revision_id"])
                if row["previous_result_revision_id"]
                else None,
                UUID(row["diff_artifact_id"]),
                row["validation_snapshot_json"],
                row["summary"],
                row["created_by"],
                row["created_at"],
            )
        )


class IntegrationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, value: Integration) -> None:
        await self._session.execute(
            insert(models.integrations).values(
                id=str(value.id),
                result_revision_id=str(value.result_revision_id),
                repository_id=str(value.repository_id),
                target_ref=value.target_ref,
                expected_target_revision=value.expected_target_revision,
                idempotency_key=value.idempotency_key,
                status=value.status,
                observed_before_revision=value.observed_before_revision,
                observed_after_revision=value.observed_after_revision,
                failure_code=value.failure_code,
                failure_detail=value.failure_detail,
                created_at=value.created_at,
                completed_at=value.completed_at,
            )
        )
        await self._session.flush()

    async def get(self, value_id: UUID) -> Integration | None:
        row = (
            (
                await self._session.execute(
                    select(models.integrations).where(
                        models.integrations.c.id == str(value_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return (
            None
            if row is None
            else Integration(
                UUID(row["id"]),
                UUID(row["result_revision_id"]),
                UUID(row["repository_id"]),
                row["target_ref"],
                row["expected_target_revision"],
                row["idempotency_key"],
                IntegrationStatus(row["status"]),
                row["observed_before_revision"],
                row["observed_after_revision"],
                row["failure_code"],
                row["failure_detail"],
                row["created_at"],
                row["completed_at"],
            )
        )
