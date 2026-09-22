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
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import Event, EventType
from crucible.domain.repository import Repository
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
from crucible.storage import models


class RepositoryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, repository: Repository) -> None:
        await self._session.execute(
            insert(models.repositories).values(
                id=str(repository.id),
                root_path=str(repository.root_path),
                created_at=repository.created_at,
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

    async def list_for_task(self, task_id: UUID) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(models.runs.c.task_id == str(task_id))
                .order_by(models.runs.c.created_at, models.runs.c.id)
            )
        ).mappings()
        return tuple(self._from_row(row) for row in rows)

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
