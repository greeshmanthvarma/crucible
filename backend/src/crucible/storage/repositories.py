from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.application.idempotency import IdempotencyRecord
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
from crucible.domain.task import Task, TaskStatus
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

    async def list_stale_running(self, now: datetime) -> tuple[Run, ...]:
        rows = (
            await self._session.execute(
                select(models.runs)
                .where(
                    models.runs.c.status == RunStatus.RUNNING,
                    models.runs.c.lease_expires_at <= now,
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
                    conversation_sequence=row["conversation_sequence"],
                    role=MessageRole(row["role"]),
                    status=MessageStatus(row["status"]),
                    parts=tuple(
                        MessagePart(
                            id=UUID(part["id"]),
                            part_sequence=part["part_sequence"],
                            kind=MessagePartKind(part["kind"]),
                            text_content=part["text_content"],
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
