"""Disposable whole-task worker. Executed only by the Docker launcher."""

import asyncio
import sys
from pathlib import Path
from uuid import uuid5

from crucible.application.worker_container import create_worker_engine
from crucible.domain.clock import SystemClock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository, RepositorySettings
from crucible.domain.run import Run, RunStatus
from crucible.domain.task import Task
from crucible.engine.gateway import ModelError, PreparedModelRequest
from crucible.improvement.worker_gateway import WorkerModelGateway
from crucible.improvement.worker_protocol import WorkerJob, WorkerObservation
from crucible.repair.gateway import WorkerCredential
from crucible.repair.identity import canonical_json
from crucible.storage.database import Database
from crucible.storage.models import metadata


async def main() -> None:
    raw = Path("/job/request.json").read_bytes()
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("job exceeds cap")
    job = WorkerJob.model_validate_json(raw)
    token = sys.stdin.readline(256).strip()
    if not token:
        raise ValueError("missing scoped credential")
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
        engine, uow_factory = create_worker_engine(
            database, job, CapturingGateway(), token
        )
        clock = SystemClock()
        settings = RepositorySettings(
            sandbox_image=job.solver_case.image,
        )
        repository = Repository(new_id(), Path("/workspace"), clock.now(), settings)
        task_id, run_id = uuid5(job.job_id, "task"), uuid5(job.job_id, "run")
        task = Task.provisioning(
            task_id=task_id,
            repository_id=repository.id,
            source_ref="solver-base",
            base_revision=job.solver_case.base_commit,
            workspace_path=Path("/workspace"),
            clock=clock,
        )
        task = task.activate(workspace_path=Path("/workspace"), clock=clock)
        run = Run.queued(
            run_id=run_id,
            task_id=task_id,
            triggering_message_id=None,
            created_at=clock.now(),
            settings_snapshot=settings,
        )
        message = Message(
            new_id(),
            task_id,
            run_id,
            None,
            0,
            MessageRole.USER,
            MessageStatus.COMPLETED,
            (MessagePart(new_id(), 1, MessagePartKind.TEXT, job.solver_case.prompt),),
            clock.now(),
            clock.now(),
        )
        async with uow_factory() as uow:
            await uow.repositories.add_if_absent(repository)
            await uow.tasks.add(task)
            await uow.runs.add(run)
            await uow.messages.add(message)
            await uow.runs.set_triggering_message(run_id, message.id)
            await uow.commit()
        async with asyncio.timeout(job.wall_deadline_seconds):
            await engine.execute(run_id)
        async with uow_factory() as uow:
            terminal = await uow.runs.get(run_id)
            messages = await uow.messages.list_for_task(task_id)
        termination = "infrastructure_error"
        if terminal and terminal.status == RunStatus.COMPLETED:
            termination = "completed"
        elif terminal and terminal.outcome_code == "budget_exhausted":
            termination = "budget_exhausted"
        if provider_failed:
            termination = "provider_error"
        trace = canonical_json(messages)
        if len(trace) > job.output_limit_bytes:
            raise ValueError("worker trace exceeds cap")
        Path("/state/trace.json").write_bytes(trace)
        Path("/state/usage.json").write_bytes(b'{"authority":"host-broker"}')
        observation = WorkerObservation.model_validate(
            dict(
                schema_version=1,
                job_id=job.job_id,
                trial_id=job.trial_id,
                export_digest=job.export_digest,
                termination=termination,
                trace_ref="trace.json",
                usage_ref="usage.json",
                detail_code=terminal.outcome_code if terminal else "missing_run",
            )
        )
        temp = Path("/state/observation.tmp")
        temp.write_text(observation.model_dump_json())
        temp.replace("/state/observation.json")
    finally:
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(main())
