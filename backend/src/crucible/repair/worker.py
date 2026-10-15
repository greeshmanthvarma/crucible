"""Protected worker entry point. Candidate imports happen only in the container."""

import asyncio
import json
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid5

# `python -I` ignores PYTHONPATH. The controller mounts the frozen export here.
if __name__ == "__main__":
    if not Path("/.dockerenv").exists() or not Path("/job/request.json").is_file():
        raise SystemExit("Worker entry requires a controller-launched Docker job")
    sys.path.insert(0, "/harness/backend/src")


def bounded_detail(value: str) -> str:
    return value.encode("utf-8")[:1024].decode("utf-8", errors="ignore")


class WorkerInfrastructureFailure(Exception):
    """Model transport/provider errors are never incident reproduction."""


class WorkerBehaviorFailure(Exception):
    def __init__(self, error: Exception) -> None:
        self.code = type(error).__name__
        self.detail = bounded_detail(str(error))
        super().__init__(self.detail)


@contextmanager
def behavior_errors() -> Iterator[None]:
    """Classify exceptions only inside the admitted behavior entry boundary."""
    from sqlalchemy.exc import SQLAlchemyError

    try:
        yield
    except (OSError, TimeoutError, SQLAlchemyError):
        raise
    except Exception as error:
        cause: BaseException | None = error
        seen: set[int] = set()
        while cause is not None and id(cause) not in seen:
            seen.add(id(cause))
            if isinstance(
                cause,
                (WorkerInfrastructureFailure, OSError, TimeoutError, SQLAlchemyError),
            ):
                raise WorkerInfrastructureFailure(
                    "Model or worker infrastructure failed"
                ) from error
            cause = cause.__cause__ or cause.__context__
        raise WorkerBehaviorFailure(error) from error


async def execute_job(job: Any) -> Any:
    from alembic import command
    from alembic.config import Config
    from pydantic import TypeAdapter

    from crucible.application.container import ApplicationContainer
    from crucible.context.compaction import CompactionLifecycle
    from crucible.context.invariants import validate_compaction_boundary
    from crucible.context.selection import (
        group_conversation_units,
        select_compaction_boundary,
    )
    from crucible.domain.clock import SystemClock
    from crucible.domain.conversation import (
        Message,
        MessagePart,
        MessagePartKind,
        MessageRole,
        MessageStatus,
    )
    from crucible.domain.evals import EvalBudgets
    from crucible.domain.ids import new_id
    from crucible.domain.repository import Repository
    from crucible.domain.run import Run
    from crucible.domain.steps import Step
    from crucible.domain.task import Task
    from crucible.domain.tools import ToolCall, ToolResult
    from crucible.engine.compaction_gateway import ModelCompactionGateway
    from crucible.engine.gateway import (
        CompleteToolCall,
        ModelError,
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.repair.gateway import WorkerCredential
    from crucible.repair.identity import canonical_json
    from crucible.repair.incontainer_commands import InContainerSandbox
    from crucible.repair.worker_gateway import WorkerModelGateway
    from crucible.repair.worker_protocol import WorkerObservation
    from crucible.tools.dispatcher import DispatchContext

    state, workspace = Path("/state"), Path("/workspace")
    # Only this disposable repository is trusted across the host/worker UID change.
    process = await asyncio.create_subprocess_exec(
        "git", "config", "--global", "--add", "safe.directory", "/workspace"
    )
    if await process.wait() != 0:
        raise ValueError("Cannot configure disposable fixture repository")
    configuration = Config("/harness/backend/alembic.ini")
    configuration.set_main_option("script_location", "/harness/backend/migrations")
    url = "sqlite+aiosqlite:////state/worker.db"
    configuration.set_main_option("sqlalchemy.url", url)
    await asyncio.to_thread(command.upgrade, configuration, "head")
    manifest = job.input_manifest
    settings = job.settings
    clock = SystemClock()
    model = str(settings.get("model", "recorded-replay"))

    class RecordedGateway:
        async def stream(self, request: Any) -> Any:
            raw = manifest.get("raw_response")
            if not isinstance(raw, str):
                raise ValueError("Recorded compaction response is missing")
            yield TextDelta(raw)
            yield ModelUsage(None, None)
            yield ModelStop(ModelStopReason.COMPLETE)

    token, endpoint = (
        os.environ.get("REPAIR_GATEWAY_TOKEN"),
        os.environ.get("REPAIR_GATEWAY_ENDPOINT"),
    )
    if token and endpoint:
        model_gateway: Any = WorkerModelGateway(
            WorkerCredential(token, endpoint), job.job_id
        )
    else:
        model_gateway = RecordedGateway()

    class CapturingGateway:
        def __init__(self) -> None:
            self.responses: list[str] = []

        async def stream(self, request: Any) -> Any:
            chunks = []
            try:
                async for event in model_gateway.stream(request):
                    if isinstance(event, ModelError):
                        raise WorkerInfrastructureFailure(
                            "Model request could not complete"
                        )
                    if isinstance(event, TextDelta):
                        chunks.append(event.text)
                    yield event
            except Exception as error:
                raise WorkerInfrastructureFailure(
                    "Model request could not complete"
                ) from error
            self.responses.append("".join(chunks))

    capture_gateway = CapturingGateway()
    container = await ApplicationContainer.create(
        url,
        state,
        gateway_factory=lambda: capture_gateway,
        model_id=model,
        context_limits=(
            int(settings.get("input_limit", 100_000)),
            int(settings.get("output_reserve", 4096)),
        ),
        worker_job=job,
        run_budgets=TypeAdapter(EvalBudgets).validate_python(manifest["budgets"])
        if manifest.get("kind") == "repository"
        else None,
    )
    task_id, run_id = uuid5(job.job_id, "task"), uuid5(job.job_id, "run")
    try:
        from crucible.domain.repository import RepositorySettings

        repository = Repository(
            new_id(),
            workspace,
            clock.now(),
            TypeAdapter(RepositorySettings).validate_python(
                manifest["repository_settings"]
            )
            if manifest.get("kind") == "repository"
            else RepositorySettings(),
        )
        task = Task.provisioning(
            task_id=task_id,
            repository_id=repository.id,
            source_ref="worker-fixture",
            base_revision=job.source_digest,
            workspace_path=workspace,
            clock=clock,
        )
        task = task.activate(workspace_path=workspace, clock=clock)
        run = Run.queued(
            run_id=run_id,
            task_id=task_id,
            triggering_message_id=None,
            created_at=clock.now(),
            settings_snapshot=repository.settings,
        )
        step = Step.preparing(new_id(), task_id, run_id, 1, clock.now())
        async with container.unit_of_work() as uow:
            await uow.repositories.add_if_absent(repository)
            await uow.tasks.add(task)
            await uow.runs.add(run)
            if manifest.get("kind") != "repository":
                await uow.steps.add(step)
            await uow.commit()
        kind = manifest.get("kind")
        if kind == "repository":
            from crucible.domain.run import RunStatus

            prompt = manifest.get("prompt")
            if not isinstance(prompt, str):
                raise ValueError("Repository prompt is missing")
            message = Message(
                new_id(),
                task_id,
                run_id,
                None,
                0,
                MessageRole.USER,
                MessageStatus.COMPLETED,
                (MessagePart(new_id(), 1, MessagePartKind.TEXT, prompt),),
                clock.now(),
                clock.now(),
            )
            async with container.unit_of_work() as uow:
                await uow.messages.add(message)
                await uow.runs.set_triggering_message(run_id, message.id)
                await uow.commit()
            await container.supervisor.submit(run_id)
            try:
                while True:
                    async with container.unit_of_work() as uow:
                        terminal = await uow.runs.get(run_id)
                    if terminal and terminal.status not in {
                        RunStatus.QUEUED,
                        RunStatus.RUNNING,
                        RunStatus.VALIDATING,
                    }:
                        return WorkerObservation(
                            job_id=job.job_id,
                            status="completed"
                            if terminal.status == RunStatus.COMPLETED
                            else "error",
                            error_code=None
                            if terminal.status == RunStatus.COMPLETED
                            else "repository_run_failed",
                            tool_result={
                                "run_status": terminal.status.value,
                                "run_outcome_code": terminal.outcome_code,
                            },
                            detail=bounded_detail(terminal.outcome_detail or "")
                            or None,
                        )
                    await asyncio.sleep(0.05)
            except BaseException:
                await container.supervisor.cancel(run_id)
                raise
        if kind == "tool":
            assistant = Message(
                new_id(),
                task_id,
                run_id,
                step.id,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (
                    MessagePart(
                        new_id(), 1, MessagePartKind.TEXT, "Incident reproduction"
                    ),
                ),
                clock.now(),
                clock.now(),
            )
            name, arguments = manifest.get("tool_name"), manifest.get("arguments")
            if not isinstance(name, str) or not isinstance(arguments, dict):
                raise ValueError("Missing captured tool inputs")
            results = await container.tool_dispatcher.execute_batch(
                DispatchContext(
                    task_id,
                    run_id,
                    step.id,
                    assistant.id,
                    workspace,
                    sandbox_image=os.environ["CRUCIBLE_REPAIR_IMAGE"],
                ),
                (CompleteToolCall(new_id(), name, arguments),),
                assistant_message=assistant,
            )
            result = results[0]
            records = (
                container.command_sandbox.records
                if isinstance(container.command_sandbox, InContainerSandbox)
                else []
            )
            return WorkerObservation.model_validate(
                {
                    "job_id": job.job_id,
                    "status": "completed" if result.error_code is None else "error",
                    "tool_result": {
                        "status": result.status.value,
                        "output_text": result.display_text,
                        "data": dict(result.result),
                    },
                    "error_code": result.error_code,
                    "detail": result.display_text if result.error_code else None,
                    "command_records": json.loads(canonical_json(records)),
                }
            )
        if kind not in {"context_selection", "compaction"}:
            return WorkerObservation(
                job_id=job.job_id,
                status="unsupported",
                error_code="unsupported_case",
                detail="Unsupported incident kind",
            )
        # Supervisor resolves artifact references into captured data. No arbitrary
        # host paths or generated source fixtures are accepted here.
        if not isinstance(manifest.get("messages"), list):
            return WorkerObservation(
                job_id=job.job_id,
                status="unsupported",
                error_code="missing_context_capture",
                detail="Complete captured context inputs are required",
            )
        messages = TypeAdapter(tuple[Message, ...]).validate_python(
            manifest.get("messages")
        )
        calls = TypeAdapter(tuple[ToolCall, ...]).validate_python(
            manifest.get("calls", [])
        )
        results = TypeAdapter(tuple[ToolResult, ...]).validate_python(
            manifest.get("results", [])
        )
        remapped = tuple(
            replace(message, task_id=task_id, run_id=run_id, step_id=step.id)
            for message in messages
        )
        # Original tool exchanges retain their IDs/correlation links but use the
        # private job's identities and assistant message associations.
        async with container.unit_of_work() as uow:
            for message in remapped:
                await uow.messages.add(message)
            for call in calls:
                await uow.tool_calls.add(
                    replace(call, task_id=task_id, run_id=run_id, step_id=step.id)
                )
            for result in results:
                await uow.tool_results.add(
                    replace(result, task_id=task_id, run_id=run_id, step_id=step.id)
                )
            await uow.commit()
        if manifest.get("prior_compaction") is not None:
            from crucible.domain.compaction import Compaction

            previous = TypeAdapter(Compaction).validate_python(
                manifest["prior_compaction"]
            )
            # Artifact references are private worker copies resolved by supervisor.
            artifact_text = manifest.get("prior_summary")
            if not isinstance(artifact_text, str):
                raise ValueError("Prior summary artifact is missing")

            async def content() -> Any:
                yield artifact_text.encode()

            artifact = await container.artifact_service.put(
                task_id, "text/markdown", "private", content()
            )
            async with container.unit_of_work() as uow:
                await uow.compactions.add(
                    replace(
                        previous,
                        task_id=task_id,
                        summary_artifact_id=artifact.id,
                        previous_compaction_id=None,
                    )
                )
                await uow.commit()
        with behavior_errors():
            if kind == "compaction":
                units = group_conversation_units(
                    remapped, {result.id: result.tool_call_id for result in results}
                )
                boundary = select_compaction_boundary(
                    units,
                    retain_complete_units=int(settings.get("retain_complete_units", 0)),
                )
                if boundary is None:
                    raise ValueError(
                        "Captured context has no complete compaction prefix"
                    )
                validate_compaction_boundary(
                    boundary,
                    remapped,
                    {result.id: result.tool_call_id for result in results},
                )
                lifecycle = CompactionLifecycle(
                    container.unit_of_work,
                    container.artifact_service,
                    ModelCompactionGateway(capture_gateway),
                    clock,
                    attempt_limit=1,
                    model=model,
                )
                await lifecycle.compact(run, boundary, step_id=step.id)
            prepared = await container.context_manager.prepare(run, step)
        summary = None
        if kind == "compaction":
            # The parser has already validated the response and lifecycle stored it.
            # Recorded text is reused only when explicitly supplied by the controller.
            if capture_gateway.responses:
                summary = json.loads(capture_gateway.responses[-1])
        context_messages = json.loads(canonical_json(prepared.request.messages))
        retained = []
        sources = settings.get("source_messages", [])
        for message in context_messages:
            match = next(
                (
                    source
                    for source in sources
                    if isinstance(source, dict)
                    and {
                        key: value
                        for key, value in source.items()
                        if key != "conversation_sequence"
                    }
                    == message
                ),
                None,
            )
            if match is not None:
                retained.append(match)
            elif message.get("role") != "system":
                retained.append(message)
        return WorkerObservation.model_validate(
            {
                "job_id": job.job_id,
                "status": "completed",
                "context_messages": context_messages,
                "tool_result": {"retained_messages": retained},
                "summary": summary,
            }
        )
    finally:
        await container.close()


async def main() -> None:
    from crucible.repair.worker_protocol import WorkerJob, WorkerObservation

    raw = Path("/job/request.json").read_bytes()
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("Job manifest too large")
    job = WorkerJob.model_validate_json(raw)
    try:
        async with asyncio.timeout(job.deadline_seconds):
            observation = await execute_job(job)
    except TimeoutError:
        observation = WorkerObservation(
            job_id=job.job_id,
            status="error",
            error_code="deadline_exceeded",
            detail="Worker deadline exceeded",
        )
    except WorkerBehaviorFailure as error:
        observation = WorkerObservation(
            job_id=job.job_id,
            status="error",
            error_code=error.code,
            detail=error.detail,
        )
    except Exception as error:
        observation = WorkerObservation(
            job_id=job.job_id,
            status="error",
            error_code="worker_execution_error",
            detail=type(error).__name__,
        )
    payload = observation.model_dump_json().encode()
    token = os.environ.get("REPAIR_GATEWAY_TOKEN")
    if token and token.encode() in payload:
        observation = WorkerObservation(
            job_id=job.job_id,
            status="unsupported",
            error_code="credential_output_redacted",
            detail="Observation contained a scoped credential",
        )
        payload = observation.model_dump_json().encode()
    if len(payload) > job.observation_bytes:
        payload = (
            WorkerObservation(
                job_id=job.job_id,
                status="unsupported",
                error_code="observation_limit",
                detail="Essential observation exceeds the byte limit",
            )
            .model_dump_json()
            .encode()
        )
    # The only envelope path is fixed and owned by the worker job.
    temporary = Path("/state/observation.tmp")
    temporary.write_bytes(payload)
    temporary.replace("/state/observation.json")


if __name__ == "__main__":
    asyncio.run(main())
