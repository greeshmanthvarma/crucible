"""Untrusted interface observations only. All assertions execute outside Docker."""

import asyncio
import json
from pathlib import Path
from uuid import UUID

from crucible.application.worker_container import create_worker_engine
from crucible.engine import coding_behavior
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelStop,
    ModelStopReason,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)
from crucible.improvement.worker import seed_solver_task
from crucible.improvement.worker_protocol import WorkerJob
from crucible.repair.identity import canonical_json
from crucible.storage.database import Database
from crucible.storage.models import metadata
from crucible.tools.workspace_authority import WorkspacePathResolver


async def main() -> None:
    operation = json.loads(Path("/job/probe.json").read_bytes())["operation"]
    job = WorkerJob.model_validate_json(Path("/job/request.json").read_bytes())
    value: dict[str, object]
    if operation == "guidance":
        observation = coding_behavior.BehaviorObservation(1, 3, 8, 100, 30.0, (), False)
        value = {
            "coding": coding_behavior.coding_guidance(),
            "step": coding_behavior.step_guidance(observation),
        }
    elif operation == "workspace":
        value = {"error": "not_rejected"}
        try:
            WorkspacePathResolver(Path("/workspace")).resolve("../outside")
        except ValueError:
            value = {"error": "workspace_escape"}
    else:

        class ScriptedGateway:
            def __init__(self) -> None:
                self.requests: list[PreparedModelRequest] = []

            async def stream(self, request):  # type: ignore[no-untyped-def]
                self.requests.append(request)
                if len(self.requests) <= 2:
                    yield CompleteToolCall(
                        UUID(int=len(self.requests)),
                        "read_file",
                        {
                            "path": "missing.txt"
                            if len(self.requests) == 1
                            else "README.md"
                        },
                    )
                    yield ModelUsage(1, 1)
                    yield ModelStop(ModelStopReason.TOOL_CALLS)
                else:
                    yield TextDelta("Inspected repository.")
                    yield ModelUsage(1, 1)
                    yield ModelStop(ModelStopReason.COMPLETE)

        gateway = ScriptedGateway()
        database = await Database.create("sqlite+aiosqlite:////state/probe.sqlite3")
        try:
            async with database.engine.begin() as conn:
                await conn.run_sync(metadata.create_all)
            engine, uow_factory = create_worker_engine(
                database,
                job,
                gateway,
                "no-authority",
                reduced_model_budget=1 if operation == "budget" else None,
            )
            run_id = await seed_solver_task(job, uow_factory)
            await engine.execute(run_id)
            async with uow_factory() as uow:
                run = await uow.runs.get(run_id)
                if run is None:
                    raise ValueError("missing run")
                attempts = await uow.validation_attempts.list_for_run(run_id)
                tools = []
                for step in await uow.steps.list_for_run(run_id):
                    calls = await uow.tool_calls.list_for_step(step.id)
                    results = await uow.tool_results.list_for_step(step.id)
                    tools.extend(
                        [
                            [c.name, r.status.value]
                            for c, r in zip(calls, results, strict=True)
                        ]
                    )
            value = {
                "requests": json.loads(canonical_json(gateway.requests)),
                "tools": tools,
                "validation": [a.status.value for a in attempts],
                "outcome": run.outcome_code,
                "file": "controller-will-read-stopped-workspace",
            }
        finally:
            await database.dispose()
    payload = canonical_json(value)
    if len(payload) > job.output_limit_bytes:
        raise ValueError("probe output cap")
    Path("/state/probe.json").write_bytes(payload)


if __name__ == "__main__":
    asyncio.run(main())
