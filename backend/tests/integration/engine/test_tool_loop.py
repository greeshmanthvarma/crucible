from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy import select

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    PreparedModelRequest,
    TextDelta,
)
from crucible.engine.run_engine import RunEngine
from crucible.storage import models
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


class TwoStepGateway:
    def __init__(self) -> None:
        self.requests: list[PreparedModelRequest] = []

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        self.requests.append(request)
        if len(self.requests) == 1:
            yield TextDelta("Inspecting.")
            yield CompleteToolCall(new_id(), "read_file", {"path": "README.md"})
            yield ModelStop(ModelStopReason.TOOL_CALLS)
        else:
            yield TextDelta("The fixture contains fixture.")
            yield ModelStop(ModelStopReason.COMPLETE)


async def test_two_step_tool_trajectory_persists_exact_exchange(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "tool-loop")
    gateway = TwoStepGateway()

    assert await RunEngine(uow_factory(database), FixedClock(), gateway).execute(
        submitted.run_id
    )

    async with database.engine.connect() as connection:
        run = (
            (
                await connection.execute(
                    select(models.runs).where(models.runs.c.id == str(submitted.run_id))
                )
            )
            .mappings()
            .one()
        )
        roles = (
            (
                await connection.execute(
                    select(models.messages.c.role)
                    .where(models.messages.c.task_id == run["task_id"])
                    .order_by(models.messages.c.conversation_sequence)
                )
            )
            .scalars()
            .all()
        )
        steps = (
            (
                await connection.execute(
                    select(models.steps.c.status)
                    .where(models.steps.c.run_id == str(submitted.run_id))
                    .order_by(models.steps.c.step_sequence)
                )
            )
            .scalars()
            .all()
        )
        calls = (
            (
                await connection.execute(
                    select(models.tool_calls.c.name).where(
                        models.tool_calls.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        results = (
            (
                await connection.execute(
                    select(models.tool_results.c.status).where(
                        models.tool_results.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .scalars()
            .all()
        )

    assert run["status"] == "completed"
    assert roles == ["user", "assistant", "tool", "assistant"]
    assert steps == ["completed", "completed"]
    assert calls == ["read_file"]
    assert results == ["succeeded"]
    assert len(gateway.requests) == 2
    assert gateway.requests[1].messages[-1].role == "tool"
