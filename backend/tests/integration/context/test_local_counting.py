from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.context.manager import ContextLimitExceeded, ContextManager
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.engine.gateway import ModelToolDefinition
from crucible.models.token_counting import LocalTokenEstimator
from crucible.repair.gateway import ScopedModelGateway
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_prepared_context_and_host_reservation_share_full_local_count(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    tools = (ModelToolDefinition("inspect", "Use this tool", {"type": "object"}),)
    manager = ContextManager(
        factory,
        FixedClock(),
        LocalTokenEstimator(tools=tools),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=10000,
        output_reserve=50,
        tools=tools,
    )
    prepared = await manager.prepare(run, step)

    class NoDispatch:
        async def stream(self, request):
            raise AssertionError("Model dispatch forbidden")
            yield

    gateway = ScopedModelGateway(NoDispatch(), clock=FixedClock(), endpoint="local")
    job = uuid4()
    credential = gateway.issue(
        job, "fixture", {"max_requests": 1, "max_tokens": 10000}, timedelta(minutes=1)
    )
    reservation = await gateway.reserve(credential.token, job, prepared.request)
    assert reservation.input_estimate == prepared.manifest.estimated_tokens
    assert reservation.tokens == prepared.manifest.estimated_tokens + 50
    assert (
        prepared.manifest.parameters["estimator_version"] == LocalTokenEstimator.version
    )


async def test_dense_input_without_a_safe_boundary_fails_before_model_dispatch(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(
        database, tmp_path, instructions="0123456789abcdef " * 1000
    )
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    manager = ContextManager(
        factory,
        FixedClock(),
        LocalTokenEstimator(),
        harness_policy="policy",
        tool_contract="tools",
        model="fixture",
        input_limit=1000,
        output_reserve=50,
    )
    with pytest.raises(ContextLimitExceeded, match="hard capacity"):
        await manager.prepare(run, step)
