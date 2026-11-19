from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import select

from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.storage import models
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_engine_claims_and_completes_a_run_once(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    gateway = FakeModelGateway()
    engine = RunEngine(uow_factory(database), FixedClock(), gateway)

    assert await engine.execute(submitted.run_id) is True
    assert await engine.execute(submitted.run_id) is False

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
        messages = (
            (
                await connection.execute(
                    select(models.messages)
                    .where(models.messages.c.task_id == run["task_id"])
                    .order_by(models.messages.c.conversation_sequence)
                )
            )
            .mappings()
            .all()
        )
        events = (
            (
                await connection.execute(
                    select(models.task_events)
                    .where(models.task_events.c.task_id == run["task_id"])
                    .order_by(models.task_events.c.task_sequence)
                )
            )
            .mappings()
            .all()
        )
        usage = (
            (
                await connection.execute(
                    select(models.step_usage).where(
                        models.step_usage.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        summary = (
            (
                await connection.execute(
                    select(models.run_summaries).where(
                        models.run_summaries.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .mappings()
            .one()
        )

    assert run["status"] == "completed", run["outcome_detail"]
    assert run["execution_id"] is not None
    assert run["lease_expires_at"] is None
    assert [message["conversation_sequence"] for message in messages] == [1, 2]
    assert [message["role"] for message in messages] == ["user", "assistant"]
    event_types = [event["type"] for event in events]
    assert event_types[-4:] == [
        "message.completed",
        "validation.started",
        "validation.completed",
        "run.completed",
    ]
    assert event_types.index("run.started") < event_types.index("step.preparing")
    assert len(gateway.requests) == 1
    user_message = next(
        message for message in gateway.requests[0].messages if message.role == "user"
    )
    assert user_message.parts[0].text_content == "Explain the change"
    assert len(usage) == 1
    assert usage[0]["source"] == "estimated"
    assert summary["projection_json"]["tokens"]["sources"] == ["estimated"]
    assert summary["projection_json"]["estimated_cost"] is None


async def test_gateway_failure_persists_stable_outcome(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    gateway = FakeModelGateway(error=RuntimeError("provider unavailable"))

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
        messages = (
            (
                await connection.execute(
                    select(models.messages).where(
                        models.messages.c.task_id == run["task_id"]
                    )
                )
            )
            .mappings()
            .all()
        )
        event_types = (
            (
                await connection.execute(
                    select(models.task_events.c.type)
                    .where(models.task_events.c.task_id == run["task_id"])
                    .order_by(models.task_events.c.task_sequence)
                )
            )
            .scalars()
            .all()
        )

    assert run["status"] == "failed"
    assert run["outcome_code"] == "model_gateway_error"
    assert len(messages) == 1
    assert event_types[-1] == "run.failed"
    async with database.engine.connect() as connection:
        summary = (
            (
                await connection.execute(
                    select(models.run_summaries).where(
                        models.run_summaries.c.run_id == str(submitted.run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
    assert summary["projection_json"]["outcome"] == "failed"


async def test_invalid_proposal_records_response_failure(
    database: Database, tmp_path: Path
) -> None:
    from crucible.improvement.proposal_context import ProposalStepPolicy

    submitted = await queued_run(database, tmp_path)
    gateway = FakeModelGateway()
    engine = RunEngine(
        uow_factory(database),
        FixedClock(),
        gateway,
        step_policy_factory=lambda: ProposalStepPolicy(200000, 100000),
    )
    assert await engine.execute(submitted.run_id)
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
    assert run["outcome_code"] == "model_response_invalid"
    assert run["outcome_detail"].startswith("Invalid proposal response:")


@pytest.mark.parametrize(
    "responses,max_steps,expected_status,expected_requests,feedback_count,first_tokens,code",
    [
        (
            [
                "not JSON",
                '{"decision":"decline","selected_weakness_id":"budget_exhaustion",'
                '"reason":"No justified edit.","paths":[]}',
            ],
            5,
            "completed",
            2,
            1,
            110,
            None,
        ),
        (["not JSON"] * 3, 5, "failed", 3, 2, 110, "model_response_invalid"),
        (["not JSON"], 1, "failed", 1, 0, 110, "model_response_invalid"),
        (["not JSON"], 5, "failed", 1, 1, 199999, "budget_exhausted"),
    ],
)
async def test_proposer_bounded_correction(
    database,
    tmp_path,
    responses,
    max_steps,
    expected_status,
    expected_requests,
    feedback_count,
    first_tokens,
    code,
):
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.improvement.proposal_context import ProposalStepPolicy

    class Gateway:
        def __init__(self):
            self.requests = []

        async def stream(self, request):
            self.requests.append(request)
            yield TextDelta(responses[len(self.requests) - 1])
            yield ModelUsage(first_tokens - 10, 10)
            yield ModelStop(ModelStopReason.COMPLETE)

    submitted = await queued_run(database, tmp_path)
    gateway = Gateway()
    engine = RunEngine(
        uow_factory(database),
        FixedClock(),
        gateway,
        max_steps=max_steps,
        step_policy_factory=lambda: ProposalStepPolicy(200000, 100000),
    )
    assert await engine.execute(submitted.run_id)
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
        usage = (await connection.execute(select(models.step_usage))).mappings().all()
    assert run["status"] == expected_status
    assert len(gateway.requests) == expected_requests
    assert len(usage) == expected_requests
    async with uow_factory(database)() as uow:
        messages = await uow.messages.list_for_task(UUID(run["task_id"]))
    assistant_texts = [
        part.text_content
        for m in messages
        if m.role.value == "assistant"
        for part in m.parts
        if part.text_content
    ]
    assert assistant_texts.count("not JSON") == (
        1 if expected_status == "completed" else expected_requests
    )
    feedback = [
        part.text_content
        for m in messages
        if m.role.value == "system"
        for part in m.parts
        if part.text_content
        and part.text_content.startswith("Response validation error:")
    ]
    assert len(feedback) == feedback_count
    assert all("missing decision object" in text for text in feedback)
    for request in gateway.requests[1:]:
        assert not request.tools
        instructions = "\n".join(
            p.text_content or ""
            for m in request.messages
            if m.role.value == "system"
            for p in m.parts
        )
        assert (
            "Correction" in instructions and "missing decision object" in instructions
        )
        assert "selected_weakness_id" in instructions and "required" in instructions
    if expected_status == "failed":
        assert run["outcome_code"] == code
