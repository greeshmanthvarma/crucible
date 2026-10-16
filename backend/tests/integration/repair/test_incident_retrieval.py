"""Compact navigation and explicit, source-authorized historical detail pages."""

from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.repair import RepairAttempt
from crucible.domain.run import RunStatus
from crucible.engine.gateway import CompleteToolCall, ModelStop, ModelStopReason
from crucible.repair.controller import RepairController
from crucible.repair.evidence import EvidenceCapture
from crucible.repair.identity import canonical_json, digest
from crucible.repair.roles import IncidentTool
from crucible.repair.trials import private_artifact
from tests.integration.repair.test_incident_binding import record_failed_tool
from tests.integration.repair.test_roles import role_environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


async def test_large_capture_preview_and_detail_remain_source_authorized(
    database, tmp_path
):
    from types import SimpleNamespace

    service, task, factory, _, _, incident, artifacts = await role_environment(
        database, tmp_path, []
    )
    repair = (await service.list_for_task(task.id))[0]
    async with factory() as uow:
        run = (await uow.runs.list_for_task(task.id))[0]
    body = {
        "kind": "request",
        "complete": True,
        "source_ids": {"run_id": str(run.id)},
        "data": "large capture " * 6000,
    }
    artifact = await private_artifact(artifacts, task.id, canonical_json(body))
    capture = EvidenceCapture(
        uuid4(),
        task.id,
        run.id,
        "request",
        artifact,
        True,
        None,
        body["source_ids"],
        digest(body),
        CLOCK.now(),
        "large",
    )
    async with factory() as uow:
        await uow.repair_evidence.add(capture)
        await uow.commit()
    outcome = await IncidentTool(incident, repair.id, False).invoke(
        SimpleNamespace(), {"evidence_id": capture.id}
    )
    assert outcome.data["truncated"] is True
    assert len(outcome.display_text.encode()) < 8000
    page = await incident.read_detail(repair.id, "evidence", capture.id, 0, 4000)
    assert page["text"] == canonical_json(body).decode()[:4000]
    with pytest.raises(ValueError):
        await incident.read_detail(repair.id, "evidence", uuid4(), 0, 100)


async def test_multiple_history_reads_fit_role_context_without_larger_limits(
    database, tmp_path
):
    from crucible.context.manager import ContextManager, SimpleTokenEstimator
    from crucible.engine.run_engine import RunEngine
    from crucible.tools.dispatcher import ToolDispatcher

    outputs = [
        [
            CompleteToolCall(
                uuid4(), "read_incident_messages", {"start": 0, "limit": 50}
            ),
            ModelStop(ModelStopReason.COMPLETE),
        ]
        for _ in range(3)
    ]
    outputs.append(
        {
            "decision": "no_change_needed",
            "reasons": ["Inspected source"],
            "fix_items": [],
        }
    )
    service, task, factory, roles, provider, incident, _ = await role_environment(
        database, tmp_path, outputs
    )
    repair = (await service.list_for_task(task.id))[0]
    async with factory() as uow:
        for _ in range(12):
            await uow.messages.add(
                Message(
                    uuid4(),
                    task.id,
                    None,
                    None,
                    0,
                    MessageRole.USER,
                    MessageStatus.COMPLETED,
                    (
                        MessagePart(
                            uuid4(), 1, MessagePartKind.TEXT, "evidence " * 1200
                        ),
                    ),
                    CLOCK.now(),
                    CLOCK.now(),
                )
            )
        attempt = RepairAttempt(
            uuid4(),
            repair.id,
            None,
            "diagnosis",
            1,
            None,
            "pending",
            False,
            CLOCK.now(),
        )
        await uow.repairs.add_attempt(attempt)
        await uow.commit()

    def engine(registry):
        context = ContextManager(
            factory,
            CLOCK,
            SimpleTokenEstimator(),
            harness_policy="Protected role.",
            tool_contract="Use supplied tools.",
            model="fake",
            input_limit=20000,
            output_reserve=1000,
            tools=registry.definitions,
        )
        return RunEngine(
            factory,
            CLOCK,
            provider,
            context_manager=context,
            dispatcher=ToolDispatcher(registry, factory, CLOCK),
        )

    roles.engine_factory = engine
    result = await roles.execute(repair.id, "diagnosis", None, attempt.id, ())
    assert result.decision == "no_change_needed"
    assert len(provider.requests) == 4


async def test_history_does_not_duplicate_step_tools_and_bounds_large_parts(
    database, tmp_path
):
    service, task, factory, _, _, incident, _ = await role_environment(
        database, tmp_path, []
    )
    repair = (await service.list_for_task(task.id))[0]
    _, step, call = await record_failed_tool(
        factory, task, arguments={"path": "large.txt"}
    )
    content = "é" * 50000
    part = MessagePart(uuid4(), 1, MessagePartKind.TEXT, content)
    async with factory() as uow:
        await uow.messages.add(
            Message(
                uuid4(),
                task.id,
                call.run_id,
                step.id,
                0,
                MessageRole.TOOL,
                MessageStatus.COMPLETED,
                (part,),
                CLOCK.now(),
                CLOCK.now(),
            )
        )
        await uow.commit()
    page = await incident.read_messages(repair.id, 0, 50)
    assert len(canonical_json(page)) <= 16000
    assert page["next"] == page["total"]
    assert all(
        not item.get("tool_calls") and not item.get("tool_results")
        for item in page["messages"]
    )
    preview = next(
        p for m in page["messages"] for p in m["parts"] if p["id"] == str(part.id)
    )
    assert preview["truncated"] is True
    chunks, offset = [], 0
    while True:
        detail = await incident.read_detail(
            repair.id, "message_part", part.id, offset, 4000
        )
        chunks.append(detail["text"])
        if detail["next"] is None:
            break
        assert detail["next"] > offset
        offset = detail["next"]
    assert "".join(chunks) == content
    with pytest.raises(ValueError):
        await incident.read_detail(repair.id, "message_part", uuid4(), 0, 100)
    detail = await incident.read_detail(repair.id, "tool_call", call.id)
    import json

    assert json.loads(detail["text"])["arguments"] == {"path": "large.txt"}


@pytest.mark.parametrize("code", ["context_limit", "budget_exhausted"])
async def test_role_limits_exhaust_bounded_diagnosis_not_infrastructure_retry(
    database, tmp_path, code
):
    service, task, factory, roles, _, incident, artifacts = await role_environment(
        database, tmp_path, []
    )
    repair = (await service.list_for_task(task.id))[0]

    class LimitedEngine:
        async def execute(self, run_id):
            async with factory() as uow:
                run = await uow.runs.get(run_id)
                await uow.runs.update(
                    replace(
                        run,
                        status=RunStatus.FAILED,
                        outcome_code=code,
                        outcome_detail="PRIVATE_LIMIT_DETAIL",
                    )
                )
                await uow.commit()

    roles.engine_factory = lambda registry: LimitedEngine()
    controller = RepairController(factory, CLOCK, roles, incident, artifacts)
    await controller.execute(repair.id)
    final = await service.get(repair.id)
    assert final.status == "exhausted"
    assert code in final.outcome_detail
    assert "PRIVATE_LIMIT_DETAIL" not in final.outcome_detail
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
    assert len(attempts) == 3
    assert all(a.charged and a.status == "invalid_output" for a in attempts)
