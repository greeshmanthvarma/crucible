"""Solver evidence retains the structured exchange from durable storage."""

import json

from crucible.domain.ids import new_id
from crucible.domain.tools import (
    ToolCall,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import NOW, seed_exchange


async def test_trace_exports_tool_linkage_and_sanitizes_secrets(database, monkeypatch):
    from crucible.improvement.worker import export_solver_trace

    monkeypatch.setenv("TEST_API_KEY", "private-canary")
    async with SqlAlchemyUnitOfWork(database) as unit:
        task, run, step, message = await seed_exchange(unit, "trace")
        call = ToolCall(
            new_id(),
            task.id,
            run.id,
            step.id,
            message.id,
            1,
            "read_file",
            {"path": "example.py", "secret": "private-canary"},
            1,
            "provider-call-1",
            ToolExecutionMode.SEQUENTIAL,
            NOW,
        )
        await unit.tool_calls.add(call)
        result = ToolResult(
            new_id(),
            task.id,
            run.id,
            step.id,
            call.id,
            ToolResultStatus.SUCCEEDED,
            {"lines": ["return 42"], "ok": True},
            1,
            "display summary",
            None,
            1,
            NOW,
            NOW,
        )
        await unit.tool_results.add(result)
        await unit.commit()
    async with SqlAlchemyUnitOfWork(database) as unit:
        raw = await export_solver_trace(unit, task.id, run.id, 65536)
    trace = json.loads(raw)
    assert trace["messages"][0]["id"] == str(message.id)
    assert trace["tool_calls"][0]["name"] == "read_file"
    assert trace["tool_calls"][0]["arguments"]["path"] == "example.py"
    assert trace["tool_calls"][0]["assistant_message_id"] == str(message.id)
    assert trace["tool_calls"][0]["provider_correlation_id"] == "provider-call-1"
    assert trace["tool_results"][0]["tool_call_id"] == str(call.id)
    assert trace["tool_results"][0]["result"] == {"lines": ["return 42"], "ok": True}
    assert b"private-canary" not in raw
    async with SqlAlchemyUnitOfWork(database) as unit:
        import pytest

        with pytest.raises(ValueError, match="trace exceeds cap"):
            await export_solver_trace(unit, task.id, run.id, 16)
