"""Bounded solver-visible evidence without controller artifact access."""

import json
import os
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.tools import ToolCall, ToolResult
from crucible.repair.identity import canonical_json


def _sanitize(value: object, secrets: tuple[str, ...]) -> object:
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, dict):
        return {str(key): _sanitize(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item, secrets) for item in value]
    return value


async def export_solver_trace(
    uow: UnitOfWork,
    task_id: UUID,
    run_id: UUID,
    maximum: int,
    credential: str = "",
) -> bytes:
    """Export solver-visible records; never dereference result artifact paths."""
    messages = await uow.messages.list_for_task(task_id)
    calls: list[ToolCall] = []
    results: list[ToolResult] = []
    for step in await uow.steps.list_for_run(run_id):
        calls.extend(await uow.tool_calls.list_for_step(step.id))
        results.extend(await uow.tool_results.list_for_step(step.id))
    secrets = tuple(
        value
        for name, value in os.environ.items()
        if value and name.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD"))
    )
    if credential:
        secrets += (credential,)
    raw = canonical_json(
        _sanitize(
            json.loads(
                canonical_json(
                    {
                        "schema_version": 2,
                        "messages": [
                            message for message in messages if message.run_id == run_id
                        ],
                        "tool_calls": calls,
                        "tool_results": results,
                    }
                )
            ),
            secrets,
        )
    )
    if len(raw) > maximum:
        raise ValueError("worker trace exceeds cap")
    return raw
