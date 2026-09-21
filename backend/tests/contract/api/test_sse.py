import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

from crucible.api.app import create_app
from crucible.api.events import _sse_event, event_stream
from crucible.application.event_service import TaskEventSource
from crucible.engine.notifier import TaskEventNotifier
from crucible.storage.database import Database
from tests.integration.application.test_event_replay import task_id_for_run
from tests.integration.engine.conftest import queued_run, uow_factory


class ConnectedRequest:
    async def is_disconnected(self) -> bool:
        return False


def request_for(app: object, *, disconnected: bool = False) -> Request:
    async def receive() -> dict[str, Any]:
        if disconnected:
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {"type": "http", "method": "GET", "path": "/", "headers": [], "app": app},
        receive,
    )


async def test_sse_envelope_cursor_errors_and_heartbeat(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    task_id = await task_id_for_run(database, submitted.run_id)
    source = TaskEventSource(
        uow_factory(database), TaskEventNotifier(), heartbeat_seconds=0.001
    )
    events = await source.list_after(task_id, 0)

    wire = _sse_event(events[-1])
    lines = wire.strip().splitlines()
    assert lines[0] == f"id: {events[-1].id}"
    assert lines[1] == "event: task_event"
    payload = json.loads(lines[2].removeprefix("data: "))
    assert payload["taskSequence"] == 3
    assert payload["runSequence"] == 1
    assert payload["createdAt"].endswith("Z")

    heartbeat = source.follow(
        ConnectedRequest().is_disconnected, task_id, events[-1].task_sequence
    )
    assert await anext(heartbeat) is None
    await heartbeat.aclose()

    app = create_app(event_source=source)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        unknown_task = await client.get(f"/api/tasks/{uuid4()}/events")
        invalid_cursor = await client.get(
            f"/api/tasks/{task_id}/events",
            headers={"Last-Event-ID": str(uuid4())},
        )

    assert unknown_task.status_code == 404
    assert invalid_cursor.status_code == 409
    assert invalid_cursor.json()["code"] == "event_cursor_not_found"

    initial = await event_stream(task_id, request_for(app), None)
    initial_body = initial.body_iterator
    first_wire = await anext(initial_body)
    assert f"id: {events[0].id}" in first_wire
    await initial_body.aclose()

    replay = await event_stream(task_id, request_for(app), str(events[0].id))
    replay_body = replay.body_iterator
    replay_wires = [await anext(replay_body), await anext(replay_body)]
    delivered = [
        json.loads(wire.split("data: ", 1)[1])["taskSequence"] for wire in replay_wires
    ]
    assert delivered == [2, 3]
    await replay_body.aclose()

    disconnected = await event_stream(
        task_id, request_for(app, disconnected=True), None
    )
    with pytest.raises(StopAsyncIteration):
        await anext(disconnected.body_iterator)


def test_sse_route_is_in_openapi() -> None:
    assert "/api/tasks/{task_id}/events" in create_app().openapi()["paths"]
