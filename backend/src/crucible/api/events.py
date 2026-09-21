import json
from collections.abc import AsyncIterator
from datetime import UTC
from uuid import UUID

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse

from crucible.application.errors import EventCursorNotFound
from crucible.application.event_service import TaskEventSource
from crucible.domain.events import Event

router = APIRouter(tags=["events"])


def _sse_event(event: Event) -> str:
    created_at = event.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    data = {
        "eventId": str(event.id),
        "taskId": str(event.task_id),
        "runId": str(event.run_id) if event.run_id else None,
        "taskSequence": event.task_sequence,
        "runSequence": event.run_sequence,
        "type": event.type,
        "schemaVersion": event.schema_version,
        "payload": event.payload_json(),
        "createdAt": created_at,
    }
    return (
        f"id: {event.id}\n"
        "event: task_event\n"
        f"data: {json.dumps(data, separators=(',', ':'))}\n\n"
    )


@router.get("/api/tasks/{task_id}/events")
async def event_stream(
    task_id: UUID,
    request: Request,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    source: TaskEventSource = request.app.state.event_source
    try:
        cursor = UUID(last_event_id) if last_event_id else None
    except ValueError as error:
        raise EventCursorNotFound("Event cursor is not a valid UUID") from error
    sequence = await source.resolve_cursor(task_id, cursor)

    async def encode_stream() -> AsyncIterator[str]:
        async for event in source.follow(request.is_disconnected, task_id, sequence):
            yield _sse_event(event) if event is not None else ": heartbeat\n\n"

    return StreamingResponse(
        encode_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
