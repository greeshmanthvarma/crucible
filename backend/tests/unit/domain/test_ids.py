import json
from datetime import UTC, datetime
from typing import Mapping, cast

import pytest

from crucible.domain.events import Event, EventType
from crucible.domain.ids import new_id


def test_new_ids_are_unique_ordered_uuid7_values() -> None:
    ids = [new_id() for _ in range(100)]

    assert all(value.version == 7 for value in ids)
    assert len(set(ids)) == 100
    assert ids == sorted(ids)


def test_event_requires_and_freezes_schema_version() -> None:
    payload: dict[str, object] = {"schema_version": 1, "items": ["original"]}
    event = Event(
        id=new_id(),
        task_id=new_id(),
        run_id=None,
        task_sequence=1,
        run_sequence=None,
        type=EventType.TASK_PROVISIONING_STARTED,
        schema_version=1,
        payload=payload,
        created_at=datetime(2026, 9, 20, tzinfo=UTC),
    )

    payload["schema_version"] = 2
    items = payload["items"]
    assert isinstance(items, list)
    items.append("mutated")

    assert event.payload == {"schema_version": 1, "items": ("original",)}
    assert json.loads(json.dumps(event.payload_json())) == {
        "schema_version": 1,
        "items": ["original"],
    }
    with pytest.raises(ValueError):
        Event(
            id=new_id(),
            task_id=new_id(),
            run_id=None,
            task_sequence=1,
            run_sequence=None,
            type=EventType.TASK_PROVISIONING_STARTED,
            schema_version=1,
            payload={},
            created_at=datetime(2026, 9, 20, tzinfo=UTC),
        )

    with pytest.raises(TypeError, match="JSON-compatible"):
        Event(
            id=new_id(),
            task_id=new_id(),
            run_id=None,
            task_sequence=1,
            run_sequence=None,
            type=EventType.TASK_PROVISIONING_STARTED,
            schema_version=1,
            payload={"schema_version": 1, "value": object()},
            created_at=datetime(2026, 9, 20, tzinfo=UTC),
        )

    with pytest.raises(TypeError, match="keys must be strings"):
        Event(
            id=new_id(),
            task_id=new_id(),
            run_id=None,
            task_sequence=1,
            run_sequence=None,
            type=EventType.TASK_PROVISIONING_STARTED,
            schema_version=1,
            payload=cast(
                Mapping[str, object],
                {"schema_version": 1, 1: "not a string key"},
            ),
            created_at=datetime(2026, 9, 20, tzinfo=UTC),
        )
