from datetime import UTC, datetime
from uuid import uuid4

import pytest

from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.gateway import ModelRequest


def request(text: str) -> ModelRequest:
    now = datetime(2026, 9, 20, tzinfo=UTC)
    return ModelRequest(
        (
            Message(
                uuid4(),
                uuid4(),
                None,
                None,
                1,
                MessageRole.USER,
                MessageStatus.COMPLETED,
                (MessagePart(uuid4(), 1, MessagePartKind.TEXT, text),),
                now,
                now,
            ),
        )
    )


async def test_fake_gateway_is_deterministic_and_records_requests() -> None:
    gateway = FakeModelGateway()
    model_request = request("Explain the change")

    assert await gateway.complete(model_request) == "Fake response: Explain the change"
    assert gateway.requests == [model_request]


async def test_fake_gateway_supports_injected_failure() -> None:
    gateway = FakeModelGateway(error=RuntimeError("offline"))

    with pytest.raises(RuntimeError, match="offline"):
        await gateway.complete(request("hello"))
