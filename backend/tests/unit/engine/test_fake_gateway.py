from uuid import uuid4

import pytest

from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.gateway import (
    ModelMessage,
    ModelPart,
    ModelRole,
    PreparedModelRequest,
    TextDelta,
)


def request(text: str) -> PreparedModelRequest:
    return PreparedModelRequest(
        run_id=uuid4(),
        step_id=uuid4(),
        model="fixture",
        messages=(ModelMessage(ModelRole.USER, (ModelPart("text", text),)),),
        tools=(),
        max_output_tokens=100,
    )


async def test_fake_gateway_is_deterministic_and_records_requests() -> None:
    gateway = FakeModelGateway()
    model_request = request("Explain the change")

    items = [item async for item in gateway.stream(model_request)]

    assert items[0] == TextDelta("Fake response: Explain the change")
    assert gateway.requests == [model_request]


async def test_fake_gateway_supports_injected_failure() -> None:
    gateway = FakeModelGateway(error=RuntimeError("offline"))

    with pytest.raises(RuntimeError, match="offline"):
        _ = [item async for item in gateway.stream(request("hello"))]
