import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from crucible.engine.gateway import (
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    PreparedModelRequest,
    TextDelta,
)


@dataclass(frozen=True)
class ScriptedExchange:
    expected_request: PreparedModelRequest
    items: tuple[ModelStreamItem, ...]


class ScriptedModelGateway:
    def __init__(self, exchanges: tuple[ScriptedExchange, ...]) -> None:
        self._exchanges = exchanges
        self.requests: list[PreparedModelRequest] = []

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        index = len(self.requests)
        if index >= len(self._exchanges):
            raise AssertionError("Unexpected model request")
        exchange = self._exchanges[index]
        if request != exchange.expected_request:
            raise AssertionError(
                f"Model request {index + 1} did not match scripted context"
            )
        self.requests.append(request)
        for item in exchange.items:
            yield item

    def assert_exhausted(self) -> None:
        if len(self.requests) != len(self._exchanges):
            raise AssertionError(
                f"Missing model requests: expected {len(self._exchanges)}, "
                f"received {len(self.requests)}"
            )


class FakeModelGateway:
    def __init__(
        self,
        *,
        barrier: asyncio.Event | None = None,
        error: Exception | None = None,
    ) -> None:
        self.requests: list[PreparedModelRequest] = []
        self._barrier = barrier
        self._error = error

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        self.requests.append(request)
        if self._barrier is not None:
            await self._barrier.wait()
        if self._error is not None:
            raise self._error
        newest = next(
            message for message in reversed(request.messages) if message.role == "user"
        )
        text = "".join(part.text_content or "" for part in newest.parts)
        yield TextDelta(f"Fake response: {text}")
        yield ModelStop(ModelStopReason.COMPLETE)
