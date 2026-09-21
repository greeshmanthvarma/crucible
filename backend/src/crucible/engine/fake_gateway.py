import asyncio

from crucible.domain.conversation import MessageRole
from crucible.engine.gateway import ModelRequest


class FakeModelGateway:
    def __init__(
        self,
        *,
        barrier: asyncio.Event | None = None,
        error: Exception | None = None,
    ) -> None:
        self.requests: list[ModelRequest] = []
        self._barrier = barrier
        self._error = error

    async def complete(self, request: ModelRequest) -> str:
        self.requests.append(request)
        if self._barrier is not None:
            await self._barrier.wait()
        if self._error is not None:
            raise self._error
        newest = next(
            message
            for message in reversed(request.messages)
            if message.role is MessageRole.USER
        )
        text = "".join(part.text_content or "" for part in newest.parts)
        return f"Fake response: {text}"
