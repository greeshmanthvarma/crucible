import inspect

from crucible.domain.resources import ExternalResource
from crucible.sandbox.protocol import (
    OutputCallback,
    OutputChunk,
    SandboxOutcome,
    SandboxRequest,
)


class FakeSandboxBackend:
    def __init__(
        self, outcome: SandboxOutcome, chunks: tuple[OutputChunk, ...] = ()
    ) -> None:
        self._outcome = outcome
        self._chunks = chunks
        self.requests: list[SandboxRequest] = []
        self.cancelled: list[str] = []
        self.reconciled: list[tuple[ExternalResource, ...]] = []

    async def execute(
        self, request: SandboxRequest, on_chunk: OutputCallback
    ) -> SandboxOutcome:
        self.requests.append(request)
        for chunk in self._chunks:
            emitted = on_chunk(chunk)
            if inspect.isawaitable(emitted):
                await emitted
        return self._outcome

    async def cancel(self, container_id: str) -> None:
        self.cancelled.append(container_id)

    async def reconcile(self, resources: tuple[ExternalResource, ...]) -> None:
        self.reconciled.append(resources)
