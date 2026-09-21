from dataclasses import dataclass
from typing import Protocol

from crucible.domain.conversation import Message


@dataclass(frozen=True)
class ModelRequest:
    messages: tuple[Message, ...]


class ModelGateway(Protocol):
    async def complete(self, request: ModelRequest) -> str: ...
