from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from crucible.context.compaction import CompactionRequest, ConversationUnit
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.engine.compaction_gateway import ModelCompactionGateway
from crucible.engine.gateway import (
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)

NOW = datetime(2026, 9, 24, tzinfo=UTC)


class RecordingGateway:
    def __init__(self, text: str) -> None:
        self.text = text
        self.requests: list[PreparedModelRequest] = []

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        self.requests.append(request)
        yield TextDelta(self.text)
        yield ModelUsage(21, 8)
        yield ModelStop(ModelStopReason.COMPLETE)


def request() -> CompactionRequest:
    message = Message(
        new_id(),
        new_id(),
        None,
        None,
        1,
        MessageRole.USER,
        MessageStatus.COMPLETED,
        (MessagePart(new_id(), 1, MessagePartKind.TEXT, "keep this objective"),),
        NOW,
        NOW,
    )
    return CompactionRequest(
        message.task_id,
        (ConversationUnit((message,), True),),
        None,
        "provider/model",
        "v1",
    )


async def test_model_compaction_gateway_requires_and_normalizes_structured_json() -> (
    None
):
    gateway = RecordingGateway(
        '{"objective_and_constraints":"objective","decisions":"decision","repository_facts":"facts","changes":"changes","commands_and_validation":"checks","unresolved_problems":"problem","execution_state":"state","important_paths_and_symbols":"paths"}'
    )
    adapter = ModelCompactionGateway(gateway)

    result = await adapter.compact(request())

    assert result.objective_and_constraints == "objective"
    assert (result.input_tokens, result.output_tokens) == (21, 8)
    assert "keep this objective" in (
        gateway.requests[0].messages[0].parts[0].text_content or ""
    )


async def test_model_compaction_gateway_rejects_unstructured_output() -> None:
    with pytest.raises(ValueError, match="structured summary schema"):
        await ModelCompactionGateway(RecordingGateway("not json")).compact(request())
