import os

import pytest

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    ModelError,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    PreparedModelRequest,
)
from crucible.models.litellm_gateway import LiteLLMModelGateway

REAL_MODEL = os.environ.get("CRUCIBLE_REAL_MODEL")


@pytest.mark.skipif(
    REAL_MODEL is None,
    reason="set CRUCIBLE_REAL_MODEL and its provider credential to opt in",
)
@pytest.mark.real_model
async def test_configured_real_model_stream_smoke() -> None:
    request = PreparedModelRequest(
        new_id(),
        new_id(),
        REAL_MODEL or "",
        (
            ModelMessage(
                ModelRole.SYSTEM,
                (ModelPart("text", "This is a read-only connectivity smoke test."),),
            ),
            ModelMessage(
                ModelRole.USER,
                (ModelPart("text", "Reply with the single word ready."),),
            ),
        ),
        (),
        16,
    )

    items = [item async for item in LiteLLMModelGateway().stream(request)]

    assert not [item for item in items if isinstance(item, ModelError)]
    assert any(isinstance(item, ModelStop) for item in items)
