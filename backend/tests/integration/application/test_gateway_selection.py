from pathlib import Path

import pytest

from crucible.application.container import ApplicationContainer
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.storage.database import Database


@pytest.mark.parametrize(
    "model", ["openai/test-model", "anthropic/test-model", "custom/test-model"]
)
async def test_all_real_models_use_responses_gateway(
    database: Database, database_url: str, tmp_path: Path, model: str
) -> None:
    container = await ApplicationContainer.create(
        database_url, tmp_path, model_id=model
    )
    try:
        assert isinstance(
            container.supervisor._engine._gateway, LiteLLMResponsesGateway
        )
    finally:
        await container.close()


async def test_fake_model_keeps_fake_gateway(
    database: Database, database_url: str, tmp_path: Path
) -> None:
    container = await ApplicationContainer.create(
        database_url, tmp_path, model_id="fake"
    )
    try:
        assert isinstance(container.supervisor._engine._gateway, FakeModelGateway)
    finally:
        await container.close()


async def test_injected_gateway_still_takes_precedence(
    database: Database, database_url: str, tmp_path: Path
) -> None:
    gateway = FakeModelGateway()
    container = await ApplicationContainer.create(
        database_url,
        tmp_path,
        model_id="custom/test-model",
        gateway_factory=lambda: gateway,
    )
    try:
        assert container.supervisor._engine._gateway is gateway
    finally:
        await container.close()
