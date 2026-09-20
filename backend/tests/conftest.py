"""Shared pytest configuration for the backend test suite."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from crucible.storage.database import Database


@pytest.fixture
async def database_url(tmp_path: Path) -> AsyncIterator[str]:
    yield f"sqlite+aiosqlite:///{tmp_path / 'crucible.db'}"


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    value = await Database.create(database_url)
    yield value
    await value.dispose()
