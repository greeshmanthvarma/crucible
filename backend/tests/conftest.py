"""Shared pytest configuration for the backend test suite."""

import asyncio
import shutil
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from crucible.storage.database import Database


@pytest.fixture(scope="session")
def migrated_database_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    template = tmp_path_factory.mktemp("migrated") / "template.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{template}")
    command.upgrade(config, "head")
    if any(template.with_name(template.name + s).exists() for s in ("-wal", "-shm")):
        raise RuntimeError("migrated template left an open SQLite journal")
    return template


@pytest.fixture
async def database_url(tmp_path: Path) -> AsyncIterator[str]:
    yield f"sqlite+aiosqlite:///{tmp_path / 'crucible.db'}"


@pytest.fixture
async def database(
    database_url: str, migrated_database_template: Path
) -> AsyncIterator[Database]:
    path = Path(database_url.removeprefix("sqlite+aiosqlite:///"))
    await asyncio.to_thread(shutil.copyfile, migrated_database_template, path)
    value = await Database.create(database_url)
    yield value
    await value.dispose()
