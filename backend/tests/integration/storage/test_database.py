from pathlib import Path

from sqlalchemy import text

from crucible.storage.database import Database


async def test_sqlite_policy_and_migrated_table_set(database: Database) -> None:
    async with database.engine.connect() as connection:
        foreign_keys = await connection.scalar(text("PRAGMA foreign_keys"))
        journal_mode = await connection.scalar(text("PRAGMA journal_mode"))
        rows = await connection.execute(
            text("SELECT name FROM sqlite_master WHERE type = 'table'")
        )

    assert foreign_keys == 1
    assert journal_mode == "wal"
    assert set(rows.scalars()) == {
        "repositories",
        "tasks",
        "runs",
        "messages",
        "message_parts",
        "task_events",
        "idempotency_records",
        "steps",
        "context_manifests",
        "tool_calls",
        "tool_results",
        "approvals",
        "artifacts",
        "external_resources",
        "alembic_version",
    }
    assert Path(database.path).exists()
