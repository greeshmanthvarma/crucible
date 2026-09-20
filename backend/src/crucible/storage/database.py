from dataclasses import dataclass

from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@dataclass(frozen=True)
class Database:
    engine: AsyncEngine
    path: str

    @classmethod
    async def create(cls, url: str) -> "Database":
        engine = create_async_engine(url)

        @event.listens_for(engine.sync_engine, "connect")
        def configure_sqlite(dbapi_connection: object, _: object) -> None:
            cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

        database = make_url(url).database
        if database is None or database == ":memory:":
            raise ValueError("Database requires a file-backed SQLite URL")
        async with engine.begin() as connection:
            mode = await connection.scalar(text("PRAGMA journal_mode=WAL"))
            if mode != "wal":
                raise RuntimeError(f"SQLite refused WAL mode: {mode}")
        return cls(engine=engine, path=database)

    async def dispose(self) -> None:
        await self.engine.dispose()
