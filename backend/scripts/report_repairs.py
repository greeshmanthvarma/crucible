"""Read-only private local repair report. Output can contain private case IDs."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from crucible.domain.clock import SystemClock
from crucible.repair.identity import canonical_json
from crucible.repair.metrics import RepairMetrics
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork


async def report(database_path: Path, repair_ids: list[UUID]) -> None:
    if not database_path.is_file():
        raise ValueError("Existing database file required")
    database = await Database.create(f"sqlite+aiosqlite:///{database_path.resolve()}")
    try:
        metrics = RepairMetrics(lambda: SqlAlchemyUnitOfWork(database), SystemClock())
        print(
            json.dumps(
                json.loads(
                    canonical_json([await metrics.build(r) for r in repair_ids])
                ),
                indent=2,
            )
        )
    finally:
        await database.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("repair_ids", nargs="+", type=UUID)
    args = parser.parse_args()
    asyncio.run(report(args.database, args.repair_ids))


if __name__ == "__main__":
    main()
