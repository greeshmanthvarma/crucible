from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.run_service import RunService
from crucible.domain.run import RunStatus
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


class CancellingSupervisor:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.calls = 0

    async def submit(self, run_id) -> None:
        return None

    async def reconcile(self) -> None:
        return None

    async def cancel(self, run_id) -> None:
        self.calls += 1
        async with SqlAlchemyUnitOfWork(self.database) as uow:
            run = await uow.runs.get(run_id)
            assert run is not None
            await uow.runs.update(
                run.cancel("cancelled", "Run cancelled by user", now=NOW)
            )
            await uow.commit()


async def test_cancel_endpoint_is_terminal_and_idempotent(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        _task, run, _step, _message = await seed_exchange(uow, "cancel-api")
        await uow.commit()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    supervisor = CancellingSupervisor(database)
    service = RunService(factory, CLOCK, supervisor)
    app = create_app(run_service=service)
    endpoint = f"/api/runs/{run.id}/cancel"

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        missing = await client.post(endpoint)
        cancelled = await client.post(endpoint, headers={"Idempotency-Key": "cancel-1"})
        replay = await client.post(endpoint, headers={"Idempotency-Key": "cancel-1"})

    assert missing.status_code == 400
    assert cancelled.json()["status"] == RunStatus.CANCELLED
    assert cancelled.json()["outcomeCode"] == "cancelled"
    assert replay.json() == cancelled.json()
    assert supervisor.calls == 1
