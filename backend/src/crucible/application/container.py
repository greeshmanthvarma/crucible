from dataclasses import dataclass
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from crucible.application.message_service import MessageService
from crucible.application.ports import UnitOfWork
from crucible.application.repository_service import RepositoryService
from crucible.application.task_service import TaskService
from crucible.domain.clock import SystemClock
from crucible.engine.fake_gateway import FakeModelGateway
from crucible.engine.run_engine import RunEngine
from crucible.engine.supervisor import LocalRunSupervisor
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.workspaces.git import SubprocessGitClient
from crucible.workspaces.manager import WorkspaceManager


@dataclass(frozen=True)
class ApplicationContainer:
    database: Database
    repository_service: RepositoryService
    task_service: TaskService
    message_service: MessageService
    supervisor: LocalRunSupervisor

    @classmethod
    async def create(cls, database_url: str, data_dir: Path) -> "ApplicationContainer":
        database = await Database.create(database_url)
        await _verify_current_revision(database)

        def unit_of_work() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(database)

        clock = SystemClock()
        git = SubprocessGitClient()
        engine = RunEngine(unit_of_work, clock, FakeModelGateway())
        supervisor = LocalRunSupervisor(engine, unit_of_work, clock)
        return cls(
            database=database,
            repository_service=RepositoryService(git, unit_of_work, clock),
            task_service=TaskService(
                WorkspaceManager(git, data_dir), unit_of_work, clock
            ),
            message_service=MessageService(unit_of_work, clock, supervisor),
            supervisor=supervisor,
        )

    async def start(self) -> None:
        await self.supervisor.reconcile()

    async def close(self) -> None:
        await self.supervisor.close()
        await self.database.dispose()


async def _verify_current_revision(database: Database) -> None:
    config_path = Path(__file__).parents[3] / "alembic.ini"
    expected = ScriptDirectory.from_config(Config(config_path)).get_current_head()
    async with database.engine.connect() as connection:
        actual = await connection.scalar(
            text("SELECT version_num FROM alembic_version")
        )
    if actual != expected:
        await database.dispose()
        raise RuntimeError(
            f"Database schema is not current: expected {expected}, found {actual}"
        )
