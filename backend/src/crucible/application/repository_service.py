from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository
from crucible.workspaces.git import GitClient


@dataclass(frozen=True)
class RegisteredRepository:
    repository: Repository
    head_revision: str
    created: bool


class RepositoryService:
    def __init__(
        self,
        git: GitClient,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
    ) -> None:
        self._git = git
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def register(self, candidate: Path) -> RegisteredRepository:
        resolved = await self._git.resolve_repository(candidate)
        async with self._unit_of_work() as uow:
            repository = Repository(new_id(), resolved.root, self._clock.now())
            created = await uow.repositories.add_if_absent(repository)
            if not created:
                existing = await uow.repositories.get_by_root(resolved.root)
                if existing is None:
                    raise RuntimeError("Repository insert conflict was not readable")
                repository = existing
            await uow.commit()
        return RegisteredRepository(repository, resolved.head_revision, created)

    async def list(self) -> tuple[RegisteredRepository, ...]:
        async with self._unit_of_work() as uow:
            repositories = await uow.repositories.list()
        results = []
        for repository in repositories:
            resolved = await self._git.resolve_repository(repository.root_path)
            results.append(
                RegisteredRepository(
                    repository=repository,
                    head_revision=resolved.head_revision,
                    created=False,
                )
            )
        return tuple(results)
