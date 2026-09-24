from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import UUID

from crucible.application.errors import RepositoryNotFound
from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.ids import new_id
from crucible.domain.repository import Repository, RepositorySettings
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

    async def update_settings(
        self, repository_id: UUID, settings: RepositorySettings
    ) -> RegisteredRepository:
        async with self._unit_of_work() as uow:
            repository = await uow.repositories.get(repository_id)
            if repository is None:
                raise RepositoryNotFound(f"Repository {repository_id} was not found")
            repository = replace(repository, settings=settings)
            await uow.repositories.update(repository)
            await uow.commit()
        resolved = await self._git.resolve_repository(repository.root_path)
        return RegisteredRepository(repository, resolved.head_revision, created=False)

    async def get(self, repository_id: UUID) -> Repository:
        async with self._unit_of_work() as uow:
            repository = await uow.repositories.get(repository_id)
        if repository is None:
            raise RepositoryNotFound(f"Repository {repository_id} was not found")
        return repository
