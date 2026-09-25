from collections.abc import AsyncIterable, Callable
from uuid import UUID

from crucible.application.errors import ArtifactNotFound, TaskNotFound
from crucible.application.ports import UnitOfWork
from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.artifacts import Artifact


class ArtifactService:
    def __init__(
        self,
        store: LocalArtifactStore,
        unit_of_work: Callable[[], UnitOfWork],
    ) -> None:
        self._store = store
        self._unit_of_work = unit_of_work

    async def put(
        self,
        task_id: UUID,
        media_type: str,
        sensitivity: str,
        stream: AsyncIterable[bytes],
        *,
        hard_limit: int | None = None,
    ) -> Artifact:
        async with self._unit_of_work() as uow:
            if await uow.tasks.get(task_id) is None:
                raise TaskNotFound(f"Task not found: {task_id}")
        artifact = await self._store.put(
            task_id, media_type, sensitivity, stream, hard_limit=hard_limit
        )
        async with self._unit_of_work() as uow:
            existing = await uow.artifacts.get_by_content(
                task_id, artifact.content_hash, media_type, sensitivity
            )
            if existing is not None:
                return existing
            await uow.artifacts.add(artifact)
            await uow.commit()
        return artifact

    async def get(self, artifact_id: UUID) -> Artifact:
        async with self._unit_of_work() as uow:
            artifact = await uow.artifacts.get(artifact_id)
        if artifact is None:
            raise ArtifactNotFound(f"Artifact not found: {artifact_id}")
        return artifact

    async def read(self, artifact_id: UUID) -> tuple[Artifact, bytes]:
        artifact = await self.get(artifact_id)
        try:
            content = await self._store.read(artifact.storage_identity)
        except FileNotFoundError as error:
            raise ArtifactNotFound(
                f"Artifact bytes not found: {artifact_id}"
            ) from error
        return artifact, content
