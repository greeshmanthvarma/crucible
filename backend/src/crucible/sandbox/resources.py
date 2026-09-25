from collections.abc import Callable
from dataclasses import dataclass, replace
from uuid import UUID

from crucible.application.errors import TaskNotFound
from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.ids import new_id
from crucible.domain.resources import (
    ExternalResource,
    ExternalResourceKind,
    ExternalResourceStatus,
)
from crucible.sandbox.docker_client import DockerClient, VolumeInfo
from crucible.sandbox.protocol import SandboxMount

MANAGED_LABEL = "harness.managed"
TASK_LABEL = "harness.task_id"
KIND_LABEL = "harness.resource_kind"
RESOURCE_LABEL = "harness.resource_id"
DEPENDENCY_TARGET = "/workspace/node_modules"


class ResourceOwnershipMismatch(RuntimeError):
    pass


class ManagedResourceMissing(RuntimeError):
    pass


@dataclass(frozen=True)
class ResourceReconciliation:
    present: tuple[ExternalResource, ...]
    missing: tuple[ExternalResource, ...]
    mismatched: tuple[ExternalResource, ...]
    orphaned: tuple[VolumeInfo, ...]


class TaskResourceManager:
    def __init__(
        self,
        docker: DockerClient,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
    ) -> None:
        self._docker = docker
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def ensure_dependency_volume(self, task_id: UUID) -> ExternalResource:
        async with self._unit_of_work() as uow:
            if await uow.tasks.get(task_id) is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            existing = self._dependency_record(
                await uow.external_resources.list_for_task(task_id)
            )
        if existing is not None:
            await self._verify(existing)
            return existing

        resource_id = new_id()
        labels = {
            MANAGED_LABEL: "true",
            TASK_LABEL: str(task_id),
            KIND_LABEL: ExternalResourceKind.VOLUME.value,
            RESOURCE_LABEL: str(resource_id),
        }
        requested_name = f"crucible-task-{task_id}-dependencies"
        created = await self._docker.create_volume(requested_name, labels)
        resource = ExternalResource.volume(
            resource_id,
            task_id,
            created.identity,
            DEPENDENCY_TARGET,
            self._clock.now(),
            labels=labels,
        )
        self._require_labels(resource, created)
        async with self._unit_of_work() as uow:
            await uow.external_resources.add(resource)
            await uow.commit()
        return resource

    async def verified_mounts(self, task_id: UUID) -> tuple[SandboxMount, ...]:
        async with self._unit_of_work() as uow:
            resources = await uow.external_resources.list_for_task(task_id)
        record = self._dependency_record(resources)
        if record is None:
            raise ManagedResourceMissing(
                f"Task dependency volume is not recorded: {task_id}"
            )
        await self._verify(record)
        return (
            SandboxMount(
                record.id,
                record.external_identity,
                record.mount_target or DEPENDENCY_TARGET,
            ),
        )

    async def reconcile(self) -> ResourceReconciliation:
        async with self._unit_of_work() as uow:
            records = tuple(
                resource
                for resource in await uow.external_resources.list_managed()
                if resource.kind is ExternalResourceKind.VOLUME
            )
        discovered = {
            volume.identity: volume
            for volume in await self._docker.list_volumes(f"{MANAGED_LABEL}=true")
        }
        present: list[ExternalResource] = []
        missing: list[ExternalResource] = []
        mismatched: list[ExternalResource] = []
        for record in records:
            volume = discovered.pop(record.external_identity, None)
            if volume is None:
                missing.append(
                    await self._record_status(record, ExternalResourceStatus.MISSING)
                )
                continue
            try:
                self._require_labels(record, volume)
            except ResourceOwnershipMismatch:
                mismatched.append(
                    await self._record_status(record, ExternalResourceStatus.ERROR)
                )
            else:
                present.append(
                    await self._record_status(record, ExternalResourceStatus.PRESENT)
                )
        return ResourceReconciliation(
            tuple(present),
            tuple(missing),
            tuple(mismatched),
            tuple(discovered.values()),
        )

    async def _verify(self, resource: ExternalResource) -> VolumeInfo:
        volume = await self._docker.inspect_volume(resource.external_identity)
        if volume is None:
            await self._record_status(resource, ExternalResourceStatus.MISSING)
            raise ManagedResourceMissing(
                f"Managed volume is missing: {resource.external_identity}"
            )
        self._require_labels(resource, volume)
        return volume

    @staticmethod
    def _require_labels(resource: ExternalResource, volume: VolumeInfo) -> None:
        expected = {
            MANAGED_LABEL: "true",
            TASK_LABEL: str(resource.task_id),
            KIND_LABEL: resource.kind.value,
            RESOURCE_LABEL: str(resource.id),
        }
        if volume.identity != resource.external_identity or any(
            volume.labels.get(key) != value for key, value in expected.items()
        ):
            raise ResourceOwnershipMismatch(
                f"Docker volume ownership does not match record {resource.id}"
            )

    async def _record_status(
        self, resource: ExternalResource, status: ExternalResourceStatus
    ) -> ExternalResource:
        updated = replace(resource, status=status, updated_at=self._clock.now())
        if updated == resource:
            return resource
        async with self._unit_of_work() as uow:
            await uow.external_resources.update(updated)
            await uow.commit()
        return updated

    @staticmethod
    def _dependency_record(
        resources: tuple[ExternalResource, ...],
    ) -> ExternalResource | None:
        matches = tuple(
            resource
            for resource in resources
            if resource.kind is ExternalResourceKind.VOLUME
            and resource.mount_target == DEPENDENCY_TARGET
            and resource.status is not ExternalResourceStatus.REMOVED
        )
        if len(matches) > 1:
            raise ResourceOwnershipMismatch("Task has multiple dependency volumes")
        return matches[0] if matches else None
