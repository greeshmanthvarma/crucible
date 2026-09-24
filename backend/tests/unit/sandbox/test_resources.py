from datetime import UTC, datetime

import pytest

from crucible.domain.ids import new_id
from crucible.sandbox.docker_client import VolumeInfo
from crucible.sandbox.resources import (
    ResourceOwnershipMismatch,
    TaskResourceManager,
)
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


class FakeDockerClient:
    def __init__(self) -> None:
        self.volumes: dict[str, VolumeInfo] = {}

    async def create_volume(self, name: str, labels: dict[str, str]) -> VolumeInfo:
        value = VolumeInfo(name, labels)
        self.volumes[name] = value
        return value

    async def inspect_volume(self, identity: str) -> VolumeInfo | None:
        return self.volumes.get(identity)

    async def list_volumes(self, label: str) -> tuple[VolumeInfo, ...]:
        key, expected = label.split("=", 1)
        return tuple(
            item for item in self.volumes.values() if item.labels.get(key) == expected
        )


async def test_resume_mounts_only_exact_stored_volume_after_label_verification(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, *_ = await seed_exchange(uow, "resource-labels")
        await uow.commit()
    docker = FakeDockerClient()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    manager = TaskResourceManager(docker, factory, CLOCK)
    record = await manager.ensure_dependency_volume(task.id)
    docker.volumes[record.external_identity] = VolumeInfo(
        record.external_identity,
        {**record.labels, "harness.task_id": str(new_id())},
    )

    with pytest.raises(ResourceOwnershipMismatch):
        await manager.verified_mounts(task.id)


async def test_dependency_volume_is_idempotent_and_returns_verified_mount(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, *_ = await seed_exchange(uow, "resource-idempotent")
        await uow.commit()
    docker = FakeDockerClient()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    manager = TaskResourceManager(docker, factory, CLOCK)

    first = await manager.ensure_dependency_volume(task.id)
    second = await manager.ensure_dependency_volume(task.id)
    mounts = await manager.verified_mounts(task.id)

    assert second == first
    assert mounts[0].external_identity == first.external_identity
    assert mounts[0].target == "/workspace/node_modules"
    assert len(docker.volumes) == 1


async def test_reconciliation_reports_orphans_without_claiming_them(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, *_ = await seed_exchange(uow, "resource-reconcile")
        await uow.commit()
    docker = FakeDockerClient()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    manager = TaskResourceManager(docker, factory, CLOCK)
    record = await manager.ensure_dependency_volume(task.id)
    orphan = VolumeInfo("unrecorded", {"harness.managed": "true"})
    docker.volumes[orphan.identity] = orphan

    report = await manager.reconcile()

    assert report.present[0].id == record.id
    assert report.orphaned == (orphan,)
    assert orphan.identity in docker.volumes
