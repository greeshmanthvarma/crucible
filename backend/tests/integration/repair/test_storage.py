from uuid import uuid4

from crucible.domain.repair import RepairCheckpoint
from tests.integration.repair.test_admission import environment
from tests.integration.storage.test_tool_loop_storage import NOW


async def test_checkpoint_immutable_and_dead_owner_recovery_is_explicit(database):
    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "key")
    checkpoint = RepairCheckpoint(
        uuid4(), result.repair_id, "intake", None, None, "inputs", "a" * 40, {}, {}, NOW
    )
    async with factory() as uow:
        await uow.repairs.add_checkpoint(checkpoint)
        await uow.commit()
    async with factory() as uow:
        assert checkpoint in await uow.repairs.list_checkpoints(result.repair_id)
    await service.reconcile(lambda owner: False)
    assert (await service.get(result.repair_id)).status == "queued"
    await service.reconcile(lambda owner: True)
    assert (await service.get(result.repair_id)).status == "interrupted"
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
    assert (await service.command(task.id, "/repair", "new")).code == "resume_or_close"


async def test_all_repair_records_and_claim_rollback(database):
    from dataclasses import replace

    import pytest
    from sqlalchemy.exc import IntegrityError

    from crucible.domain.repair import (
        RepairAcceptance,
        RepairAttempt,
        RepairFix,
        RepairPublication,
    )

    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "key")
    fix = RepairFix(uuid4(), result.repair_id, 0, "diagnosis", None, "pending")
    attempt = RepairAttempt(
        uuid4(), result.repair_id, fix.id, "diagnosis", 0, None, "running", True, NOW
    )
    acceptance = RepairAcceptance(
        uuid4(), result.repair_id, fix.id, "parent", "tree", "source", {}, "intent"
    )
    publication = RepairPublication(
        result.repair_id, "repair/test", "main", "sha", "pending"
    )
    async with factory() as uow:
        await uow.repairs.add_fix(fix)
        await uow.repairs.add_attempt(attempt)
        await uow.repairs.add_acceptance(acceptance)
        await uow.repairs.update_publication(publication)
        await uow.commit()
    async with factory() as uow:
        assert await uow.repairs.list_fixes(result.repair_id) == (fix,)
        assert await uow.repairs.list_attempts(result.repair_id) == (attempt,)
        assert await uow.repairs.get_acceptance(acceptance.id) == acceptance
        assert await uow.repairs.get_publication(result.repair_id) == publication
        assert not await uow.repairs.claim_global(result.repair_id, "another", NOW)
        assert not await uow.repairs.heartbeat(result.repair_id, "another", NOW)
        assert await uow.repairs.heartbeat(result.repair_id, service.execution_id, NOW)
        with pytest.raises(ValueError, match="immutable"):
            await uow.repairs.update_acceptance(
                replace(acceptance, candidate_tree_digest="other")
            )
        await uow.repairs.update_acceptance(
            replace(acceptance, status="committed", commit_sha="sha")
        )
        await uow.repairs.update_attempt(
            replace(attempt, status="completed", completed_at=NOW)
        )
        await uow.repairs.update_fix(
            replace(fix, status="accepted", accepted_commit="sha")
        )
        await uow.repairs.update_publication(
            replace(
                publication,
                status="published",
                pr_number=1,
                pr_url="https://example.test/pr/1",
            )
        )
        await uow.commit()
    checkpoint = RepairCheckpoint(
        uuid4(),
        result.repair_id,
        "intake",
        fix.id,
        None,
        "inputs",
        "parent",
        {},
        {},
        NOW,
    )
    async with factory() as uow:
        await uow.repairs.add_checkpoint(checkpoint)
        await uow.commit()
    with pytest.raises(IntegrityError):
        async with factory() as uow:
            await uow.repairs.add_checkpoint(checkpoint)
    async with factory() as uow:
        await uow.repairs.release_global(result.repair_id)
        # No commit: ownership rollback must preserve the previous owner.
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id == result.repair_id


async def test_graceful_shutdown_interrupts_without_scheduling(database):
    service, task, factory, _, _, scheduler = await environment(database)
    result = await service.command(task.id, "/repair", "key")
    await service.start()
    await service.close()
    assert (await service.get(result.repair_id)).status == "interrupted"
    assert scheduler.calls == 1
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None


async def test_dead_owner_keeps_ownership_until_orphan_workers_are_stopped(database):
    from crucible.repair.trials import TrialJob

    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "key")
    job = TrialJob(
        uuid4(),
        uuid4(),
        0,
        "case",
        "case-digest",
        "source",
        "fingerprint",
        "baseline",
        0,
        "running",
        NOW,
        repair_id=result.repair_id,
    )
    async with factory() as uow:
        await uow.repair_trials.add_job(job)
        await uow.commit()
    await service.reconcile(lambda owner: True)
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id == result.repair_id
        assert (await uow.repair_trials.get_job(job.id)).status == "running"

    class Recovery:
        async def stop_jobs(self, job_ids):
            assert job_ids == (job.id,)
            async with factory() as uow:
                assert (await uow.repairs.owner()).repair_id == result.repair_id

    service._worker_recovery = Recovery()
    await service.reconcile(lambda owner: True)
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
        assert (await uow.repair_trials.get_job(job.id)).status == "interrupted"
    assert (await service.get(result.repair_id)).status == "interrupted"


async def test_orphan_cleanup_uses_only_exact_job_labels(tmp_path):
    from crucible.repair.admission import LocalRepairWorkerRecovery

    job_id = uuid4()

    class Process:
        calls = []
        containers = "owned-container"
        networks = "owned-network"

        async def run(self, root, *argv):
            self.calls.append(argv)
            if argv[1] == "ps":
                return self.containers
            if argv[1] == "rm":
                self.containers = ""
                return ""
            if argv[2] == "ls":
                return self.networks
            if argv[2] == "rm":
                self.networks = ""
                return ""
            raise AssertionError(argv)

    process = Process()
    await LocalRepairWorkerRecovery(tmp_path, process).stop_jobs((job_id,))
    reads = [call for call in process.calls if "--filter" in call]
    assert all(call[-1] == f"label=crucible.repair.job={job_id}" for call in reads)
    assert ("docker", "rm", "--force", "owned-container") in process.calls
    assert ("docker", "network", "rm", "owned-network") in process.calls
