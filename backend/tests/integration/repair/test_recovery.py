import pytest

from crucible.repair.controller import RepairController
from tests.integration.repair.test_roles import role_environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


async def test_malformed_diagnosis_is_bounded_across_explicit_resume(
    database, tmp_path
):
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(database, tmp_path, ["not-json"] * 3)
    repair = (await service.list_for_task(task.id))[0]
    controller = RepairController(factory, CLOCK, roles, incident, artifacts)
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "exhausted"
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert len(attempts) == 3 and all(a.charged for a in attempts)
    assert len(provider.requests) == 3


async def test_resume_restarts_unaccepted_candidate_but_keeps_baselines(
    database, tmp_path
):
    import asyncio

    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path)
    service.bind_scheduler(controller)
    worker.pause_candidate = True
    await controller.submit(repair.id)
    await asyncio.wait_for(worker.candidate_entered.wait(), 15)
    stopped = await service.stop(repair.id, "stop")
    assert stopped.status == "stopped" and worker.cancelled
    assert (root / editable).read_text() == "# original\n"
    baseline_jobs = [job.job_id for job in worker.jobs if job.purpose == "baseline"]
    worker.pause_candidate = False
    result = await service.command(repair.source_task_id, "/repair resume", "resume")
    assert result.status == "resumed"
    # Wait on the application task, not polling a model or external service.
    execution = controller.active.get(repair.id)
    if execution:
        await execution
    current = await service.get(repair.id)
    assert current.stage == "commit_acceptance" and current.status == "retryable_error"
    assert [
        job.job_id for job in worker.jobs if job.purpose == "baseline"
    ] == baseline_jobs
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert [a.attempt_index for a in attempts if a.role == "proposal"] == [1, 2]


async def test_provider_failure_is_retryable_without_consuming_a_new_behavioral_attempt(
    database, tmp_path
):
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(
        database,
        tmp_path,
        [
            RuntimeError("PRIVATE_PROVIDER_TRACE"),
            {"decision": "no_change_needed", "reasons": ["No defect"], "fix_items": []},
        ],
    )
    repair = (await service.list_for_task(task.id))[0]
    controller = RepairController(factory, CLOCK, roles, incident, artifacts)
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "retryable_error"
    await service.command(task.id, "/repair resume", "resume")
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "completed"
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert [a.attempt_index for a in attempts] == [1, 1]
        assert all(a.charged for a in attempts)
    assert "PRIVATE_PROVIDER_TRACE" not in repr(provider.requests)


async def test_reviewer_outage_reuses_completed_diagnosis_and_same_chain(
    database, tmp_path
):
    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    tail = provider.outputs[2:]
    provider.outputs = [diagnosis, RuntimeError("provider unavailable"), review, *tail]
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "retryable_error"
    assert worker.jobs == []
    await service.command(repair.source_task_id, "/repair resume", "resume")
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).stage == "commit_acceptance"
    async with factory() as uow:
        attempts = await uow.repairs.list_attempts(repair.id)
        assert [(a.role, a.attempt_index) for a in attempts] == [
            ("diagnosis", 1),
            ("review", 1),
            ("review", 1),
            ("proposal", 1),
        ]


@pytest.mark.parametrize("same_path", [False, True])
async def test_unexpected_edit_is_preserved_and_resume_stops_for_review(
    database, tmp_path, same_path
):
    import asyncio

    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path)
    service.bind_scheduler(controller)
    worker.pause_candidate = True
    await controller.submit(repair.id)
    await asyncio.wait_for(worker.candidate_entered.wait(), 15)
    sentinel = root / editable if same_path else root / "user.txt"
    sentinel.write_text("USER_EDIT")
    await service.stop(repair.id, "stop")
    assert sentinel.read_text() == "USER_EDIT"
    if not same_path:
        assert (root / editable).read_text() == "# candidate one\n"
    worker.pause_candidate = False
    await service.command(repair.source_task_id, "/repair resume", "resume")
    execution = controller.active.get(repair.id)
    if execution:
        await execution
    current = await service.get(repair.id)
    assert current.status == "stopped" and current.outcome_code == "needs_review"
    assert sentinel.read_text() == "USER_EDIT"


async def test_main_source_drift_invalidates_before_model_work(database, tmp_path):
    from crucible.repair.identity import capture_runtime_identity
    from crucible.repair.recovery import ManagedRepairWorkspace
    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path)
    runtime = await capture_runtime_identity(root)
    controller.recovery = ManagedRepairWorkspace(
        factory, controller.artifacts, main_root=root, runtime_identity=runtime
    )
    (root / "visible.txt").write_text("user changed main")
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "invalidated"
    assert provider.requests == []
    assert (root / "visible.txt").read_text() == "user changed main"


async def test_completed_diagnosis_is_reused_after_interruption_before_transition(
    database, tmp_path
):
    import asyncio

    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(
        database,
        tmp_path,
        [{"decision": "no_change_needed", "reasons": ["No defect"], "fix_items": []}],
    )
    repair = (await service.list_for_task(task.id))[0]
    entered = asyncio.Event()

    class Notifier:
        blocked = False

        async def notify(self, task_id):
            async with factory() as uow:
                attempts = await uow.repairs.list_attempts(repair.id)
            if not self.blocked and any(a.status == "completed" for a in attempts):
                self.blocked = True
                entered.set()
                await asyncio.Future()

    controller = RepairController(
        factory, CLOCK, roles, incident, artifacts, notifier=Notifier()
    )
    service.bind_scheduler(controller)
    await controller.submit(repair.id)
    await asyncio.wait_for(entered.wait(), 10)
    await service.stop(repair.id, "stop")
    assert len(provider.requests) == 1
    await service.command(task.id, "/repair resume", "resume")
    execution = controller.active.get(repair.id)
    if execution:
        await execution
    assert (await service.get(repair.id)).status == "completed"
    assert len(provider.requests) == 1


async def test_interruption_does_not_reset_the_third_proposal_allowance(
    database, tmp_path
):
    import asyncio

    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        *_,
    ) = await cycle_environment(database, tmp_path, heldout_failure=True)
    entered = asyncio.Event()

    async def pause(request):
        entered.set()
        await asyncio.Future()

    provider.outputs[-1] = pause
    service.bind_scheduler(controller)
    await controller.submit(repair.id)
    await asyncio.wait_for(entered.wait(), 40)
    await service.stop(repair.id, "stop")
    assert (root / editable).read_text() == "# original\n"
    request_count = len(provider.requests)
    await service.command(repair.source_task_id, "/repair resume", "resume")
    execution = controller.active.get(repair.id)
    if execution:
        await execution
    assert (await service.get(repair.id)).status == "exhausted"
    assert len(provider.requests) == request_count
    async with factory() as uow:
        assert [
            a.attempt_index
            for a in await uow.repairs.list_attempts(repair.id)
            if a.role == "proposal"
        ] == [1, 2, 3]


async def test_changed_role_configuration_invalidates_saved_stages(database, tmp_path):
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(
        database, tmp_path, [RuntimeError("provider unavailable")]
    )
    repair = (await service.list_for_task(task.id))[0]
    controller = RepairController(factory, CLOCK, roles, incident, artifacts)
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "retryable_error"
    roles.configuration = {"model": "different-model"}
    await service.command(task.id, "/repair resume", "resume")
    await controller.execute(repair.id)
    assert (await service.get(repair.id)).status == "invalidated"
    assert len(provider.requests) == 1
