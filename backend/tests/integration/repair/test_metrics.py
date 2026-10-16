from uuid import uuid4

import pytest

from crucible.domain.repair import RepairAttempt
from crucible.repair.metrics import RepairMetrics
from tests.integration.repair.test_admission import environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


async def test_metrics_count_all_attempts_without_inventing_missing_measurements(
    database,
):
    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "metrics")
    async with factory() as uow:
        for index, status in enumerate(
            ("infrastructure_error", "invalid_output", "completed"), 1
        ):
            await uow.repairs.add_attempt(
                RepairAttempt(
                    uuid4(),
                    result.repair_id,
                    None,
                    "diagnosis",
                    index,
                    None,
                    status,
                    True,
                    CLOCK.now(),
                    CLOCK.now(),
                )
            )
        await uow.commit()
    report = await RepairMetrics(factory, CLOCK).build(result.repair_id)
    assert report["attempts"]["total"] == 3
    assert report["attempts"]["by_status"] == {
        "infrastructure_error": 1,
        "invalid_output": 1,
        "completed": 1,
    }
    assert report["evidence_kind"] == "unknown"
    assert report["usage"]["reported"]["input_tokens"] == 0
    assert report["usage"]["estimated"]["input_tokens"] == 0
    assert report["cache_hits"] is None
    assert report["wall_seconds"] == 0
    assert report["trial_active_seconds"] is None
    assert report["cases"] == []


async def test_metrics_include_executed_cases_and_batch_reuse_without_double_billing(
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
        *_,
    ) = await cycle_environment(database, tmp_path)
    await controller.execute(repair.id)
    first = await RepairMetrics(factory, CLOCK).build(repair.id)
    assert first["trial_jobs"] == 14
    assert sum(c["observations"] for c in first["cases"]) == 14
    assert first["trial_active_seconds"] == pytest.approx(0.14)
    assert first["cache_hits"] == 0
    assert first["evidence_kind"] == "simulated"
    assert first["natural_failure_verified"] is False
    assert first["usage"]["estimated"]["input_tokens"] > 0
    await service.command(repair.source_task_id, "/repair resume", "resume-metrics")
    await controller.execute(repair.id)
    later = await RepairMetrics(factory, CLOCK).build(repair.id)
    assert later["trial_jobs"] == 21
    assert later["cache_hits"] == 7
    assert sum(c["observations"] for c in later["cases"]) == 21


async def test_private_cli_reads_existing_report_without_starting_work(database):
    import asyncio
    import json
    import sys

    service, task, factory, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "cli-report")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "scripts/report_repairs.py",
        "--database",
        database.path,
        str(result.repair_id),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, error = await process.communicate()
    assert process.returncode == 0, error.decode()
    report = json.loads(output)[0]
    assert report["repair_id"] == str(result.repair_id)
    assert report["attempts"]["total"] == 0
    assert (await service.get(result.repair_id)).status == "queued"


async def test_direct_trial_cache_records_reuse_without_an_extra_job(
    database, tmp_path
):
    from crucible.domain.repair import RepairRun, RepairStage, RepairStatus
    from tests.integration.repair.test_trials import (
        Worker,
        approved_case,
        configuration,
    )
    from tests.integration.repair.test_trials import environment as trial_environment
    from tests.integration.repair.test_trials import (
        service as trial_service,
    )

    env = await trial_environment(database, tmp_path)
    repair = RepairRun(
        uuid4(),
        env["task"].id,
        RepairStatus.RUNNING,
        RepairStage.BASELINE_REPRODUCTION,
        {},
        None,
        None,
        "baseline",
        None,
        CLOCK.now(),
        CLOCK.now(),
    )
    async with env["factory"]() as uow:
        await uow.repairs.add(repair)
        await uow.commit()
    worker = Worker(
        tmp_path / "workers", content=None, status="error", code="write_failed"
    )
    trials = trial_service(env, worker)
    trials.repair_id = repair.id
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    first = await trials.run(case, env["source"], "baseline", 0, fingerprint)
    reused = await trials.run(case, env["source"], "baseline", 0, fingerprint)
    assert reused.id == first.id
    report = await RepairMetrics(env["factory"], CLOCK).build(repair.id)
    assert report["trial_jobs"] == 1
    assert report["cache_hits"] == 1
    assert report["completed_observations"] == 1
    assert report["usage"]["reported"]["input_tokens"] == 11
