"""Cache equivalence and recovery generation tests with a temporary real DB."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from crucible.repair.baselines import BaselineCache
from sqlalchemy.exc import IntegrityError

from tests.integration.repair.test_trials import (
    Worker,
    approved_case,
    configuration,
    environment,
    service,
)


async def test_completed_baseline_is_reused_after_service_restart(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    worker = Worker(
        tmp_path / "workers", content=None, status="error", code="write_failed"
    )
    first = await service(env, worker).run(
        case, env["source"], "baseline", 0, fingerprint
    )
    assert first.verdict == "pass"
    assert len(worker.jobs) == 1
    # A new invocation and cache/service object must discover durable evidence.
    later_worker = Worker(tmp_path / "later-workers")
    later = await service(env, later_worker).run(
        case, env["source"], "baseline", 0, fingerprint
    )
    assert later.id == first.id
    assert not later_worker.jobs
    assert (
        await BaselineCache(env["factory"]).get(fingerprint, case.case.case_id, 0)
        == first
    )


async def test_cache_misses_changed_config_case_source_and_repeat(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    worker = Worker(
        tmp_path / "workers", content=None, status="error", code="write_failed"
    )
    await service(env, worker).run(case, env["source"], "baseline", 0, fingerprint)
    cache = BaselineCache(env["factory"])
    assert (
        await cache.get(
            configuration(case, env["source"], revision="changed-model"),
            case.case.case_id,
            0,
        )
        is None
    )
    assert await cache.get(fingerprint, "different-case", 0) is None
    assert await cache.get(fingerprint, case.case.case_id, 1) is None
    (env["source_root"] / "new.py").write_text("# new controlled source")
    from crucible.repair.fingerprints import capture_source_identity

    changed = await capture_source_identity(env["source_root"])
    assert await cache.get(configuration(case, changed), case.case.case_id, 0) is None


async def test_candidate_results_are_never_exposed_as_baseline_cache(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    result = await service(env, Worker(tmp_path / "workers")).run(
        case, env["source"], "candidate", 0, fingerprint
    )
    assert result.verdict == "pass"
    assert (
        await BaselineCache(env["factory"]).get(fingerprint, case.case.case_id, 0)
        is None
    )


async def test_candidate_trial_not_reused_after_interruption(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    invocation = uuid4()
    fingerprint = configuration(case, env["source"])
    worker = Worker(tmp_path / "workers")
    initial = service(env, worker, invocation_id=invocation, candidate_generation=0)
    completed = await initial.run(case, env["source"], "candidate", 0, fingerprint)
    # A second trial is interrupted after a completed result already exists.
    fresh = approved_case(env, case_id="synthetic-fresh", mode="fresh_call")
    worker.block = True
    worker.entered.clear()
    unfinished = asyncio.create_task(
        initial.run(
            fresh, env["source"], "candidate", 0, configuration(fresh, env["source"])
        )
    )
    try:
        await asyncio.wait_for(worker.entered.wait(), 2)
        unfinished.cancel()
        with pytest.raises(asyncio.CancelledError):
            await unfinished
    finally:
        if not unfinished.done():
            unfinished.cancel()
            await asyncio.gather(unfinished, return_exceptions=True)
    resumed_worker = Worker(tmp_path / "resumed-workers")
    resumed = service(
        env, resumed_worker, invocation_id=invocation, candidate_generation=1
    )
    repeated = await resumed.run(case, env["source"], "candidate", 0, fingerprint)
    assert repeated.id != completed.id
    assert len(resumed_worker.jobs) == 1
    async with env["factory"]() as uow:
        historical = await uow.repair_trials.get_job(completed.id)
        results = await uow.repair_trials.list_results(
            fingerprint, case.case.case_id, "candidate"
        )
        jobs = await uow.repair_trials.list_jobs(invocation)
    assert historical.candidate_generation == 0
    assert {result.id for result in results} == {completed.id, repeated.id}
    assert any(job.status == "interrupted" for job in jobs)


async def test_unchanged_candidate_is_reused_only_within_same_invocation_generation(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    invocation = uuid4()
    worker = Worker(tmp_path / "workers")
    trials = service(env, worker, invocation_id=invocation)
    first = await trials.run(case, env["source"], "candidate", 0, fingerprint)
    same = await trials.run(case, env["source"], "candidate", 0, fingerprint)
    assert same.id == first.id
    assert len(worker.jobs) == 1
    other_worker = Worker(tmp_path / "other-workers")
    other = await service(env, other_worker, invocation_id=uuid4()).run(
        case, env["source"], "candidate", 0, fingerprint
    )
    assert other.id != first.id
    assert len(other_worker.jobs) == 1


@pytest.mark.parametrize("verdict", ["error", "unsupported"])
async def test_nonbehavioral_baselines_are_not_cache_hits(database, tmp_path, verdict):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    worker = Worker(
        tmp_path / "workers",
        status="unsupported" if verdict == "unsupported" else "completed",
        termination="backend_error" if verdict == "error" else "completed",
    )
    result = await service(env, worker).run(
        case, env["source"], "baseline", 0, fingerprint
    )
    assert result.verdict == verdict
    assert (
        await BaselineCache(env["factory"]).get(fingerprint, case.case.case_id, 0)
        is None
    )


async def test_result_is_immutable_and_job_scope_has_database_uniqueness(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    result = await service(env, Worker(tmp_path / "workers")).run(
        case, env["source"], "candidate", 0, configuration(case, env["source"])
    )
    with pytest.raises((ValueError, IntegrityError)):
        async with env["factory"]() as uow:
            await uow.repair_trials.add_result(replace(result, verdict="fail"))
            await uow.commit()
    async with env["factory"]() as uow:
        job = await uow.repair_trials.get_job(result.id)
    with pytest.raises((ValueError, IntegrityError)):
        async with env["factory"]() as uow:
            await uow.repair_trials.add_job(replace(job, id=uuid4()))
            await uow.commit()
    async with env["factory"]() as uow:
        saved = await uow.repair_trials.list_results(
            result.fingerprint, case.case.case_id, "candidate"
        )
    assert saved == (result,)
