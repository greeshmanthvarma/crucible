"""Infrastructure failures cannot lower behavioral regression thresholds."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from crucible.domain.repository import RepositorySettings
from crucible.evals.evaluators import EvaluatorRegistry
from crucible.evals.manifests import (
    EvalCaseDefinition,
    EvalPartition,
    EvalSuiteDefinition,
    EvaluatorDefinition,
)
from crucible.repair.baselines import BaselineCache
from tests.integration.repair.test_trials import Worker, environment, service


@pytest.mark.parametrize("side", ["baseline", "candidate"])
@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("model_gateway_error", "error"),
        (None, "error"),
        ("validation_failed", "fail"),
    ],
)
async def test_repository_failure_provenance_controls_verdict_and_cache(
    database, tmp_path, side, outcome, expected
):
    env = await environment(database, tmp_path)
    worker = Worker(tmp_path / "workers", status="error", code="repository_run_failed")
    original = worker.run

    async def failed_run(*args):
        execution = await original(*args)
        provenance = {"run_status": "failed"}
        if outcome is not None:
            provenance["run_outcome_code"] = outcome
        return replace(
            execution,
            observation=execution.observation.model_copy(
                update={"tool_result": provenance}
            ),
        )

    worker.run = failed_run
    case = EvalCaseDefinition(
        "synthetic-repository-failure",
        env["fixture"],
        "a" * 40,
        "Synthetic failure provenance fixture",
        RepositorySettings(),
        "test-model",
        (EvaluatorDefinition("required_file", {"path": "note.txt"}),),
        "b" * 64,
        EvalPartition.DEVELOPMENT,
        "synthetic-fixture",
    )
    suite = EvalSuiteDefinition(
        "synthetic-provenance", (case,), "c" * 64, EvalPartition.DEVELOPMENT
    )

    class Fixtures:
        def prepare(self, case, job_id):
            return SimpleNamespace(root=env["fixture"])

    async def credential(job):
        return None  # Trusted deterministic supervisor seam; never a provider call.

    trials = service(env, worker)
    trials.credential_factory = credential
    fingerprint = "d" * 64
    rows = (
        await trials.run_repository_suite(
            suite,
            env["source"],
            side,
            {case.case_id: fingerprint},
            fixtures=Fixtures(),
            evaluators=EvaluatorRegistry(data_dir=tmp_path),
            task_id=env["task"].id,
            run_id=env["run"].id,
        )
    )[case.case_id]
    assert {row.verdict for row in rows} == {expected}
    assert len(rows) == (1 if expected == "error" else 3)
    cached = await BaselineCache(env["factory"]).get(fingerprint, case.case_id, 0)
    if side == "baseline" and expected == "fail":
        assert cached is not None and cached.id == rows[0].id
    else:
        assert cached is None
    async with env["factory"]() as uow:
        saved = await uow.repair_trials.list_results(fingerprint, case.case_id, side)
    assert {row.id for row in saved} == {row.id for row in rows}


async def test_legacy_provider_failure_without_assertions_is_not_reused(
    database, tmp_path
):
    from uuid import uuid4

    from crucible.repair.trials import TrialEvidence, TrialJob
    from tests.integration.repair.test_trials import (
        NOW,
        approved_case,
        configuration,
    )

    env = await environment(database, tmp_path)
    case = approved_case(env)
    fingerprint = configuration(case, env["source"])
    identifier = uuid4()
    legacy = TrialEvidence(
        identifier,
        fingerprint,
        case.case.case_id,
        0,
        "baseline",
        env["source"].tree_digest,
        None,
        "fail",
        (),
        {"requests": 1, "input_tokens": 0, "output_tokens": 0, "basis": "reported"},
        NOW,
    )
    job = TrialJob(
        identifier,
        uuid4(),
        0,
        case.case.case_id,
        case.case_digest,
        env["source"].tree_digest,
        fingerprint,
        "baseline",
        0,
        "running",
        NOW,
    )
    async with env["factory"]() as uow:
        await uow.repair_trials.add_job(job)
        await uow.repair_trials.add_result(legacy)
        await uow.repair_trials.update_job(
            replace(job, status="completed", completed_at=NOW)
        )
        await uow.commit()
    assert (
        await BaselineCache(env["factory"]).get(fingerprint, case.case.case_id, 0)
        is None
    )
    worker = Worker(
        tmp_path / "workers", status="error", code="write_failed", content=None
    )
    result = await service(env, worker).run(
        case, env["source"], "baseline", 0, fingerprint
    )
    assert result.id != legacy.id
    assert result.verdict == "pass"
    assert len(worker.jobs) == 1
