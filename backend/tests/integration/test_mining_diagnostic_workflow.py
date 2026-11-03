"""End-to-end scripted diagnostics: real exports, ledger, validation and reporting."""

import json
from dataclasses import replace

import pytest

from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner
from crucible.improvement.mining_reporting import write_mining_report
from crucible.repair.gateway import TrustedUsage
from tests.integration import test_mining_diagnostic as fixtures

harness = fixtures.harness
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial
diagnostic_config = fixtures.diagnostic_config
prepared_diagnostic = fixtures.prepared_diagnostic


class ReportingSupervisor(fixtures.ScriptedSupervisor):
    async def execute(self, *args, **kwargs):
        result = await super().execute(*args, **kwargs)
        return replace(
            result, outcome=replace(result.outcome, model_usage=TrustedUsage(1, 60, 40))
        )


@pytest.mark.parametrize("blocked", [False, True])
async def test_report_includes_every_attempt_and_distinguishes_mining_from_improvement(
    prepared_diagnostic, tmp_path, blocked, monkeypatch
):
    from crucible.benchmarks.swebench import SwebenchGrader
    from crucible.improvement.roles import RoleRunner
    from crucible.improvement.trials import IsolatedTrialRunner
    from crucible.improvement.worker_supervisor import DockerRuntime
    from crucible.models.responses_gateway import LiteLLMResponsesGateway

    def forbidden(*args, **kwargs):
        raise AssertionError(
            "diagnostic reached proposal, grading, trial, or live execution"
        )

    monkeypatch.setattr(RoleRunner, "propose", forbidden)
    for port in (
        IsolatedTrialRunner,
        SwebenchGrader,
        LiteLLMResponsesGateway,
        DockerRuntime,
    ):
        monkeypatch.setattr(port, "__init__", forbidden)
    prepared, store = prepared_diagnostic
    worker = ReportingSupervisor(
        store,
        prepared.root,
        failure=1 if blocked else None,
        faults={0: "invalid", 1: "exhaust_invalid"} if blocked else {},
    )
    state = await MiningDiagnosticRunner(store, prepared.root, worker).continue_run(
        prepared.diagnostic_id
    )
    await write_mining_report(store, prepared.diagnostic_id, tmp_path / "report")
    metrics = json.loads((tmp_path / "report/metrics.json").read_bytes())
    assert state.status == ("blocked" if blocked else "complete")
    assert metrics["outcome"]["mining_completed"] != blocked
    assert metrics["outcome"]["measured_improvement"] is None
    assert metrics["outcome"]["semantic_review"] == "not_performed"
    assert metrics["usage"]["charged"]["confirmed"]["tokens"] == (
        50000 if blocked else 400
    )
    attempts = [a for slot in metrics["slots"] for a in slot["attempts"]]
    assert len(attempts) == (2 if blocked else 4)
    assert metrics["usage"]["reported_model"]["input_tokens"] == (
        120 if blocked else 240
    )
    if blocked:
        assert [a["status"] for a in attempts] == ["invalid", "budget_exhausted"]
        assert metrics["outcome"]["disposition"] is None
        assert metrics["outcome"]["weakness_count"] is None
    else:
        assert metrics["outcome"]["disposition"] == "no_actionable_weakness"
        assert metrics["outcome"]["weakness_count"] == 0
    assert all(
        job.job_kind in ("trial_analysis", "mining_synthesis", "format_correction")
        for job in worker.jobs
    )
