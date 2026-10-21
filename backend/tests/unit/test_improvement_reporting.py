"""Reports expose typed facts while keeping private artifact contents local."""

import json

from crucible.improvement.reporting import ReportExporter

pytest_plugins = ["tests.integration.test_improvement_final"]


async def test_report_is_deterministic_and_incomplete_is_prominent(
    final_setup, tmp_path
):
    evaluator, _, store, manifest, _, _, _, _ = final_setup
    await store.record_event(
        manifest.experiment_id,
        "untrusted_notes",
        {
            "key": "CANARY_SECRET",
            "raw_log": "CANARY_REFERENCE",
            "path": "/private/CANARY_PATH",
        },
    )
    from uuid import uuid4

    await store.record_event(
        manifest.experiment_id,
        "job_outcome",
        {
            "job_id": str(uuid4()),
            "failure_category": "CANARY_SECRET",
            "termination": "CANARY_REFERENCE",
            "cleanup_confirmed": True,
        },
    )
    exporter = ReportExporter(store, evaluator.paths)
    first = await exporter.export_report(manifest.experiment_id, tmp_path / "report-a")
    second = await exporter.export_report(manifest.experiment_id, tmp_path / "report-b")
    assert first == second
    assert not first.complete
    markdown = (tmp_path / "report-a/report.md").read_text()
    metrics_raw = (tmp_path / "report-a/metrics.json").read_text()
    assert "INCONCLUSIVE" in markdown
    for value in (
        "CANARY_SECRET",
        "CANARY_REFERENCE",
        "CANARY_PATH",
        str(tmp_path),
        "driver_source",
    ):
        assert value not in markdown + metrics_raw
    metrics = json.loads(metrics_raw)
    assert metrics["job_outcomes"][0]["failure_category"] == "unknown"
    assert metrics["job_outcomes"][0]["termination"] == "failed"
    assert metrics["recovery"]["cleanup_confirmed"] is True
    assert metrics["usage"]["dollars"] is None
    assert metrics["usage"]["provider_cache_tokens"] is None
    assert metrics["measurement"]["splits"][0]["expected_trials_per_version"] == 3


async def test_trial_and_majority_task_denominators_stay_distinct(
    final_setup, tmp_path
):
    evaluator, _, store, manifest, _, _, _, _ = final_setup
    await evaluator.evaluate_final(manifest.experiment_id)
    bundle = await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, tmp_path / "report"
    )
    assert bundle.complete
    data = json.loads((tmp_path / "report/metrics.json").read_text())
    final = data["rates"]["final-test"]
    assert final["trial_denominator_per_version"] == 3
    assert final["task_denominator_per_version"] == 1
    assert final["trial_gain_percentage_points"] == 0
    assert final["task_gain_percentage_points"] == 0
    assert data["execution_counts"]["actual_trial_generations"] == 9
    assert data["execution_counts"]["same_source_final_links"] == 1


async def test_unknown_usage_and_human_intervention_remain_explicit(
    final_setup, tmp_path
):
    from crucible.improvement.contracts import UsageAmounts

    evaluator, _, store, manifest, _, _, _, _ = final_setup
    await evaluator.evaluate_final(manifest.experiment_id)
    reservation = await store.reserve_usage(
        manifest.experiment_id, None, UsageAmounts(tokens=7)
    )
    await store.mark_uncertain(reservation)
    await store.record_event(
        manifest.experiment_id,
        "command_approval",
        {"approved": True, "command": "CANARY_COMMAND"},
    )
    bundle = await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, tmp_path / "unknown"
    )
    assert not bundle.complete
    raw = (tmp_path / "unknown/metrics.json").read_text()
    data = json.loads(raw)
    assert data["usage"]["unsettled_reserved"]["tokens"] == 7
    assert data["usage"]["unsettled_reservations"] == 1
    assert data["usage"]["dollars"] is None
    assert data["execution_counts"]["human_approval_decisions"] == 1
    assert "CANARY_COMMAND" not in raw
    assert any(e.get("approved") for e in data["audit_events"])


async def test_verified_public_case_provenance_is_exported(final_setup, tmp_path):
    evaluator, _, store, manifest, _, _, _, _ = final_setup
    directory = tmp_path / "provenance"
    await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, directory
    )
    rows = json.loads((directory / "metrics.json").read_text())["provenance"]
    assert len(rows) == len(manifest.cases)
    assert all(
        row["repository"] and row["base_commit"] and row["task_image"] for row in rows
    )
    assert all(
        row["metadata_verification"] == "frozen case and admission digests verified"
        for row in rows
    )
    assert all(row["license"] is None for row in rows)
    assert "fixture_path" not in (directory / "metrics.json").read_text()


async def test_report_creation_crash_is_recoverable(final_setup, tmp_path, monkeypatch):
    import pytest

    import crucible.improvement.reporting as module

    evaluator, _, store, manifest, _, _, _, _ = final_setup
    exporter = ReportExporter(store, evaluator.paths)
    original = module.atomic_source_file
    crashed = False

    def crash_once(*args):
        nonlocal crashed
        original(*args)
        if not crashed:
            crashed = True
            raise RuntimeError("crash after atomic export")

    monkeypatch.setattr(module, "atomic_source_file", crash_once)
    directory = tmp_path / "atomic-report"
    with pytest.raises(RuntimeError, match="atomic export"):
        await exporter.export_report(manifest.experiment_id, directory)
    assert await exporter.export_report(manifest.experiment_id, directory)


async def test_conservative_accounting_is_never_confirmed_provider_usage(
    final_setup, tmp_path
):
    from crucible.improvement.contracts import UsageAmounts

    evaluator, _, store, manifest, _, _, _, _ = final_setup
    reservation = await store.reserve_usage(
        manifest.experiment_id, None, UsageAmounts(tokens=70)
    )
    await store.mark_uncertain(reservation)
    stopped = await store.request_stop(manifest.experiment_id)
    owner = await store.claim_recovery(
        manifest.experiment_id, stopped.revision, None, 123, "test"
    )
    await store.acknowledge_usage(manifest.experiment_id, owner, (reservation,))
    await store.recover_ready(manifest.experiment_id, owner)
    await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, tmp_path / "conservative"
    )
    data = json.loads((tmp_path / "conservative/metrics.json").read_text())
    assert data["usage"]["conservative_charges"]["tokens"] == 70
    assert data["usage"]["known_actual"]["tokens"] == 0
    assert data["usage"]["dollars"] is None


async def test_report_and_status_block_exhausted_remaining_allowance(
    final_setup, tmp_path
):
    from crucible.improvement.contracts import UsageAmounts
    from crucible.improvement.lifecycle import ExperimentService

    evaluator, _, store, manifest, _, _, _, _ = final_setup
    amount = UsageAmounts(tokens=manifest.budget.max_reported_and_reserved_tokens)
    reservation = await store.reserve_usage(manifest.experiment_id, None, amount)
    await store.settle_usage(reservation, amount)
    await ReportExporter(store, evaluator.paths).export_report(
        manifest.experiment_id, tmp_path / "exhausted"
    )
    data = json.loads((tmp_path / "exhausted/metrics.json").read_text())
    status = await ExperimentService(store, evaluator.paths.root).status(
        manifest.experiment_id
    )
    assert status["permitted_actions"] == ["remain_blocked"]
    assert data["recovery"]["required_operator_action"] == "remain_blocked"
