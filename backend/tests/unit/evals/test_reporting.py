from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from crucible.domain.evals import (
    EvalResult,
    EvalTrial,
    ResultVerdict,
    RunSummary,
    StepUsage,
    UsageSource,
)
from crucible.domain.ids import new_id
from crucible.evals.reporting import PriceTable, build_report, load_price_table


def test_report_keeps_token_provenance_unknown_cost_and_trace() -> None:
    now = datetime.now(UTC)
    trial = EvalTrial.queued(
        new_id(),
        new_id(),
        new_id(),
        new_id(),
        1,
        "development",
        "case-digest",
        "config-digest",
        now,
    )
    run_id = new_id()
    trial = trial.prepare(now).run(new_id(), new_id(), run_id, "a" * 40, now)
    trial = trial.evaluate(now).complete(now)
    result = EvalResult(
        trial.id, ResultVerdict.FAILED, ({"code": "property_failed"},), None, now
    )
    summary = RunSummary(
        run_id,
        1,
        {
            "outcome": "completed",
            "wall_seconds": 2.0,
            "active_seconds": 1.5,
            "command_seconds": 0.0,
            "tool_calls_by_kind": {"write_file": 1},
            "tool_results_by_status": {"succeeded": 1},
            "validation_attempts": 1,
            "compactions": 0,
            "files_changed": 1,
            "raw_trace_ids": {"run": str(run_id), "steps": ["step"]},
        },
        now,
    )
    usage = (
        StepUsage(new_id(), run_id, "exact-model", 100, 20, UsageSource.ESTIMATED, now),
    )
    report = build_report(trial, result, summary, usage, ("denied",), "suite-digest")
    assert report.body["estimated_cost"] is None
    assert report.body["tokens"]["source"] == "estimated"
    assert report.body["policy"]["approval_decisions"] == ["denied"]
    assert report.body["raw_trace_ids"]["run"] == str(run_id)
    assert report.body["configuration_digest"] == "config-digest"


def test_exact_versioned_price_matches_model_and_usage_basis() -> None:
    now = datetime.now(UTC)
    run_id = new_id()
    trial = (
        EvalTrial.queued(
            new_id(),
            new_id(),
            new_id(),
            new_id(),
            1,
            "development",
            "case",
            "config",
            now,
        )
        .prepare(now)
        .run(new_id(), new_id(), run_id, "a" * 40, now)
        .evaluate(now)
        .complete(now)
    )
    result = EvalResult(trial.id, ResultVerdict.PASSED, (), None, now)
    summary = RunSummary(run_id, 1, {"outcome": "completed", "raw_trace_ids": {}}, now)
    usage = (
        StepUsage(
            new_id(),
            run_id,
            "exact-model",
            1_000_000,
            500_000,
            UsageSource.REPORTED,
            now,
        ),
    )
    prices = PriceTable(
        "prices-1",
        "USD",
        {("exact-model", UsageSource.REPORTED): (Decimal("2"), Decimal("4"))},
    )
    report = build_report(trial, result, summary, usage, (), "suite", prices)
    assert report.body["estimated_cost"] == {
        "amount": "4",
        "currency": "USD",
        "price_table_version": "prices-1",
    }
    estimated = (
        StepUsage(new_id(), run_id, "exact-model", 100, 20, UsageSource.ESTIMATED, now),
    )
    assert (
        build_report(trial, result, summary, estimated, (), "suite", prices).body[
            "estimated_cost"
        ]
        is None
    )


def test_price_table_loader_requires_explicit_version_and_basis(tmp_path: Path) -> None:
    path = tmp_path / "prices.toml"
    path.write_text(
        'schema_version = 1\nversion = "2026-09"\ncurrency = "USD"\n'
        'unit = "per_million_tokens"\n'
        '[[rates]]\nmodel = "exact-model"\nbasis = "reported"\n'
        'input_per_million = "2"\noutput_per_million = "4"\n'
    )
    table = load_price_table(path)
    assert table.version == "2026-09"
    assert table.rates[("exact-model", UsageSource.REPORTED)] == (
        Decimal("2"),
        Decimal("4"),
    )
