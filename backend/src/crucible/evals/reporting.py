"""Rebuildable, comparable Trial reports from durable Run evidence."""

import json
import tomllib
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import UnitOfWork
from crucible.domain.evals import (
    EvalResult,
    EvalTrial,
    RunSummary,
    StepUsage,
    UsageSource,
)


@dataclass(frozen=True)
class PriceTable:
    version: str
    currency: str
    rates: Mapping[tuple[str, UsageSource], tuple[Decimal, Decimal]]


@dataclass(frozen=True)
class EvalReport:
    body: dict[str, object]
    artifact_id: UUID | None = None

    def json_bytes(self) -> bytes:
        return (
            json.dumps(self.body, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()


def load_price_table(path: Path) -> PriceTable:
    value = tomllib.loads(path.read_text())
    if value.get("schema_version") != 1 or value.get("unit") != "per_million_tokens":
        raise ValueError("unsupported price table schema or unit")
    version = str(value.get("version", ""))
    currency = str(value.get("currency", ""))
    if not version or not currency:
        raise ValueError("price table requires version and currency")
    rates: dict[tuple[str, UsageSource], tuple[Decimal, Decimal]] = {}
    for row in value.get("rates", []):
        key = (str(row["model"]), UsageSource(str(row["basis"])))
        if key in rates:
            raise ValueError("duplicate model and usage basis in price table")
        input_rate = Decimal(str(row["input_per_million"]))
        output_rate = Decimal(str(row["output_per_million"]))
        if input_rate < 0 or output_rate < 0:
            raise ValueError("token prices cannot be negative")
        rates[key] = (input_rate, output_rate)
    return PriceTable(version, currency, rates)


def build_report(
    trial: EvalTrial,
    result: EvalResult,
    summary: RunSummary,
    usage: tuple[StepUsage, ...],
    approval_decisions: tuple[str, ...],
    suite_digest: str,
    price_table: PriceTable | None = None,
) -> EvalReport:
    sources = {item.source.value for item in usage}
    source = (
        next(iter(sources))
        if len(sources) == 1
        else "mixed"
        if sources
        else "unavailable"
    )
    cost: dict[str, str] | None = None
    if price_table is not None and usage:
        amounts = []
        for item in usage:
            rates = price_table.rates.get((item.model_id, item.source))
            if rates is None:
                break
            amounts.append(
                Decimal(item.input_tokens) * rates[0] / Decimal(1_000_000)
                + Decimal(item.output_tokens) * rates[1] / Decimal(1_000_000)
            )
        else:
            cost = {
                "amount": str(sum(amounts, Decimal(0))),
                "currency": price_table.currency,
                "price_table_version": price_table.version,
            }
    projection = summary.projection
    body: dict[str, object] = {
        "schema_version": 1,
        "invocation_id": str(trial.invocation_id),
        "suite_id": str(trial.suite_id),
        "suite_digest": suite_digest,
        "case_id": str(trial.case_id),
        "case_digest": trial.case_digest,
        "trial_id": str(trial.id),
        "repeat_index": trial.repeat_index,
        "partition": trial.partition,
        "configuration_digest": trial.configuration_digest,
        "fixture_commit": trial.fixture_commit,
        "repository_id": str(trial.repository_id) if trial.repository_id else None,
        "task_id": str(trial.task_id) if trial.task_id else None,
        "run_id": str(trial.run_id) if trial.run_id else None,
        "trial_status": trial.status.value,
        "verdict": result.verdict.value,
        "evaluator_results": list(result.evaluator_results),
        "run_outcome": projection.get("outcome"),
        "policy": {
            "approval_decisions": sorted(approval_decisions),
            "compliant": "pending" not in approval_decisions,
        },
        "tokens": {
            "input": sum(item.input_tokens for item in usage),
            "output": sum(item.output_tokens for item in usage),
            "source": source,
            "by_step": [
                {
                    "step_id": str(item.step_id),
                    "model_id": item.model_id,
                    "input": item.input_tokens,
                    "output": item.output_tokens,
                    "source": item.source.value,
                }
                for item in sorted(usage, key=lambda item: str(item.step_id))
            ],
        },
        "estimated_cost": cost,
        "time": {
            "wall_seconds": projection.get("wall_seconds"),
            "active_seconds": projection.get("active_seconds"),
            "command_seconds": projection.get("command_seconds"),
            "approval_wait_seconds": projection.get("approval_wait_seconds"),
        },
        "tools": {
            "by_kind": projection.get("tool_calls_by_kind", {}),
            "by_status": projection.get("tool_results_by_status", {}),
        },
        "validation_attempts": projection.get("validation_attempts"),
        "compactions": projection.get("compactions"),
        "files_changed": projection.get("files_changed"),
        "raw_trace_ids": projection.get("raw_trace_ids", {}),
    }
    return EvalReport(body)


class EvalReporter:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        artifacts: ArtifactService,
        price_table: PriceTable | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._artifacts = artifacts
        self._price_table = price_table

    async def create(
        self, trial: EvalTrial, result: EvalResult, suite_digest: str
    ) -> EvalReport:
        if trial.run_id is None or trial.task_id is None:
            raise ValueError("a report requires a Task and Run")
        async with self._unit_of_work() as uow:
            summary = await uow.evals.get_summary(trial.run_id)
            usage = await uow.evals.list_usage(trial.run_id)
            approvals = await uow.approvals.list_for_run(trial.run_id)
        if summary is None:
            raise ValueError("terminal Run summary is missing")
        report = build_report(
            trial,
            result,
            summary,
            usage,
            tuple(item.status.value for item in approvals),
            suite_digest,
            self._price_table,
        )

        async def chunks() -> AsyncIterator[bytes]:
            yield report.json_bytes()

        artifact = await self._artifacts.put(
            trial.task_id, "application/json", "private", chunks()
        )
        return EvalReport(report.body, artifact.id)
