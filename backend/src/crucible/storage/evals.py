"""SQLite backed evaluation identities and rebuildable projections."""

import subprocess
from collections import Counter
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from crucible.domain.evals import (
    EvalCase,
    EvalResult,
    EvalSuite,
    EvalTrial,
    ResultVerdict,
    RunSummary,
    StepUsage,
    TrialStatus,
    UsageSource,
)
from crucible.storage import models


class SqlAlchemyEvalStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_suite(self, value: EvalSuite) -> None:
        await self._session.execute(
            insert(models.eval_suites).values(
                id=str(value.id),
                partition=value.partition,
                name=value.name,
                definition_digest=value.definition_digest,
                definition_json=value.definition,
                created_at=value.created_at,
            )
        )

    async def get_suite(
        self, partition: str, name: str, digest: str
    ) -> EvalSuite | None:
        row = (
            (
                await self._session.execute(
                    select(models.eval_suites).where(
                        models.eval_suites.c.partition == partition,
                        models.eval_suites.c.name == name,
                        models.eval_suites.c.definition_digest == digest,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return EvalSuite(
            UUID(row["id"]),
            row["partition"],
            row["name"],
            row["definition_digest"],
            row["definition_json"],
            row["created_at"],
        )

    async def add_case(self, value: EvalCase) -> None:
        await self._session.execute(
            insert(models.eval_cases).values(
                id=str(value.id),
                suite_id=str(value.suite_id),
                name=value.name,
                definition_digest=value.definition_digest,
                definition_json=value.definition,
                created_at=value.created_at,
            )
        )

    async def get_case(self, suite_id: UUID, name: str, digest: str) -> EvalCase | None:
        row = (
            (
                await self._session.execute(
                    select(models.eval_cases).where(
                        models.eval_cases.c.suite_id == str(suite_id),
                        models.eval_cases.c.name == name,
                        models.eval_cases.c.definition_digest == digest,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return EvalCase(
            UUID(row["id"]),
            UUID(row["suite_id"]),
            row["name"],
            row["definition_digest"],
            row["definition_json"],
            row["created_at"],
        )

    async def add_trial(self, value: EvalTrial) -> None:
        case_suite = await self._session.scalar(
            select(models.eval_cases.c.suite_id).where(
                models.eval_cases.c.id == str(value.case_id)
            )
        )
        if case_suite != str(value.suite_id):
            raise ValueError("Trial Case belongs to a different Suite")
        await self._session.execute(
            insert(models.eval_trials).values(**_trial_values(value))
        )

    async def get_trial(self, trial_id: UUID) -> EvalTrial | None:
        row = (
            (
                await self._session.execute(
                    select(models.eval_trials).where(
                        models.eval_trials.c.id == str(trial_id)
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return EvalTrial(
            id=UUID(row["id"]),
            suite_id=UUID(row["suite_id"]),
            case_id=UUID(row["case_id"]),
            invocation_id=UUID(row["invocation_id"]),
            repeat_index=row["repeat_index"],
            partition=row["partition"],
            case_digest=row["case_digest"],
            configuration_digest=row["configuration_digest"],
            status=TrialStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            fixture_commit=row["fixture_commit"],
            repository_id=UUID(row["repository_id"]) if row["repository_id"] else None,
            task_id=UUID(row["task_id"]) if row["task_id"] else None,
            run_id=UUID(row["run_id"]) if row["run_id"] else None,
            failure_code=row["failure_code"],
        )

    async def update_trial(self, value: EvalTrial) -> None:
        current = await self.get_trial(value.id)
        if current is None:
            raise ValueError("Trial does not exist")
        if current.status in (
            TrialStatus.COMPLETED,
            TrialStatus.FAILED,
            TrialStatus.INTERRUPTED,
        ):
            raise ValueError("terminal Trial cannot be rewritten")
        identity = (
            "suite_id",
            "case_id",
            "invocation_id",
            "repeat_index",
            "partition",
            "case_digest",
            "configuration_digest",
            "created_at",
        )
        if any(getattr(current, name) != getattr(value, name) for name in identity):
            raise ValueError("Trial lineage is immutable")
        result = await self._session.execute(
            update(models.eval_trials)
            .where(
                models.eval_trials.c.id == str(value.id),
                models.eval_trials.c.status == current.status.value,
            )
            .values(
                **{
                    key: item
                    for key, item in _trial_values(value).items()
                    if key
                    not in {
                        "id",
                        "suite_id",
                        "case_id",
                        "invocation_id",
                        "repeat_index",
                        "partition",
                        "case_digest",
                        "configuration_digest",
                        "created_at",
                    }
                }
            )
        )
        if cast(CursorResult[object], result).rowcount != 1:
            raise ValueError("Trial changed concurrently")

    async def add_result(self, value: EvalResult) -> None:
        await self._session.execute(
            insert(models.eval_results).values(
                trial_id=str(value.trial_id),
                verdict=value.verdict.value,
                evaluator_results_json=list(value.evaluator_results),
                report_artifact_id=str(value.report_artifact_id)
                if value.report_artifact_id
                else None,
                created_at=value.created_at,
            )
        )

    async def get_result(self, trial_id: UUID) -> EvalResult | None:
        row = (
            (
                await self._session.execute(
                    select(models.eval_results).where(
                        models.eval_results.c.trial_id == str(trial_id)
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return EvalResult(
            trial_id,
            ResultVerdict(row["verdict"]),
            tuple(cast(list[dict[str, object]], row["evaluator_results_json"])),
            UUID(row["report_artifact_id"]) if row["report_artifact_id"] else None,
            row["created_at"],
        )

    async def list_trials(self, invocation_id: UUID) -> tuple[EvalTrial, ...]:
        ids = (
            (
                await self._session.execute(
                    select(models.eval_trials.c.id)
                    .where(models.eval_trials.c.invocation_id == str(invocation_id))
                    .order_by(
                        models.eval_trials.c.case_id, models.eval_trials.c.repeat_index
                    )
                )
            )
            .scalars()
            .all()
        )
        values = [await self.get_trial(UUID(item)) for item in ids]
        return tuple(value for value in values if value is not None)

    async def list_incomplete_trials(self) -> tuple[EvalTrial, ...]:
        ids = (
            (
                await self._session.execute(
                    select(models.eval_trials.c.id)
                    .where(
                        models.eval_trials.c.status.in_(
                            ("queued", "preparing", "running", "evaluating")
                        )
                    )
                    .order_by(models.eval_trials.c.created_at, models.eval_trials.c.id)
                )
            )
            .scalars()
            .all()
        )
        values = [await self.get_trial(UUID(item)) for item in ids]
        return tuple(value for value in values if value is not None)

    async def add_usage(self, value: StepUsage) -> None:
        await self._session.execute(
            insert(models.step_usage).values(
                step_id=str(value.step_id),
                run_id=str(value.run_id),
                model_id=value.model_id,
                input_tokens=value.input_tokens,
                output_tokens=value.output_tokens,
                source=value.source.value,
                created_at=value.created_at,
            )
        )

    async def list_usage(self, run_id: UUID) -> tuple[StepUsage, ...]:
        rows = (
            (
                await self._session.execute(
                    select(models.step_usage)
                    .where(models.step_usage.c.run_id == str(run_id))
                    .order_by(models.step_usage.c.step_id)
                )
            )
            .mappings()
            .all()
        )
        return tuple(
            StepUsage(
                UUID(row["step_id"]),
                UUID(row["run_id"]),
                row["model_id"],
                row["input_tokens"],
                row["output_tokens"],
                UsageSource(row["source"]),
                row["created_at"],
            )
            for row in rows
        )

    async def upsert_summary(self, value: RunSummary) -> None:
        statement = sqlite_insert(models.run_summaries).values(
            run_id=str(value.run_id),
            schema_version=value.schema_version,
            projection_json=value.projection,
            created_at=value.created_at,
        )
        await self._session.execute(
            statement.on_conflict_do_update(
                index_elements=["run_id"],
                set_={
                    "schema_version": value.schema_version,
                    "projection_json": value.projection,
                    "created_at": value.created_at,
                },
            )
        )

    async def get_summary(self, run_id: UUID) -> RunSummary | None:
        row = (
            (
                await self._session.execute(
                    select(models.run_summaries).where(
                        models.run_summaries.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return RunSummary(
            run_id,
            row["schema_version"],
            row["projection_json"],
            cast(datetime, row["created_at"]),
        )

    async def rebuild_summary(self, run_id: UUID, now: datetime) -> RunSummary:
        run = (
            (
                await self._session.execute(
                    select(models.runs).where(models.runs.c.id == str(run_id))
                )
            )
            .mappings()
            .one()
        )
        task = (
            (
                await self._session.execute(
                    select(models.tasks).where(models.tasks.c.id == run["task_id"])
                )
            )
            .mappings()
            .one()
        )
        usages = (
            (
                await self._session.execute(
                    select(models.step_usage).where(
                        models.step_usage.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        calls = (
            (
                await self._session.execute(
                    select(models.tool_calls).where(
                        models.tool_calls.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        results = (
            (
                await self._session.execute(
                    select(models.tool_results).where(
                        models.tool_results.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        approvals = (
            (
                await self._session.execute(
                    select(models.approvals).where(
                        models.approvals.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .all()
        )
        steps = (
            (
                await self._session.execute(
                    select(models.steps.c.id).where(
                        models.steps.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        events = (
            (
                await self._session.execute(
                    select(models.task_events.c.id).where(
                        models.task_events.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        validations = (
            (
                await self._session.execute(
                    select(models.validation_attempts.c.id).where(
                        models.validation_attempts.c.run_id == str(run_id)
                    )
                )
            )
            .scalars()
            .all()
        )
        compactions = (
            (
                await self._session.execute(
                    select(models.compactions.c.id).where(
                        models.compactions.c.task_id == run["task_id"],
                        models.compactions.c.created_at >= run["created_at"],
                        models.compactions.c.created_at <= (run["completed_at"] or now),
                    )
                )
            )
            .scalars()
            .all()
        )
        approval_wait = sum(
            (item["decided_at"] - item["created_at"]).total_seconds()
            for item in approvals
            if item["decided_at"] is not None
        )
        calls_by_id = {str(item["id"]): item for item in calls}
        approval_wait_by_call = {
            str(item["tool_call_id"]): (
                item["decided_at"] - item["created_at"]
            ).total_seconds()
            for item in approvals
            if item["decided_at"] is not None
        }
        command_seconds = sum(
            max(
                0.0,
                (
                    item["completed_at"]
                    - calls_by_id[str(item["tool_call_id"])]["created_at"]
                ).total_seconds()
                - approval_wait_by_call.get(str(item["tool_call_id"]), 0.0),
            )
            for item in results
            if str(item["tool_call_id"]) in calls_by_id
            and calls_by_id[str(item["tool_call_id"])]["name"] == "execute_command"
        )
        wall = ((run["completed_at"] or now) - run["created_at"]).total_seconds()
        active = (
            (run["completed_at"] or now) - (run["started_at"] or run["created_at"])
        ).total_seconds()
        changed_files: int | None = None
        if task["base_revision"] and task["workspace_path"]:
            try:
                diff = subprocess.run(
                    [
                        "git",
                        "-C",
                        task["workspace_path"],
                        "diff",
                        "--name-only",
                        task["base_revision"],
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if diff.returncode == 0:
                    changed_files = len(set(diff.stdout.splitlines()))
            except OSError:
                pass
        projection: dict[str, object] = {
            "schema_version": 1,
            "outcome": run["status"],
            "outcome_code": run["outcome_code"],
            "step_count": len(steps),
            "tokens": {
                "input": sum(item["input_tokens"] for item in usages),
                "output": sum(item["output_tokens"] for item in usages),
                "sources": sorted({item["source"] for item in usages}),
                "models": sorted({item["model_id"] for item in usages}),
            },
            "estimated_cost": None,
            "price_table_version": None,
            "tool_calls_by_kind": dict(Counter(item["name"] for item in calls)),
            "tool_results_by_status": dict(Counter(item["status"] for item in results)),
            "approval_wait_seconds": approval_wait,
            "wall_seconds": wall,
            "active_seconds": max(0.0, active - approval_wait),
            "command_seconds": command_seconds,
            "validation_attempts": len(validations),
            "compactions": len(compactions),
            "files_changed": changed_files,
            "raw_trace_ids": {
                "run": str(run_id),
                "task": run["task_id"],
                "steps": list(steps),
                "tool_calls": [item["id"] for item in calls],
                "tool_results": [item["id"] for item in results],
                "events": list(events),
            },
        }
        summary = RunSummary(run_id, 1, projection, now)
        await self.upsert_summary(summary)
        return summary


def _trial_values(value: EvalTrial) -> dict[str, object]:
    return {
        "id": str(value.id),
        "suite_id": str(value.suite_id),
        "case_id": str(value.case_id),
        "invocation_id": str(value.invocation_id),
        "repeat_index": value.repeat_index,
        "partition": value.partition,
        "case_digest": value.case_digest,
        "configuration_digest": value.configuration_digest,
        "status": value.status.value,
        "created_at": value.created_at,
        "updated_at": value.updated_at,
        "fixture_commit": value.fixture_commit,
        "repository_id": str(value.repository_id) if value.repository_id else None,
        "task_id": str(value.task_id) if value.task_id else None,
        "run_id": str(value.run_id) if value.run_id else None,
        "failure_code": value.failure_code,
    }
