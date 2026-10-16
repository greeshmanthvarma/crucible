"""Private local observations; missing historical measurements stay unavailable."""

from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
from typing import Any
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock


class RepairMetrics:
    def __init__(self, factory: Callable[[], UnitOfWork], clock: Clock) -> None:
        self.factory, self.clock = factory, clock

    async def build(self, repair_id: UUID) -> dict[str, Any]:
        async with self.factory() as uow:
            repair = await uow.repairs.get(repair_id)
            if repair is None:
                raise ValueError("Repair not found")
            attempts = await uow.repairs.list_attempts(repair_id)
            fixes = await uow.repairs.list_fixes(repair_id)
            checkpoints = await uow.repairs.list_checkpoints(repair_id)
            jobs = await uow.repair_trials.list_for_repair(repair_id)
            publication = await uow.repairs.get_publication(repair_id)
            role_usage = [
                row
                for a in attempts
                if a.harness_run_id
                for row in await uow.evals.list_usage(a.harness_run_id)
            ]
            evidence = {}
            for job in jobs:
                for row in await uow.repair_trials.list_results(
                    job.fingerprint, job.case_id, job.side
                ):
                    if row.id == job.id:
                        evidence[row.id] = row
            runs = [
                run
                for a in attempts
                if a.harness_run_id
                if (run := await uow.runs.get(a.harness_run_id)) is not None
            ]
            compactions = {}
            for run in runs:
                compaction = await uow.compactions.latest_for_task(run.task_id)
                while compaction is not None and compaction.id not in compactions:
                    compactions[compaction.id] = compaction
                    compaction = (
                        await uow.compactions.get(compaction.previous_compaction_id)
                        if compaction.previous_compaction_id
                        else None
                    )
        role_for_run = {a.harness_run_id: a.role for a in attempts if a.harness_run_id}
        usage_by_role: dict[str, dict[str, dict[str, int]]] = {}
        for step_usage in role_usage:
            totals = usage_by_role.setdefault(
                role_for_run[step_usage.run_id], {}
            ).setdefault(
                step_usage.source,
                {"requests": 0, "input_tokens": 0, "output_tokens": 0},
            )
            totals["requests"] += 1
            totals["input_tokens"] += step_usage.input_tokens
            totals["output_tokens"] += step_usage.output_tokens
        usage = {
            basis: {"requests": 0, "input_tokens": 0, "output_tokens": 0}
            for basis in ("reported", "estimated")
        }
        for step_usage in role_usage:
            total = usage[step_usage.source]
            total["requests"] += 1
            total["input_tokens"] += step_usage.input_tokens
            total["output_tokens"] += step_usage.output_tokens
        for row in evidence.values():
            trial_total = usage.get(str(row.usage.get("basis")))
            if trial_total is not None:
                for name in trial_total:
                    trial_total[name] += int(row.usage.get(name, 0))
        cases: dict[tuple[str, str, str], Counter[str]] = {}
        for row in evidence.values():
            cases.setdefault((row.case_id, row.side, row.source_digest), Counter())[
                row.verdict
            ] += 1
        model_ids = sorted({row.model_id for row in role_usage})
        kind = (
            "simulated"
            if model_ids and all(m in {"fake", "recorded", "test"} for m in model_ids)
            else "provider_recorded"
            if model_ids
            else "unknown"
        )
        batch_reuse = []
        for checkpoint in checkpoints:
            state = checkpoint.result_refs_json.get("controller_state")
            if isinstance(state, dict):
                count = state.get("baseline_batch_cache_hits")
                if type(count) is int:
                    batch_reuse.append(count)
        cache_records = [c for c in checkpoints if "cache_reuse" in c.result_refs_json]
        # Completed trial jobs and role runs have measured intervals. These are
        # execution wall intervals, not CPU time or provider billing duration.
        active = [
            float(row.usage["worker_wall_seconds"])
            for row in evidence.values()
            if "worker_wall_seconds" in row.usage
        ]
        role_intervals = [
            (r.completed_at - r.started_at).total_seconds()
            for r in runs
            if r.completed_at and r.started_at
        ]
        terminal = repair.status in {"completed", "exhausted", "closed", "invalidated"}
        end = repair.updated_at if terminal else self.clock.now()
        return {
            "schema_version": 1,
            "repair_id": str(repair_id),
            "source_task_id": str(repair.source_task_id),
            "runtime_identity": repair.initial_identity_json,
            "trial_records": [asdict(row) for row in evidence.values()],
            "status": repair.status,
            "outcome": repair.outcome_code,
            "evidence_kind": kind,
            "natural_failure_verified": False,
            "model_ids": model_ids,
            "attempts": {
                "total": len(attempts),
                "charged": sum(a.charged for a in attempts),
                "by_role": dict(Counter(a.role for a in attempts)),
                "by_status": dict(Counter(a.status for a in attempts)),
                "records": [asdict(a) for a in attempts],
            },
            "fixes": {
                "diagnosed": len(fixes),
                "accepted": sum(f.status == "accepted" for f in fixes),
                "records": [asdict(f) for f in fixes],
            },
            "usage": usage,
            "usage_by_role": usage_by_role,
            "compaction_usage": {
                "requests": len(compactions),
                "input_tokens": sum(c.input_tokens for c in compactions.values()),
                "output_tokens": sum(c.output_tokens for c in compactions.values()),
                "basis": "unrecorded; excluded from reported/estimated totals",
            },
            "usage_scope": (
                "recorded role steps and executed trial jobs; "
                "cached evidence is not billed again"
            ),
            "wall_seconds": max(0, (end - repair.created_at).total_seconds()),
            "role_execution_seconds": sum(role_intervals)
            if len(role_intervals) == len(runs) and runs
            else None,
            "trial_active_seconds": sum(active)
            if len(active) == len(jobs) and jobs
            else None,
            "active_time_basis": (
                "worker wall intervals; role run intervals; "
                "missing or interrupted measurements remain null"
            ),
            "cache_hits": (len(cache_records) + max(batch_reuse, default=0))
            if cache_records or batch_reuse
            else None,
            "cache_units": (
                "reused trial observations, including compatible whole-baseline batches"
            ),
            "cache_coverage": (
                "recorded hits only; historical hits and "
                "whole-batch reuse before this version may be unavailable"
            ),
            "usage_missing_trial_jobs": len(jobs) - len(evidence),
            "trial_jobs": len(jobs),
            "completed_observations": len(evidence),
            "cases": [
                {
                    "case_id": key[0],
                    "side": key[1],
                    "source_digest": key[2],
                    "counts": dict(counts),
                    "observations": sum(counts.values()),
                }
                for key, counts in sorted(cases.items())
            ],
            "publication": asdict(publication) if publication else None,
        }
