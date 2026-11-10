"""Run case mining under a measured round's evidence and budget authority."""

import asyncio
import hashlib
from collections.abc import AsyncIterator
from math import ceil
from pathlib import Path
from uuid import UUID, uuid4

from crucible.engine.gateway import (
    ModelError,
    ModelGateway,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
)
from crucible.improvement.case_mining import CaseMiningReport, mine_development_cases
from crucible.improvement.contracts import (
    EDITABLE_PATHS,
    ExperimentManifest,
    UsageAmounts,
)
from crucible.improvement.evidence_views import DevelopmentViewBuilder
from crucible.improvement.role_contracts import (
    DevelopmentView,
    MiningResult,
    RoleExecution,
    Weakness,
)
from crucible.improvement.store import BudgetDenied, ExperimentStore, RoundRecord
from crucible.models.token_counting import estimate_input_tokens
from crucible.repair.identity import canonical_json
from crucible.repair.worker_protocol import encode_event


def _proposal_input(report: CaseMiningReport, view: DevelopmentView) -> MiningResult:
    traces = {
        ref.trial_id: ref for ref in view.evidence_refs if ref.artifact == "trace"
    }
    weaknesses = []
    for group in report.groups:
        supporting = tuple(
            attempt
            for attempt in group.attempts
            if attempt.diagnosis.relationship in {"linked", "contributing"}
        )
        if not supporting:
            continue
        representative = next(
            (a for a in supporting if a.trial_id == group.representative_trial_id),
            supporting[0],
        )
        supporting_cases = len({a.case_id for a in supporting})
        observed = representative.diagnosis.observed_behavior[:2800]
        citations = ", ".join(
            f"{event.id}: {(event.excerpt or event.error_code or event.status)[:160]}"
            for event in representative.cited_results[:4]
        )
        weaknesses.append(
            Weakness(
                id=group.label,
                observed_behavior=(
                    f"{len(supporting)} supporting attempts across {supporting_cases} "
                    f"development tasks. {observed} Representative results: {citations}"
                ),
                hypothesized_mechanism=(
                    "Investigate whether the harness guidance or feedback for this "
                    "tool contributed to the observed failure or hindered recovery."
                ),
                evidence=tuple(traces[a.trial_id] for a in supporting),
                editable_paths=EDITABLE_PATHS,
                confidence="low",
                risks=("A recorded tool failure can originate in the solver or task.",),
                limitations=representative.diagnosis.uncertainty[:2000],
            )
        )
    weaknesses.sort(key=lambda weakness: (-len(weakness.evidence), weakness.id))
    diagnosed = sum(a.outcome in ("grouped", "unresolved") for a in report.attempts)
    result = MiningResult(
        schema_version=1,
        disposition=(
            "actionable"
            if weaknesses
            else "no_actionable_weakness"
            if any(a.outcome == "grouped" for a in report.attempts)
            else "insufficient_evidence"
        ),
        weaknesses=tuple(weaknesses[:5]),
        summary=(
            f"Diagnosed {diagnosed} of {len(report.attempts)} failed development "
            "attempts. "
            "Groups are leads for source investigation."
        ),
    )
    result.validate_evidence(view)
    return result


class _RoundGateway:
    def __init__(
        self,
        store: ExperimentStore,
        manifest: ExperimentManifest,
        job_id: UUID,
        upstream: ModelGateway,
    ) -> None:
        self.store, self.manifest, self.job_id, self.upstream = (
            store,
            manifest,
            job_id,
            upstream,
        )
        self.deadline = asyncio.get_running_loop().time() + min(
            manifest.role_limits["mining"].wall_timeout_seconds,
            manifest.role_limits["mining"].max_active_seconds,
        )
        self.requests = 0

    async def _watch_stop(self, owner: asyncio.Task[object]) -> None:
        while True:
            await asyncio.sleep(0.5)
            if (await self.store.get(self.manifest.experiment_id)).stop_requested:
                owner.cancel()
                return

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        loop = asyncio.get_running_loop()
        remaining = min(90.0, self.deadline - loop.time())
        estimated = estimate_input_tokens(
            request.messages, request.tools, response_format=request.response_format
        )
        model = self.manifest.model
        if (
            remaining <= 0
            or self.requests >= self.manifest.role_limits["mining"].max_steps
            or estimated > model.input_limit
            or request.max_output_tokens > model.output_reserve
        ):
            yield ModelError(
                "improvement_budget_exhausted", "Frozen mining limit reached"
            )
            return
        if request.model != model.model_id or request.tools:
            raise ValueError("invalid measured mining request")
        experiment_id = self.manifest.experiment_id
        output_cap = min(65536, self.manifest.command_policy.max_output_bytes_per_call)
        reservation = UsageAmounts(
            tokens=estimated + request.max_output_tokens,
            requests=1,
            active_seconds=ceil(remaining),
            output_bytes=output_cap,
        )
        try:
            ledger = await self.store.reserve_usage(
                experiment_id, None, reservation, job_id=self.job_id
            )
        except BudgetDenied:
            yield ModelError(
                "improvement_budget_exhausted", "Frozen mining quota reached"
            )
            return
        self.requests += 1
        await self.store.record_event(
            experiment_id,
            "request_dispatched",
            {
                "job_id": str(self.job_id),
                "reservation_id": str(ledger),
                "dispatch": "possibly_sent",
            },
        )
        started = loop.time()
        input_tokens = output_tokens = None
        size = 0
        owner = asyncio.current_task()
        assert owner is not None
        watcher = asyncio.create_task(self._watch_stop(owner))
        stream = None
        try:
            async with asyncio.timeout(remaining):
                stream = self.upstream.stream(request)
                async for event in stream:
                    if isinstance(event, ModelUsage):
                        input_tokens, output_tokens = (
                            event.input_tokens,
                            event.output_tokens,
                        )
                    size += len(canonical_json(encode_event(event))) + 1
                    if size > output_cap:
                        raise ValueError("measured mining output cap exceeded")
                    yield event
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            close = getattr(stream, "aclose", None)
            try:
                if close is not None:
                    await close()
            finally:
                observed = UsageAmounts(
                    tokens=(input_tokens or 0) + (output_tokens or 0),
                    requests=1,
                    active_seconds=ceil(loop.time() - started),
                    output_bytes=size,
                )
                if input_tokens is None or output_tokens is None:
                    await self.store.record_partial_usage(ledger, observed, ("tokens",))
                else:
                    await self.store.settle_usage(ledger, observed)
                await self.store.record_event(
                    experiment_id,
                    "request_terminal",
                    {
                        "job_id": str(self.job_id),
                        "reservation_id": str(ledger),
                        "dispatch": "possibly_sent",
                        "usage_basis": "reported"
                        if input_tokens is not None and output_tokens is not None
                        else "partial",
                    },
                )


class MeasuredCaseMiner:
    def __init__(
        self,
        store: ExperimentStore,
        root: Path,
        views: DevelopmentViewBuilder,
        gateway: ModelGateway,
    ) -> None:
        self.store, self.root, self.views, self.gateway = store, root, views, gateway

    async def mine(self, round: RoundRecord, view: DevelopmentView) -> RoleExecution:
        run = await self.store.require_search_open(round.experiment_id)
        if view.baseline_snapshot_id != round.baseline_snapshot_id:
            raise ValueError("case mining baseline drift")
        if await self.store.roles(round.id):
            raise ValueError("case mining already scheduled")
        role_id = uuid4()
        packages = await self.views.build_case_packages(
            run.id, view, self.views.export_root(view).parent / str(role_id)
        )
        directory = self.root / "roles" / str(role_id)
        directory.mkdir(parents=True, mode=0o700)
        encoded = canonical_json(
            {
                "kind": "case-mining-v1",
                "view_index_digest": view.index_digest,
                "bundle_digests": [p.bundle_digest for p in packages],
            }
        )
        input_path = directory / "input.json"
        input_path.write_bytes(encoded)
        execution = RoleExecution(
            role_id=role_id,
            job_id=uuid4(),
            round_id=round.id,
            experiment_id=run.id,
            kind="mining",
            accepted_snapshot_id=view.baseline_snapshot_id,
            source_digest=view.source_digest,
            input_ref=str(input_path.relative_to(self.root)),
            input_digest=hashlib.sha256(encoded).hexdigest(),
            status="running",
            usage_ref=role_id,
        )
        await self.store.save_role(execution, 0, run.revision)
        await self.store.record_event(
            run.id,
            "role_usage_group",
            {"usage_ref": str(role_id), "job_id": str(execution.job_id)},
        )
        try:
            gateway = _RoundGateway(
                self.store, run.manifest, execution.job_id, self.gateway
            )
            report = await mine_development_cases(
                packages,
                gateway=gateway,
                model=run.manifest.model.model_id,
                batch_token_cap=run.manifest.role_limits["mining"].max_model_tokens,
            )
            report_bytes = canonical_json(report)
            report_path = directory / "case-mining-report.json"
            report_path.write_bytes(report_bytes)
            await self.store.record_event(
                run.id,
                "case_mining_report",
                {
                    "role_id": str(role_id),
                    "path": str(report_path.relative_to(self.root)),
                    "sha256": hashlib.sha256(report_bytes).hexdigest(),
                },
            )
            if await self.store.unsettled_usage(run.id):
                raise ValueError("case mining usage requires reconciliation")
            if any(a.error_code == "budget_exceeded" for a in report.attempts):
                raise BudgetDenied("case mining budget exhausted")
            if any(
                a.error_code
                in {"gateway_error", "tokenizer_unavailable", "incomplete_response"}
                for a in report.attempts
            ):
                raise ValueError("case mining execution incomplete")
            result = canonical_json(_proposal_input(report, view))
            output = directory / "assistant.txt"
            output.write_bytes(result)
            execution = execution.model_copy(
                update={
                    "status": "complete",
                    "termination": "completed",
                    "output_ref": str(output.relative_to(self.root)),
                    "output_digest": hashlib.sha256(result).hexdigest(),
                }
            )
            current = await self.store.require_search_open(run.id)
            await self.store.save_role(execution, 0, current.revision)
        except BaseException as error:
            execution = execution.model_copy(
                update={
                    "status": "interrupted"
                    if isinstance(error, asyncio.CancelledError)
                    else "error",
                    "termination": "budget_exhausted"
                    if isinstance(error, BudgetDenied)
                    else None,
                    "error_code": "case_mining_incomplete",
                }
            )
            await self.store.save_role(
                execution, 0, (await self.store.get(run.id)).revision
            )
            raise
        return execution
