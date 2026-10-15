"""Protected verdicts and durable execution checkpoints for incident trials."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID, uuid4

from crucible.repair.cases import ApprovedCase, _freeze
from crucible.repair.fingerprints import SourceIdentity, capture_source_identity
from crucible.repair.gateway import TrustedUsage
from crucible.repair.identity import canonical_json, digest
from crucible.repair.verification import _read, verify_observation
from crucible.repair.worker_protocol import WorkerJob, read_observation
from crucible.repair.worker_supervisor import WorkerExecution


@dataclass(frozen=True)
class TrialJob:
    id: UUID
    invocation_id: UUID
    candidate_generation: int
    case_id: str
    case_digest: str
    source_digest: str
    fingerprint: str
    side: str
    repeat_index: int
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    observation_artifact_id: UUID | None = None
    repair_id: UUID | None = None


@dataclass(frozen=True)
class TrialEvidence:
    id: UUID
    fingerprint: str
    case_id: str
    repeat_index: int
    side: str
    source_digest: str
    observation_artifact_id: UUID | None
    verdict: str
    assertion_results: tuple[dict[str, Any], ...]
    usage: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        from crucible.domain.clock import require_utc

        require_utc(self.created_at)
        if self.verdict not in {"pass", "fail", "error", "unsupported"}:
            raise ValueError("Invalid protected verdict")
        object.__setattr__(
            self,
            "assertion_results",
            tuple(_freeze(dict(a)) for a in self.assertion_results),
        )
        object.__setattr__(self, "usage", _freeze(dict(self.usage)))


@dataclass(frozen=True)
class GateResult:
    eligible: bool
    reasons: tuple[str, ...]
    counts: dict[str, dict[str, int]]

    def public_feedback(self, *, held_out: bool = False) -> str:
        if held_out:
            return (
                "Evaluation gate passed."
                if self.eligible
                else "Evaluation gate failed."
            )
        return (
            "Evaluation gate passed."
            if self.eligible
            else "Evaluation gate failed: " + "; ".join(self.reasons)
        )


def compare_cases(
    baseline: Mapping[str, tuple[TrialEvidence, ...]],
    candidate: Mapping[str, tuple[TrialEvidence, ...]],
    *,
    custom_cases: Mapping[str, ApprovedCase] | None = None,
) -> GateResult:
    reasons: list[str] = []
    counts: dict[str, dict[str, int]] = {}
    custom_cases = custom_cases or {}
    if (
        not baseline
        or baseline.keys() != candidate.keys()
        or not custom_cases.keys() <= baseline.keys()
    ):
        reasons.append("Incomplete or mismatched case set")
    for name in sorted(baseline.keys() | candidate.keys()):
        left, right = baseline.get(name, ()), candidate.get(name, ())
        custom = custom_cases.get(name)
        required = 1 if custom and custom.case.replay_mode != "fresh_call" else 3
        counter = {
            f"{side}_{label}": sum(r.verdict == verdict for r in rows)
            for side, rows in (("baseline", left), ("candidate", right))
            for label, verdict in (
                ("passes", "pass"),
                ("failures", "fail"),
                ("error", "error"),
                ("unsupported", "unsupported"),
            )
        }
        counts[name] = counter
        for side, rows in (("baseline", left), ("candidate", right)):
            if (
                len(rows) != required
                or len({r.id for r in rows}) != required
                or {r.repeat_index for r in rows} != set(range(required))
                or any(
                    r.side != side
                    or r.case_id != name
                    or r.verdict not in {"pass", "fail"}
                    for r in rows
                )
            ):
                reasons.append(f"{name}: incomplete or invalid {side} evidence")
            if (
                len({r.fingerprint for r in rows}) > 1
                or len({r.source_digest for r in rows}) > 1
            ):
                reasons.append(f"{name}: inconsistent {side} identity")
        if custom:
            if (
                counter["baseline_passes"] < 1
                or counter["candidate_passes"] != required
            ):
                reasons.append(
                    f"{name}: incident reproduction or candidate assertions failed"
                )
        elif counter["candidate_passes"] < counter["baseline_passes"]:
            reasons.append(f"{name}: observed pass count regressed")
    return GateResult(not reasons, tuple(reasons), counts)


def fixture_manifest(root: Path) -> dict[str, Any]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Fixture must be a real directory")
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Fixture symlinks are forbidden")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError("Fixture contains a special file")
        relative = path.relative_to(root).as_posix()
        raw = _read(root, relative)
        if raw is None:
            raise ValueError("Fixture changed during capture")
        files[relative] = raw.decode("utf-8")
    return {"schema_version": 1, "files": files}


async def private_artifact(artifacts: Any, task_id: UUID, raw: bytes) -> UUID:
    async def stream() -> AsyncIterator[bytes]:
        yield raw

    artifact = await artifacts.put(task_id, "application/json", "private", stream())
    return cast(UUID, artifact.id)


class TrialService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        worker: Any,
        *,
        source_root: Path,
        fixtures: Mapping[str, Path],
        artifacts: Any,
        clock: Any,
        invocation_id: UUID,
        candidate_generation: int = 0,
        credential_factory: Any = None,
        repair_id: UUID | None = None,
    ) -> None:
        if candidate_generation < 0:
            raise ValueError("Invalid candidate generation")
        self.repair_id = repair_id
        self.factory, self.worker = unit_of_work, worker
        self.credential_factory = credential_factory
        self.source_root, self.fixtures = source_root, fixtures
        self.artifacts, self.clock = artifacts, clock
        self.invocation_id, self.generation = invocation_id, candidate_generation

    async def _interrupt_running(self, side: str, fingerprints: set[str]) -> None:
        async with self.factory() as uow:
            for job in await uow.repair_trials.list_jobs(self.invocation_id):
                if (
                    job.status == "running"
                    and job.side == side
                    and job.candidate_generation == self.generation
                    and job.fingerprint in fingerprints
                ):
                    await uow.repair_trials.update_job(
                        replace(
                            job, status="interrupted", completed_at=self.clock.now()
                        )
                    )
            await uow.commit()

    async def _recover_older_candidates(self) -> None:
        async with self.factory() as uow:
            for job in await uow.repair_trials.list_jobs(self.invocation_id):
                if (
                    job.status == "running"
                    and job.side != "baseline"
                    and job.candidate_generation < self.generation
                ):
                    await uow.repair_trials.update_job(
                        replace(
                            job, status="interrupted", completed_at=self.clock.now()
                        )
                    )
            await uow.commit()

    async def run(
        self,
        case: ApprovedCase,
        source: SourceIdentity,
        side: str,
        repeat_index: int,
        fingerprint: str,
    ) -> TrialEvidence:
        await self._recover_older_candidates()
        try:
            return await self._run(case, source, side, repeat_index, fingerprint)
        except asyncio.CancelledError:
            await asyncio.shield(self._interrupt_running(side, {fingerprint}))
            raise

    async def _run(
        self,
        case: ApprovedCase,
        source: SourceIdentity,
        side: str,
        repeat_index: int,
        fingerprint: str,
    ) -> TrialEvidence:
        case = ApprovedCase.model_validate_json(case.model_dump_json())
        if side not in {
            "baseline",
            "candidate",
            "regression",
        } or not 0 <= repeat_index < (
            3 if case.case.replay_mode == "fresh_call" else 1
        ):
            raise ValueError("Invalid trial scope")
        if await capture_source_identity(self.source_root) != source:
            raise ValueError("Source identity changed")
        fixture = self.fixtures[case.case.fixture_manifest_digest]
        manifest = fixture_manifest(fixture)
        if digest(manifest) != case.case.fixture_manifest_digest:
            raise ValueError("Fixture digest changed")
        async with self.factory() as uow:
            results = await uow.repair_trials.list_results(
                fingerprint, case.case.case_id, side
            )
            for result in results:
                job = await uow.repair_trials.get_job(result.id)
                if (
                    result.repeat_index == repeat_index
                    and result.source_digest == source.tree_digest
                    and result.verdict in {"pass", "fail"}
                    and result.assertion_results
                    and job is not None
                    and job.status == "completed"
                    and job.case_digest == case.case_digest
                    and (
                        side == "baseline"
                        or (
                            job.invocation_id == self.invocation_id
                            and job.candidate_generation == self.generation
                        )
                    )
                ):
                    return result
        payload = await self._materialize(case)
        job = TrialJob(
            uuid4(),
            self.invocation_id,
            self.generation,
            case.case.case_id,
            case.case_digest,
            source.tree_digest,
            fingerprint,
            side,
            repeat_index,
            "running",
            self.clock.now(),
            repair_id=self.repair_id,
        )
        async with self.factory() as uow:
            await uow.repair_trials.add_job(job)
            await uow.commit()
        worker_job = WorkerJob(
            job_id=job.id,
            case_id=job.case_id,
            repeat_index=repeat_index,
            purpose=cast(Literal["baseline", "candidate", "regression"], side),
            source_digest=source.tree_digest,
            input_manifest=payload,
            settings=payload["settings"],
            deadline_seconds=600,
        )
        try:
            credential = (
                await self.credential_factory(worker_job)
                if self.credential_factory and case.case.replay_mode == "fresh_call"
                else None
            )
            execution = await self.worker.run(
                worker_job, self.source_root, fixture, credential
            )
        except asyncio.CancelledError:
            async with self.factory() as uow:
                await uow.repair_trials.update_job(
                    replace(job, status="interrupted", completed_at=self.clock.now())
                )
                await uow.commit()
            raise
        except Exception:
            execution = WorkerExecution(
                None, fixture, "backend_error", TrustedUsage(0, 0, 0, "reported"), 0
            )
        observation_id = None
        assertions: tuple[dict[str, Any], ...] = ()
        verdict = "error"
        observed = execution.observation
        try:
            source_stable = await capture_source_identity(self.source_root) == source
            fixture_stable = (
                digest(fixture_manifest(fixture)) == case.case.fixture_manifest_digest
            )
        except (OSError, ValueError):
            source_stable = fixture_stable = False
        if observed is not None:
            raw = canonical_json(observed.model_dump(mode="json"))
            try:
                observed = read_observation(raw, worker_job)
            except (ValueError, TypeError):
                observed = None
            observation_id = await private_artifact(
                self.artifacts, case.case.incident_ids["task"], raw
            )
        if (
            execution.termination == "completed"
            and source_stable
            and fixture_stable
            and observed is not None
        ):
            if observed.error_code in {
                "worker_execution_error",
                "deadline_exceeded",
                "credential_output_redacted",
            }:
                verdict = "error"
            elif observed.status == "unsupported":
                verdict = "unsupported"
            else:
                verification_input = observed.model_dump(mode="json")
                if verification_input["detail"]:
                    verification_input["detail"] = verification_input["detail"].replace(
                        "/workspace/", "{workspace}/"
                    )
                assertions = tuple(
                    asdict(a)
                    for a in verify_observation(
                        case.case,
                        verification_input,
                        execution.initial_workspace_snapshot or fixture,
                        execution.workspace_snapshot,
                        side,
                    )
                )
                verdict = (
                    "pass"
                    if assertions and all(a["passed"] for a in assertions)
                    else "fail"
                )
        if await capture_source_identity(self.source_root) != source:
            verdict = "error"
        result = TrialEvidence(
            job.id,
            fingerprint,
            job.case_id,
            repeat_index,
            side,
            source.tree_digest,
            observation_id,
            verdict,
            assertions,
            asdict(execution.usage),
            self.clock.now(),
        )
        async with self.factory() as uow:
            await uow.repair_trials.add_result(result, execution.termination)
            await uow.repair_trials.update_job(
                replace(
                    job,
                    status="completed",
                    completed_at=result.created_at,
                    observation_artifact_id=observation_id,
                )
            )
            await uow.commit()
        return result

    async def _materialize(self, approved: ApprovedCase) -> dict[str, Any]:
        """Resolve sealed task-owned captures, including original domain DTOs."""
        from pydantic import TypeAdapter

        from crucible.application.errors import ArtifactNotFound
        from crucible.domain.conversation import Message
        from crucible.domain.tools import ToolCall, ToolResult

        case = approved.case
        payload = case.model_dump(mode="json")
        if case.kind == "tool":
            return payload

        async def captured(artifact_id: UUID) -> Any:
            metadata, raw = await self.artifacts.read(artifact_id)
            if (
                metadata.task_id != case.incident_ids["task"]
                or metadata.sensitivity != "private"
                or hashlib.sha256(raw).hexdigest() != metadata.content_hash
                or len(raw) != metadata.byte_length
            ):
                raise ValueError(
                    "Captured incident artifact integrity/ownership failure"
                )
            value = json.loads(raw)
            if isinstance(value, dict) and value.get("complete") is False:
                raise ValueError("Incomplete incident capture")
            return value

        try:
            captures = [
                await captured(identifier) for identifier in case.source_artifact_ids
            ]
            if case.kind == "compaction":
                captures.append(await captured(case.request_artifact_id))
                if case.raw_response_artifact_id is not None:
                    response = await captured(case.raw_response_artifact_id)
                    if isinstance(response, dict) and isinstance(
                        response.get("raw_text"), str
                    ):
                        payload["raw_response"] = response["raw_text"]
        except ArtifactNotFound:
            # The real driver emits unsupported for missing context inputs.
            return payload
        messages = []
        for value in captures:
            if not isinstance(value, dict):
                continue
            context = value.get("context", {})
            if isinstance(value.get("messages"), list) and all(
                isinstance(m, dict) and "conversation_sequence" in m
                for m in value["messages"]
            ):
                messages = list(
                    TypeAdapter(tuple[Message, ...]).validate_python(value["messages"])
                )
            if isinstance(context, dict):
                if isinstance(context.get("source_units"), list):
                    raw_messages = [
                        message
                        for unit in context["source_units"]
                        for message in unit["messages"]
                    ]
                    messages = list(
                        TypeAdapter(tuple[Message, ...]).validate_python(raw_messages)
                    )
                if isinstance(context.get("message_ids"), list):
                    async with self.factory() as uow:
                        messages = []
                        for identity in context["message_ids"]:
                            message = await uow.messages.get(UUID(identity))
                            if message is None:
                                raise ValueError(
                                    "Captured source message is unavailable"
                                )
                            messages.append(message)
        if messages:
            if any(
                message.task_id != case.incident_ids["task"] for message in messages
            ):
                raise ValueError("Captured source messages belong to another Task")
            calls: list[ToolCall] = []
            results: list[ToolResult] = []
            call_ids = {
                part.tool_call_id
                for message in messages
                for part in message.parts
                if part.tool_call_id
            }
            result_ids = {
                part.tool_result_id
                for message in messages
                for part in message.parts
                if part.tool_result_id
            }
            async with self.factory() as uow:
                for run_id in {
                    message.run_id for message in messages if message.run_id is not None
                }:
                    run = await uow.runs.get(run_id)
                    if run is None or run.task_id != case.incident_ids["task"]:
                        raise ValueError("Captured source Run is unavailable")
                    for step in await uow.steps.list_for_run(run_id):
                        calls.extend(
                            call
                            for call in await uow.tool_calls.list_for_step(step.id)
                            if call.id in call_ids
                        )
                        results.extend(
                            result
                            for result in await uow.tool_results.list_for_step(step.id)
                            if result.id in result_ids
                        )
            if {call.id for call in calls} != call_ids or {
                result.id for result in results
            } != result_ids:
                raise ValueError("Captured tool exchange is incomplete")
            payload.update(
                json.loads(
                    canonical_json(
                        {"messages": messages, "calls": calls, "results": results}
                    )
                )
            )
        if case.prior_compaction_artifact_id is not None:
            prior = await captured(case.prior_compaction_artifact_id)
            if (
                not isinstance(prior, dict)
                or "prior_compaction" not in prior
                or not isinstance(prior.get("prior_summary"), str)
            ):
                raise ValueError("Prior compaction capture is incomplete")
            payload["prior_compaction"], payload["prior_summary"] = (
                prior["prior_compaction"],
                prior["prior_summary"],
            )
        return payload

    async def run_repository_suite(
        self,
        suite: Any,
        source: SourceIdentity,
        side: str,
        fingerprints: Mapping[str, str],
        *,
        fixtures: Any,
        evaluators: Any,
        task_id: UUID,
        run_id: UUID,
    ) -> dict[str, tuple[TrialEvidence, ...]]:
        await self._recover_older_candidates()
        try:
            return await self._run_repository_suite(
                suite,
                source,
                side,
                fingerprints,
                fixtures=fixtures,
                evaluators=evaluators,
                task_id=task_id,
                run_id=run_id,
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                self._interrupt_running(side, set(fingerprints.values()))
            )
            raise

    async def _run_repository_suite(
        self,
        suite: Any,
        source: SourceIdentity,
        side: str,
        fingerprints: Mapping[str, str],
        *,
        fixtures: Any,
        evaluators: Any,
        task_id: UUID,
        run_id: UUID,
    ) -> dict[str, tuple[TrialEvidence, ...]]:
        """Three isolated trials per fixed case; hidden definitions stay on host.

        FixturePreparer and EvaluatorRegistry are the existing protected eval
        implementations. Only prompt/settings enter the candidate worker.
        """
        if side not in {"baseline", "candidate", "regression"}:
            raise ValueError("Invalid trial side")
        if self.credential_factory is None:
            raise ValueError("Repository evaluation requires a scoped model gateway")
        results = {}
        for case in suite.cases:
            rows = []
            for repeat in range(3):
                if await capture_source_identity(self.source_root) != source:
                    raise ValueError("Source identity changed during suite")
                fingerprint = fingerprints[case.case_id]
                cached = None
                async with self.factory() as uow:
                    for result in await uow.repair_trials.list_results(
                        fingerprint, case.case_id, side
                    ):
                        prior = await uow.repair_trials.get_job(result.id)
                        if (
                            prior
                            and prior.status == "completed"
                            and prior.case_digest == case.case_digest
                            and result.source_digest == source.tree_digest
                            and result.repeat_index == repeat
                            and result.verdict in {"pass", "fail"}
                            and result.assertion_results
                            and (
                                side == "baseline"
                                or (
                                    prior.invocation_id == self.invocation_id
                                    and prior.candidate_generation == self.generation
                                )
                            )
                        ):
                            cached = result
                            break
                if cached is not None:
                    rows.append(cached)
                    continue
                job = TrialJob(
                    uuid4(),
                    self.invocation_id,
                    self.generation,
                    case.case_id,
                    case.case_digest,
                    source.tree_digest,
                    fingerprint,
                    side,
                    repeat,
                    "running",
                    self.clock.now(),
                    repair_id=self.repair_id,
                )
                async with self.factory() as uow:
                    await uow.repair_trials.add_job(job)
                    await uow.commit()
                manifest = {
                    "kind": "repository",
                    "prompt": case.prompt,
                    "repository_settings": asdict(case.repository_settings),
                    "budgets": asdict(case.budgets),
                }
                worker_job = WorkerJob(
                    job_id=job.id,
                    case_id=case.case_id,
                    repeat_index=repeat,
                    purpose=cast(Literal["baseline", "candidate", "regression"], side),
                    source_digest=source.tree_digest,
                    input_manifest=json.loads(canonical_json(manifest)),
                    settings={"model": case.model},
                    deadline_seconds=min(600, case.budgets.max_active_seconds),
                )
                try:
                    prepared = await asyncio.to_thread(fixtures.prepare, case, job.id)
                    credential = await self.credential_factory(worker_job)
                    execution = await self.worker.run(
                        worker_job, self.source_root, prepared.root, credential
                    )
                except asyncio.CancelledError:
                    async with self.factory() as uow:
                        await uow.repair_trials.update_job(
                            replace(
                                job, status="interrupted", completed_at=self.clock.now()
                            )
                        )
                        await uow.commit()
                    raise
                except Exception:
                    execution = WorkerExecution(
                        None,
                        self.source_root,
                        "backend_error",
                        TrustedUsage(0, 0, 0, "reported"),
                        0,
                    )
                observed = execution.observation
                artifact_id = None
                assertions = []
                verdict = "error"
                if observed is not None:
                    raw = canonical_json(observed.model_dump(mode="json"))
                    observed = read_observation(raw, worker_job)
                    artifact_id = await private_artifact(self.artifacts, task_id, raw)
                if (
                    execution.termination == "completed"
                    and observed is not None
                    and await capture_source_identity(self.source_root) == source
                ):
                    if observed.status == "unsupported":
                        verdict = "unsupported"
                    elif observed.status == "completed":
                        from crucible.evals.evaluators import EvaluationContext

                        for definition in case.evaluators:
                            outcome = await evaluators.evaluate(
                                EvaluationContext(
                                    task_id,
                                    run_id,
                                    execution.workspace_snapshot,
                                    (),
                                    job.id,
                                ),
                                definition,
                            )
                            assertions.append(
                                {
                                    "kind": definition.kind,
                                    "passed": outcome.verdict.value == "passed",
                                    "verdict": outcome.verdict.value,
                                    "code": outcome.code,
                                    "evidence_artifact_id": str(
                                        outcome.evidence_artifact_id
                                    )
                                    if outcome.evidence_artifact_id
                                    else None,
                                }
                            )
                        verdict = (
                            "error"
                            if not assertions
                            or any(a["verdict"] == "error" for a in assertions)
                            else "pass"
                            if all(a["passed"] for a in assertions)
                            else "fail"
                        )
                    elif observed.error_code == "repository_run_failed":
                        provenance = observed.tool_result or {}
                        # Only an authoritative validation failure is established
                        # behavior. Provider and ambiguous outcomes stay error.
                        if (
                            provenance.get("run_status") == "failed"
                            and provenance.get("run_outcome_code")
                            == "validation_failed"
                        ):
                            verdict = "fail"
                            assertions.append(
                                {
                                    "op": "repository_validation",
                                    "passed": False,
                                    "run_outcome_code": "validation_failed",
                                }
                            )
                if await capture_source_identity(self.source_root) != source:
                    verdict = "error"
                result = TrialEvidence(
                    job.id,
                    fingerprint,
                    case.case_id,
                    repeat,
                    side,
                    source.tree_digest,
                    artifact_id,
                    verdict,
                    tuple(assertions),
                    asdict(execution.usage),
                    self.clock.now(),
                )
                async with self.factory() as uow:
                    await uow.repair_trials.add_result(result, execution.termination)
                    await uow.repair_trials.update_job(
                        replace(
                            job,
                            status="completed",
                            completed_at=result.created_at,
                            observation_artifact_id=artifact_id,
                        )
                    )
                    await uow.commit()
                rows.append(result)
                if verdict in {"error", "unsupported"}:
                    break
            results[case.case_id] = tuple(rows)
            if any(row.verdict in {"error", "unsupported"} for row in rows):
                break
        return results


if TYPE_CHECKING:
    from crucible.application.ports import UnitOfWork
