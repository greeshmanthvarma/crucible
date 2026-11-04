"""Sequential diagnostic execution through real packages, ledger and validation."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID

import pytest

from crucible.improvement.contracts import EDITABLE_PATHS, MiningDiagnosticManifest
from crucible.improvement.mining_contracts import MiningDiagnosticConfig
from crucible.improvement.mining_diagnostic import freeze_mining_diagnostic
from crucible.improvement.source import _digest, inspect_source
from crucible.improvement.trajectory_contracts import (
    ExportLimits,
    TrialSelection,
    VerifiedDevelopmentInputs,
)
from crucible.improvement.trajectory_export import export_trajectory_package
from tests.contract.test_mining_diagnostic import diagnostic_payload
from tests.unit import test_improvement_source as source_fixtures
from tests.unit import test_trajectory as trace_fixtures

harness = source_fixtures.harness
parallel_records = trace_fixtures.parallel_records
parallel_trace = trace_fixtures.parallel_trace
verified_trial = trace_fixtures.verified_trial


@pytest.fixture
def diagnostic_config(harness, verified_trial, tmp_path, request):
    repo, commit = harness
    files = {"backend/src/crucible/tools/filesystem.py": (0o100644, b"# historical\n")}
    scale = getattr(request, "param", 0)
    for n in range(scale):
        files[f"backend/src/crucible/tools/module_{n}.py"] = (
            0o100644,
            f"# historical module {n}\n".encode(),
        )
    source = verified_trial.source.model_copy(update={"export_digest": _digest(files)})
    trials = []
    for i in range(3):
        trial_id, task_id, run_id = (
            UUID(int=40 + i),
            UUID(int=100 + i),
            UUID(int=200 + i),
        )
        trace = json.loads(verified_trial.trace_bytes)
        for n in range(scale):
            message = json.loads(json.dumps(trace["messages"][0]))
            message.update(id=str(UUID(int=1000 + n)), conversation_sequence=4 + n)
            message["parts"][0].update(id=str(UUID(int=5000 + n)))
            trace["messages"].append(message)
        for collection in ("messages", "tool_calls", "tool_results"):
            for record in trace[collection]:
                record.update(task_id=str(task_id), run_id=str(run_id))
        raw = json.dumps(trace).encode()
        trials.append(
            replace(
                verified_trial,
                trial_id=trial_id,
                task_id=task_id,
                run_id=run_id,
                repeat_index=i,
                source=source,
                trace_bytes=raw,
                trace_ref=verified_trial.trace_ref.model_copy(
                    update={
                        "trial_id": trial_id,
                        "sha256": hashlib.sha256(raw).hexdigest(),
                    }
                ),
                solution_ref=verified_trial.solution_ref.model_copy(
                    update={"trial_id": trial_id}
                ),
            )
        )
    trials = tuple(trials)
    ids = tuple(t.trial_id for t in trials)
    package = export_trajectory_package(
        VerifiedDevelopmentInputs(
            trials,
            source,
            files,
            ids,
            UUID(int=800),
            UUID(int=801),
            TrialSelection(mode="explicit-three-repeat", trial_ids=ids),
        ),
        tmp_path / "package",
        ExportLimits(),
    )
    inspection = inspect_source(repo, commit)
    payload = diagnostic_payload()
    if scale:
        payload["model"]["output_reserve"] = 4096
        payload["model"]["input_limit"] = 128000
    payload.update(
        initial_commit=commit,
        initial_source_digest=inspection.source_digest,
        controller_digest=_digest(
            {n: v for n, v in inspection.files.items() if n not in EDITABLE_PATHS}
        ),
        dependency_lock_digest=hashlib.sha256(
            (repo / "backend/uv.lock").read_bytes()
        ).hexdigest(),
        package_digest=package.bundle_digest,
        investigation_source=source.model_dump(mode="json"),
        selected_trial_ids=[str(i) for i in ids],
    )
    from crucible.improvement.mining_diagnostic import diagnostic_policy_digests

    payload.update(diagnostic_policy_digests(package))
    return MiningDiagnosticConfig(
        schema_version=1,
        repository=repo,
        package_dir=tmp_path / "package",
        manifest=MiningDiagnosticManifest.model_validate_json(json.dumps(payload)),
    )


def test_preparation_handles_distinct_runtime_tasks_without_live_provider(
    diagnostic_config, tmp_path, monkeypatch
):
    from crucible.improvement.worker_supervisor import DockerRuntime

    def forbidden(*args, **kwargs):
        raise AssertionError("offline preparation accessed Docker")

    monkeypatch.setattr(DockerRuntime, "invoke", forbidden)
    from crucible.repair.gateway import ScopedModelGateway

    monkeypatch.setattr(ScopedModelGateway, "__init__", forbidden)
    result = freeze_mining_diagnostic(diagnostic_config, tmp_path / "diagnostic")
    assert result.package_digest == diagnostic_config.manifest.package_digest
    overviews = [
        json.loads(path.read_bytes())
        for path in (result.root / "frozen/package/trials").glob("*/overview.json")
    ]
    assert len({o["task_id"] for o in overviews}) == 3
    assert len({o["run_id"] for o in overviews}) == 3
    assert {o["case_id"] for o in overviews} == {"development"}
    assert (result.root / "frozen/runtime/harness/backend/uv.lock").exists()
    assert not (result.root / "frozen/runtime/harness/tests").exists()
    assert (
        result.root / "frozen/package/source/backend/src/crucible/tools/filesystem.py"
    ).read_bytes() == b"# historical\n"
    assert (
        result.executing_source_digest
        != diagnostic_config.manifest.investigation_source.source_digest
    )


@pytest.fixture
async def prepared_diagnostic(diagnostic_config, tmp_path):
    from crucible.improvement.mining_store import MiningDiagnosticStore

    prepared = freeze_mining_diagnostic(diagnostic_config, tmp_path / "diagnostic")
    store = await MiningDiagnosticStore.open(prepared.root / "accounting.sqlite3")
    try:
        yield prepared, store
    finally:
        await store.close()


@pytest.mark.parametrize("diagnostic_config", [180], indirect=True)
async def test_catalog_lookup_read_and_report_fit_real_cumulative_budget(
    prepared_diagnostic,
):
    from uuid import uuid4

    from pydantic import TypeAdapter

    from crucible.domain.clock import SystemClock
    from crucible.engine.gateway import (
        CompleteToolCall,
        ModelMessage,
        ModelPart,
        ModelRole,
        ModelStop,
        ModelStopReason,
        ModelUsage,
        PreparedModelRequest,
        TextDelta,
    )
    from crucible.improvement.broker import ImprovementBroker, WorkspaceBinding
    from crucible.improvement.mining_contracts import AnalystAssignment
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner
    from crucible.improvement.mining_inspection import audited_role_registry
    from crucible.improvement.mining_validation import (
        load_inspection_package,
        validate_analysis_report,
    )
    from crucible.improvement.role_prompts import role_system_prompt
    from crucible.improvement.role_worker import role_input_prompt
    from crucible.models.token_counting import estimate_input_tokens
    from crucible.repair.gateway import ScopedModelGateway
    from crucible.repair.identity import canonical_json
    from crucible.tools.definitions import ToolContext
    from tests.unit.test_mining_store import make_role

    prepared, store = prepared_diagnostic
    runner = MiningDiagnosticRunner(store, prepared.root, None)
    status = await runner.initialize(prepared.diagnostic_id)
    manifest = (await store.get(prepared.diagnostic_id)).manifest
    assignments = TypeAdapter(tuple[AnalystAssignment, ...]).validate_json(
        (prepared.root / "frozen/assignments.json").read_bytes(), strict=True
    )
    directory = prepared.root / "budget-proof"
    directory.mkdir()
    workspace, encoded = await runner._workspace(
        directory, status.slots[0], manifest, assignments, None, status.slots[:3]
    )
    inputs = json.loads(encoded)
    assert len(encoded) < 16384
    assert isinstance(inputs["record_registry"], str)
    assert isinstance(inputs["file_hashes"], str)
    records = [
        json.loads(line)
        for line in (workspace / inputs["record_registry"]).read_text().splitlines()
    ]
    assert len(records) > 180
    assert {entry["trial_id"] for entry in records} == {str(assignments[0].trial_id)}
    from crucible.models.token_counting import _encoding

    # Two pre-fix requests exceed the cumulative slot even before framing/tools.
    assert len(_encoding().encode_ordinary(json.dumps(records))) > 25000
    entry = next(entry for entry in records if entry["kind"] == "message")
    source_path = "backend/src/crucible/tools/filesystem.py"
    receipts = []
    job = make_role(
        manifest, status.slots[0], input_digest=hashlib.sha256(encoded).hexdigest()
    )
    await store.register_attempt(status.slots[0].slot_id, job, job.input_digest)
    readers = audited_role_registry("trial_analysis", receipts, job.job_id)
    tools = readers.definitions
    messages = [
        ModelMessage(
            ModelRole.SYSTEM, (ModelPart("text", role_system_prompt("trial_analysis")),)
        ),
        ModelMessage(ModelRole.USER, (ModelPart("text", role_input_prompt(encoded)),)),
    ]
    detail = "trial/" + entry["detail_ref"].split("/", 2)[2]
    operations = [
        ("read_file_chunk", dict(path=inputs["index"], limit=1024)),
        (
            "search_files",
            dict(path=inputs["record_registry"], query=entry["original_record_id"]),
        ),
        (
            "search_files",
            dict(path=inputs["file_hashes"], query="source/" + source_path),
        ),
        ("read_file_chunk", dict(path=detail, limit=256)),
        ("read_file", dict(path="source/" + source_path, start_line=1, end_line=1)),
    ]
    report = dict(
        schema_version=1,
        trial_id=str(assignments[0].trial_id),
        package_digest=manifest.package_digest,
        investigation_source_digest=manifest.investigation_source.source_digest,
        investigation_complete=True,
        summary="Read indexed evidence; no causal claim.",
        observations=[
            dict(
                id="o1",
                observed_behavior="The recorded message is present.",
                evidence=[
                    dict(
                        original=entry["original_artifact"],
                        record_kind="message",
                        record_id=entry["original_record_id"],
                        record_key=entry["detail_ref"],
                        derived_sha256=entry["detail_sha256"],
                        start_byte=0,
                        end_byte=4,
                    )
                ],
                hypothesized_mechanism=None,
                alternatives=[],
                contradictory_evidence=[],
                source=[
                    dict(
                        path=source_path,
                        sha256=hashlib.sha256(
                            (workspace / "source" / source_path).read_bytes()
                        ).hexdigest(),
                        start_line=1,
                        end_line=1,
                    )
                ],
                limitations="Byte inspection is not causal proof.",
            )
        ],
        open_questions=[],
        limitations=[],
    )
    raw_report = canonical_json(report)

    class Upstream:
        calls = 0
        operation = None
        call_id = None

        async def stream(self, request):
            self.calls += 1
            # Charge the real conservative count, not a tiny scripted usage.
            yield ModelUsage(
                estimate_input_tokens(request.messages, request.tools), 512
            )
            if self.operation is not None:
                name, arguments = self.operation
                yield CompleteToolCall(self.call_id, name, arguments)
                yield ModelStop(ModelStopReason.TOOL_CALLS)
            else:
                yield TextDelta(raw_report.decode())
                yield ModelStop(ModelStopReason.COMPLETE)

    upstream = Upstream()
    gateway = ScopedModelGateway(
        upstream, clock=SystemClock(), endpoint="http://broker"
    )
    broker = ImprovementBroker(store, gateway, None)
    credential = broker.issue(
        job,
        manifest,
        WorkspaceBinding(workspace, manifest.worker_image, manifest.worker_platform),
    )
    estimates = []
    for operation in [*operations, None]:
        upstream.operation, upstream.call_id = operation, uuid4()
        request = PreparedModelRequest(
            uuid4(),
            uuid4(),
            manifest.model.model_id,
            tuple(messages),
            tools,
            manifest.model.output_reserve,
        )
        estimates.append(estimate_input_tokens(request.messages, request.tools))
        events = [
            event async for event in broker.model(credential.token, job.job_id, request)
        ]
        assert isinstance(events[-1], ModelStop), events
        if operation is not None:
            name, arguments = operation
            result = await readers.get(name).invoke(
                ToolContext(workspace, tool_call_id=upstream.call_id), arguments
            )
            assert result.error_code is None
            if name == "search_files":
                assert arguments["query"] in result.display_text
                assert len(result.display_text.encode()) <= 8192
            messages.extend(
                (
                    ModelMessage(
                        ModelRole.ASSISTANT,
                        (
                            ModelPart(
                                "tool_call",
                                tool_call_id=upstream.call_id,
                                tool_name=name,
                                arguments=arguments,
                            ),
                        ),
                    ),
                    ModelMessage(
                        ModelRole.TOOL,
                        (
                            ModelPart(
                                "tool_result",
                                canonical_json(result.data).decode(),
                                tool_call_id=upstream.call_id,
                            ),
                        ),
                    ),
                )
            )
    package = load_inspection_package(
        prepared.root / "frozen/package",
        job_id=job.job_id,
        workspace_files={
            p.relative_to(workspace).as_posix(): p.read_bytes()
            for p in workspace.rglob("*")
            if p.is_file() and ".git" not in p.parts
        },
    )
    returned_report = "".join(
        event.text for event in events if isinstance(event, TextDelta)
    ).encode()
    assert returned_report == raw_report
    assert validate_analysis_report(
        returned_report, assignments[0], package, tuple(receipts)
    ).investigation_complete
    assert upstream.calls == 6
    usage = await store.slot_usage(status.slots[0].slot_id)
    assert usage["spent"].tokens == sum(estimates) + 6 * 512
    assert usage["remaining"].tokens > 4096
    assert not await store.unsettled_usage(prepared.diagnostic_id)


class ScriptedSupervisor:
    """Replaces only the provider/container boundary, preserving artifact gates."""

    def __init__(self, store, root, failure=None, faults=None):
        self.store, self.root, self.failure = store, root, failure
        self.jobs, self.workspaces, self.stopped = [], [], []
        self.live = False
        self.faults = faults or {}
        self.valid_reports = {}

    async def execute(self, job, snapshot, workspace, *, harness_export):
        from crucible.improvement.contracts import UsageAmounts
        from crucible.improvement.worker_protocol import (
            IsolatedRoleExecution,
            JobOutcome,
            RoleObservation,
        )
        from crucible.repair.gateway import TrustedUsage
        from crucible.repair.identity import canonical_json

        assert not self.live
        self.live = True
        self.jobs.append(job)
        self.workspaces.append(
            {
                p.relative_to(workspace).as_posix()
                for p in workspace.rglob("*")
                if p.is_file()
            }
        )
        self.store.take_job_admission(
            job, (await self.store.get(job.experiment_id)).manifest
        )
        fault = self.faults.get(len(self.jobs) - 1)
        assert (self.root / "attempts" / str(job.job_id) / "job.json").exists()
        assert (
            hashlib.sha256((workspace / "input.json").read_bytes()).hexdigest()
            == job.input_digest
        )
        if fault != "admission_quota":
            reservation = await self.store.reserve_usage(
                job.experiment_id,
                None,
                UsageAmounts(tokens=100, requests=1),
                job_id=job.job_id,
            )
            await self.store.settle_usage(
                reservation, UsageAmounts(tokens=100, requests=1)
            )
        inputs = json.loads((workspace / "input.json").read_bytes())
        if job.job_kind != "format_correction":
            records = [
                json.loads(line)
                for line in (workspace / inputs["record_registry"])
                .read_text()
                .splitlines()
            ]
            hashes = {
                row["path"]: row["sha256"]
                for row in (
                    json.loads(line)
                    for line in (workspace / inputs["file_hashes"])
                    .read_text()
                    .splitlines()
                )
            }
        if job.job_kind == "trial_analysis":
            assert (workspace / inputs["index"]).exists()
            assert {r["trial_id"] for r in records} == {
                inputs["assignment"]["trial_id"]
            }
            assert all(not p.startswith("trials/") for p in hashes)

        termination = (
            "budget_exhausted" if self.failure == len(self.jobs) - 1 else "completed"
        )
        if job.job_kind == "format_correction":
            report = self.valid_reports[job.budget_slot_id]
        elif job.job_kind == "trial_analysis":
            report = dict(
                schema_version=1,
                trial_id=inputs["assignment"]["trial_id"],
                package_digest=inputs["assignment"]["package_digest"],
                investigation_source_digest=inputs["assignment"][
                    "investigation_source_digest"
                ],
                investigation_complete=True,
                summary="No supported observations",
                observations=[],
                open_questions=[],
                limitations=[],
            )
        else:
            report = dict(
                schema_version=1,
                package_digest=inputs["assignment"]["package_digest"],
                result=dict(
                    schema_version=1,
                    disposition="no_actionable_weakness",
                    weaknesses=[],
                    summary="No supported finding",
                ),
                support=[],
                limitations=[],
            )
        fault = self.faults.get(len(self.jobs) - 1)
        receipts = []
        if fault in ("actionable", "actionable_invalid", "uninspected"):
            from uuid import uuid4

            from crucible.improvement.mining_inspection import audited_role_registry
            from crucible.tools.definitions import ToolContext

            entry = next(r for r in records if r["kind"] == "message")
            source_path = "backend/src/crucible/tools/filesystem.py"
            evidence = dict(
                original=entry["original_artifact"],
                record_kind="message",
                record_id=entry["original_record_id"],
                record_key=entry["detail_ref"],
                derived_sha256=entry["detail_sha256"],
                start_byte=0,
                end_byte=4,
            )
            source = dict(
                path=source_path,
                sha256=hashes["source/" + source_path],
                start_line=1,
                end_line=1,
            )
            weakness = dict(
                id="w1",
                observed_behavior="Recorded behavior",
                hypothesized_mechanism="Hypothesis",
                evidence=[entry["original_artifact"]],
                editable_paths=[source_path],
                confidence="low",
                risks=[],
                limitations="Not causal proof",
            )
            report["result"].update(disposition="actionable", weaknesses=[weakness])
            report["support"] = [
                dict(weakness_id="w1", evidence=[evidence], source=[source])
            ]
            if fault != "uninspected":
                registry = audited_role_registry(
                    "mining_synthesis", receipts, job.job_id
                )
                for name in (
                    inputs["record_registry"],
                    entry["detail_ref"],
                    "source/" + source_path,
                ):
                    await registry.get("read_file_chunk").invoke(
                        ToolContext(workspace, tool_call_id=uuid4()),
                        dict(path=name, offset=0, limit=64),
                    )
        self.valid_reports[job.budget_slot_id] = report.copy()
        if fault == "actionable_invalid":
            report = {k: v for k, v in report.items() if k != "schema_version"}

        if fault == "invalid":
            report = {"bogus": "untrusted raw error text"}
        elif fault == "raised":
            raise RuntimeError("scripted infrastructure failure")
        elif fault == "incomplete":
            report["investigation_complete"] = False
        elif fault == "drift":
            (workspace / "input.json").write_bytes(b"changed")
        elif fault == "frozen_drift":
            (
                self.root
                / "frozen/package/source/backend/src/crucible/tools/filesystem.py"
            ).write_bytes(b"changed")
        elif fault == "exhaust_invalid":
            report = {"bogus": True}
            remaining = (await self.store.slot_usage(job.budget_slot_id))["remaining"]
            extra = await self.store.reserve_usage(
                job.experiment_id,
                None,
                UsageAmounts(tokens=remaining.tokens),
                job_id=job.job_id,
            )
            await self.store.settle_usage(extra, UsageAmounts(tokens=remaining.tokens))
        state = self.root / "jobs" / str(job.job_id) / "state"
        state.mkdir(parents=True)
        (state / "assistant.txt").write_bytes(canonical_json(report))
        (state / "trace.json").write_bytes(b"{}")
        (state / "inspection-receipts.json").write_bytes(canonical_json(receipts))
        if fault == "oversized":
            (state / "assistant.txt").write_bytes(b"x" * 1_048_577)
        observation = RoleObservation(
            schema_version=2,
            job_id=job.job_id,
            role_id=job.role_id,
            round_id=job.round_id,
            experiment_id=job.experiment_id,
            export_digest=job.export_digest,
            termination=termination,
            assistant_output_ref="assistant.txt",
            trace_ref="trace.json",
            detail_code=None,
            inspection_receipts_ref="inspection-receipts.json",
            inspection_receipts_sha256=hashlib.sha256(
                canonical_json(receipts)
            ).hexdigest(),
            tool_call_count=len(receipts),
        )
        if fault == "foreign_observation":
            observation = observation.model_copy(update={"job_id": UUID(int=999)})
        (state / "observation.json").write_text(observation.model_dump_json())
        pending_artifacts = tuple(
            UUID(row["reservation_id"])
            for row in await self.store.usage_records(job.experiment_id)
            if row["status"] == "reserved"
        )
        outcome = JobOutcome(
            job.job_id,
            job.experiment_id,
            job.role_id,
            job.harness_digest,
            None,
            termination,
            "observation",
            None,
            True,
            None,
            "host_measured",
            "continue",
            UsageAmounts(),
            TrustedUsage(),
            pending_artifacts,
            None,
        )
        if fault == "unknown_observation":
            observation = None
        elif fault == "cleanup":
            outcome = replace(outcome, cleanup_confirmed=False)
        elif fault == "interrupted":
            from crucible.improvement.worker_protocol import JobFailure

            outcome = replace(
                outcome,
                termination="interrupted",
                failure=JobFailure("cancellation", "controller"),
            )
        elif fault == "admission_quota":
            from crucible.improvement.worker_protocol import JobFailure

            outcome = replace(
                outcome,
                termination="failed",
                stage="admission",
                failure=JobFailure("quota", "controller", "not_sent"),
            )
            observation = None
        return IsolatedRoleExecution(
            observation,
            workspace,
            state,
            TrustedUsage(),
            (),
            fault != "cleanup",
            0,
            outcome,
        )

    async def stop(self, job_id):
        self.stopped.append(job_id)
        self.live = False


async def test_one_failed_analyst_prevents_synthesis(prepared_diagnostic):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, failure=1)
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "blocked"
    assert [j.job_kind for j in supervisor.jobs] == ["trial_analysis"] * 2
    assert status.final_disposition is None
    assert supervisor.stopped == [j.job_id for j in supervisor.jobs]


async def test_three_valid_analyses_then_synthesis_in_order(prepared_diagnostic):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root)
    runner = MiningDiagnosticRunner(store, prepared.root, supervisor)
    status = await runner.continue_run(prepared.diagnostic_id)
    assert status.status == "complete"
    assert status.final_disposition == "no_actionable_weakness"
    base = await store.get(prepared.diagnostic_id)
    assert (base.status, base.stage) == ("closed", "finished")
    assert [j.job_kind for j in supervisor.jobs] == ["trial_analysis"] * 3 + [
        "mining_synthesis"
    ]
    assert all(s.status == "complete" for s in status.slots)
    for ordinal, files in enumerate(supervisor.workspaces[:3]):
        assert "trial/overview.json" in files
        assert not any(p.startswith(("trials/", "reports/")) for p in files)
        assert not any("accounting" in p or "runtime/" in p for p in files)
    assert len([p for p in supervisor.workspaces[3] if p.startswith("reports/")]) == 3
    assert (
        json.loads((prepared.root / "mining-result.json").read_bytes())["weaknesses"]
        == []
    )
    usage = await store.slot_usage(status.slots[0].slot_id)
    assert usage["spent"].tokens == 100
    assert usage["spent"].output_bytes > 0
    assert usage["reserved"].output_bytes == 0
    assert (await runner.continue_run(prepared.diagnostic_id)).status == "complete"
    assert len(supervisor.jobs) == 4


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("incomplete", "incomplete"),
        ("drift", "infrastructure_error"),
        ("frozen_drift", "infrastructure_error"),
        ("foreign_observation", "infrastructure_error"),
        ("oversized", "infrastructure_error"),
    ],
)
async def test_ineligible_analysis_never_starts_synthesis(
    prepared_diagnostic, fault, expected
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={0: fault})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "blocked"
    assert status.slots[0].status == expected
    assert len(supervisor.jobs) == 1
    assert supervisor.stopped == [supervisor.jobs[0].job_id]


@pytest.mark.parametrize("second_invalid", [False, True])
async def test_only_invalid_normal_output_gets_one_correction(
    prepared_diagnostic, second_invalid
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    faults = {0: "invalid"}
    if second_invalid:
        faults[1] = "invalid"
    supervisor = ScriptedSupervisor(store, prepared.root, faults=faults)
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == ("blocked" if second_invalid else "complete")
    assert [j.job_kind for j in supervisor.jobs[:2]] == [
        "trial_analysis",
        "format_correction",
    ]
    first, correction = supervisor.jobs[:2]
    assert first.budget_slot_id == correction.budget_slot_id
    assert correction.correction_of_job_id == first.job_id
    assert correction.limits.max_model_tokens == 49900
    assert correction.limits.max_steps == 9
    assert correction.correction_target == "trial_analysis"
    assert all(
        p == "input.json" or p.startswith(".git/") for p in supervisor.workspaces[1]
    )
    if second_invalid:
        assert len(supervisor.jobs) == 2
    assert (prepared.root / "attempts" / str(first.job_id) / "assistant.txt").exists()
    assert (
        prepared.root / "attempts" / str(correction.job_id) / "assistant.txt"
    ).exists()


async def test_correction_cannot_mint_exhausted_allowance(prepared_diagnostic):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={0: "exhaust_invalid"})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "blocked"
    assert len(supervisor.jobs) == 1
    assert status.slots[0].remaining.tokens == 0


@pytest.mark.parametrize(
    "fault,expected",
    [("interrupted", "interrupted"), ("admission_quota", "budget_exhausted")],
)
async def test_real_terminal_envelopes_preserve_interruption_and_quota(
    prepared_diagnostic, fault, expected
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={0: fault})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.slots[0].status == expected
    assert len(supervisor.jobs) == 1
    assert status.slots[0].attempts[0].status == expected
    assert status.final_disposition is None


@pytest.mark.parametrize("fault", ["actionable", "actionable_invalid"])
async def test_synthesis_retains_inspected_support_across_format_correction(
    prepared_diagnostic, fault
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={3: fault})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "complete"
    report = json.loads((prepared.root / "synthesis-report.json").read_bytes())
    assert report["result"]["disposition"] == "actionable"
    assert report["support"][0]["evidence"][0]["record_key"].startswith("trials/")
    assert (
        report["support"][0]["source"][0]["path"]
        == "backend/src/crucible/tools/filesystem.py"
    )
    if fault == "actionable_invalid":
        assert supervisor.jobs[-1].job_kind == "format_correction"
        assert status.slots[3].spent.tokens == 200


async def test_synthesizer_output_without_original_inspection_is_rejected(
    prepared_diagnostic,
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={3: "uninspected"})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "blocked"
    assert [a.error_code for a in status.slots[3].attempts] == [
        "uninspected_support",
        "uninspected_support",
    ]
    assert not (prepared.root / "mining-result.json").exists()


@pytest.mark.parametrize("fault", ["raised", "cleanup", "unknown_observation"])
async def test_uncertain_execution_stops_and_retains_output_exposure(
    prepared_diagnostic, fault
):
    from crucible.improvement.mining_diagnostic import MiningDiagnosticRunner

    prepared, store = prepared_diagnostic
    supervisor = ScriptedSupervisor(store, prepared.root, faults={0: fault})
    status = await MiningDiagnosticRunner(
        store, prepared.root, supervisor
    ).continue_run(prepared.diagnostic_id)
    assert status.status == "blocked"
    assert status.slots[0].status == "infrastructure_error"
    assert status.slots[0].retained.output_bytes == 4 * 1_048_576
    assert len(supervisor.jobs) == 1
    assert supervisor.stopped == [supervisor.jobs[0].job_id]


@pytest.mark.parametrize(
    "field",
    [
        "package_digest",
        "prompt_digest",
        "initial_source_digest",
        "dependency_lock_digest",
    ],
)
def test_preparation_rejects_drift_before_creating_output(
    diagnostic_config, tmp_path, field
):
    changed = diagnostic_config.model_copy(
        update={
            "manifest": diagnostic_config.manifest.model_copy(update={field: "f" * 64})
        }
    )
    with pytest.raises(ValueError):
        freeze_mining_diagnostic(changed, tmp_path / "rejected")
    assert not (tmp_path / "rejected").exists()
