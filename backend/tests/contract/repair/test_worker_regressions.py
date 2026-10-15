"""Real worker regressions for review findings, with deterministic inputs only."""

import asyncio
import json
import socket
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import uvicorn
from fastapi import FastAPI
from pydantic import TypeAdapter

from crucible.api.repair_gateway import router
from crucible.domain.clock import SystemClock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.evals import EvalBudgets
from crucible.domain.repository import RepositorySettings
from crucible.engine.gateway import ModelError
from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import EvalPartition, load_suite
from crucible.repair.cases import IncidentCase
from crucible.repair.fingerprints import capture_source_identity
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.identity import canonical_json
from crucible.repair.verification import verify_observation
from crucible.repair.worker_protocol import WorkerJob
from crucible.repair.worker_supervisor import WorkerSupervisor
from crucible.sandbox.docker_client import SubprocessDockerClient
from tests.contract.repair.test_worker_isolation import (
    real_worker_image as real_worker_image,
)


async def launch(
    tmp_path,
    image,
    fixture,
    manifest,
    *,
    settings=None,
    credential=None,
    gateway=None,
    job_id=None,
):
    source = Path(__file__).resolve().parents[4]
    identity = await capture_source_identity(source)
    job = WorkerJob(
        job_id=job_id or uuid4(),
        case_id="synthetic-review-regression",
        repeat_index=0,
        purpose="baseline",
        source_digest=identity.tree_digest,
        input_manifest=manifest,
        settings=settings or {},
        deadline_seconds=40,
    )
    supervisor = WorkerSupervisor(
        SubprocessDockerClient(), image=image, root=tmp_path / "jobs", gateway=gateway
    )
    return await supervisor.run(job, source, fixture, credential)


@pytest.mark.docker
async def test_git_fixture_is_exported_without_inherited_metadata(
    tmp_path, real_worker_image
):
    case = load_suite(EvalPartition.DEVELOPMENT, "smoke").cases[0]
    prepared = FixturePreparer(tmp_path).prepare(case, uuid4())
    assert (prepared.root / ".git").is_dir()
    result = await launch(
        tmp_path,
        real_worker_image,
        prepared.root,
        {
            "kind": "tool",
            "tool_name": "read_file",
            "arguments": {"path": "README.md"},
        },
    )
    assert result.termination == "completed", result
    assert (
        result.observation is not None and result.observation.status == "completed"
    ), result.observation
    config = (result.workspace_snapshot / ".git" / "config").read_text()
    assert '[remote "origin"]' not in config
    assert str(case.fixture_path) not in config
    assert result.initial_workspace_snapshot is not None
    assert (prepared.root / ".git").is_dir()  # Caller fixture is preserved.


def context_messages(*, large=False, units=1):
    task, run = uuid4(), uuid4()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    messages = tuple(
        Message(
            uuid4(),
            task,
            run,
            None,
            index,
            role,
            MessageStatus.INTERRUPTED if large else MessageStatus.COMPLETED,
            (
                MessagePart(
                    uuid4(),
                    1,
                    MessagePartKind.TEXT,
                    "x" * 20000 if large else "synthetic captured context",
                ),
            ),
            now,
            now,
        )
        for index, role in enumerate((MessageRole.USER, MessageRole.ASSISTANT) * units)
    )
    return json.loads(canonical_json(messages))


@pytest.mark.docker
@pytest.mark.parametrize(
    "scenario,code,fragment",
    [
        ("parse", "ValueError", "structured summary schema"),
        ("capacity", "ContextLimitExceeded", "exceeds"),
    ],
)
async def test_context_behavioral_errors_reach_baseline_matcher(
    tmp_path, real_worker_image, scenario, code, fragment
):
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    kind = "compaction" if scenario == "parse" else "context_selection"
    manifest = {
        "kind": kind,
        "messages": context_messages(large=scenario == "capacity"),
        "calls": [],
        "results": [],
    }
    settings = {"retain_complete_units": 0}
    if scenario == "parse":
        manifest["raw_response"] = "{"
    else:
        settings.update(input_limit=32, output_reserve=0)
    result = await launch(
        tmp_path, real_worker_image, fixture, manifest, settings=settings
    )
    assert result.termination == "completed", result
    observed = result.observation
    assert observed is not None and observed.status == "error", observed
    assert observed.error_code == code
    assert fragment in observed.detail
    assert len(observed.detail.encode()) <= 1024
    payload = {
        "schema_version": 1,
        "case_id": "synthetic-review-context",
        "incident_ids": {},
        "kind": kind,
        "replay_mode": "recorded" if scenario == "parse" else "direct",
        "fixture_manifest_digest": "synthetic",
        "settings": settings,
        "expected_baseline_failure": {
            "status": "error",
            "error_code": code,
            "detail_contains": [fragment],
        },
        "candidate_assertions": [{"op": "error_code_equals", "value": None}],
        "expected_behavior": "Synthetic regression, not an improvement metric.",
        "evidence_ids": [],
        "source_artifact_ids": [],
        "correlation_links": {},
    }
    if scenario == "parse":
        payload.update(request_artifact_id=uuid4(), raw_response_artifact_id=uuid4())
    case = TypeAdapter(IncidentCase).validate_python(payload)
    assert all(
        item.passed
        for item in verify_observation(
            case,
            observed.model_dump(mode="json"),
            fixture,
            result.workspace_snapshot,
            "baseline",
        )
    )
    assert not all(
        item.passed
        for item in verify_observation(
            case,
            observed.model_dump(mode="json"),
            fixture,
            result.workspace_snapshot,
            "candidate",
        )
    )


@pytest.mark.docker
@pytest.mark.parametrize("kind", ["repository", "compaction", "context_selection"])
async def test_provider_failure_provenance_and_infrastructure_classification(
    tmp_path, real_worker_image, kind
):
    class Outage:
        async def stream(self, request):
            yield ModelError("provider_unavailable", "Synthetic provider outage")

    listener = socket.socket()
    listener.bind(("0.0.0.0", 0))
    listener.listen(16)
    gateway = ScopedModelGateway(
        Outage(),
        clock=SystemClock(),
        endpoint=f"http://host.docker.internal:{listener.getsockname()[1]}/internal/repair-model/responses",
    )
    app = FastAPI()
    app.state.repair_gateway = gateway
    app.include_router(router)
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="off"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(100):
            if server.started:
                break
            if serving.done():
                await serving
            await asyncio.sleep(0.05)
        assert server.started
        job_id = uuid4()
        credential = gateway.issue(
            job_id,
            "test-model",
            {"max_requests": 2, "max_tokens": 100000},
            timedelta(seconds=120),
        )
        fixture = tmp_path / "fixture"
        fixture.mkdir()
        manifest = json.loads(
            canonical_json(
                {
                    "kind": "repository",
                    "prompt": "Synthetic provider outage probe",
                    "repository_settings": asdict(RepositorySettings()),
                    "budgets": asdict(EvalBudgets()),
                }
            )
        )
        settings = {"model": "test-model"}
        if kind != "repository":
            manifest = {
                "kind": kind,
                "messages": context_messages(),
                "calls": [],
                "results": [],
            }
            if kind == "context_selection":
                # Exceed the default two-unit retained tail so compaction
                # actually calls the outage adapter before the capacity failure.
                manifest["messages"] = context_messages(units=3)
                settings.update(input_limit=32, output_reserve=0)
        result = await launch(
            tmp_path,
            real_worker_image,
            fixture,
            manifest,
            settings=settings,
            credential=credential,
            gateway=gateway,
            job_id=job_id,
        )
        assert result.termination == "completed", result
        observed = result.observation
        assert observed is not None and observed.status == "error", observed
        assert gateway.usage(job_id).requests >= 1
        if kind != "repository":
            assert observed.error_code == "worker_execution_error"
            assert "Synthetic provider outage" not in (observed.detail or "")
            return
        assert gateway.usage(job_id).requests == 1
        assert isinstance(observed.tool_result, dict), observed
        assert observed.tool_result["run_outcome_code"] == "model_gateway_error"
        assert observed.tool_result["run_status"] == "failed"
        assert "Model request could not complete" in observed.detail
        assert "Synthetic provider outage" not in observed.detail
        assert len(observed.detail.encode()) <= 1024
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 5)
        listener.close()


@pytest.mark.parametrize(
    "error",
    [
        ValueError("synthetic parse failure"),
        RuntimeError("synthetic selection failure"),
    ],
)
def test_behavior_error_identity_and_bounded_utf8_detail(error):
    from crucible.repair.worker import WorkerBehaviorFailure, behavior_errors

    with pytest.raises(WorkerBehaviorFailure) as captured:
        with behavior_errors():
            raise type(error)(str(error) + "é" * 2000)
    assert captured.value.code == type(error).__name__
    assert str(error) in captured.value.detail
    assert len(captured.value.detail.encode("utf-8")) <= 1024


def test_infrastructure_cause_is_not_context_behavior_failure():
    from crucible.context.manager import ContextLimitExceeded
    from crucible.repair.worker import WorkerInfrastructureFailure, behavior_errors

    with pytest.raises(WorkerInfrastructureFailure):
        with behavior_errors():
            try:
                raise WorkerInfrastructureFailure("synthetic outage")
            except WorkerInfrastructureFailure as error:
                raise ContextLimitExceeded("capacity failure after outage") from error
