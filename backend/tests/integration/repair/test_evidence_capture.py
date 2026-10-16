import json
from dataclasses import replace
from pathlib import Path

import pytest

from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.ids import new_id
from crucible.engine.compaction_gateway import ModelCompactionGateway
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelStop,
    ModelStopReason,
    TextDelta,
)
from crucible.repair.evidence import CaptureLimits, EvidenceRecorder
from crucible.storage.database import Database
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.execution_policy import MutationAdmission
from crucible.tools.git_tools import WriteFileTool
from crucible.tools.registry import ToolRegistry
from crucible.tools.workspace_authority import WorkspaceAuthority
from tests.integration.engine.conftest import FixedClock, uow_factory
from tests.integration.tools.test_dispatcher import prepared
from tests.unit.engine.test_compaction_gateway import request


async def setup(database, tmp_path, **kwargs):
    task, run, step, message = await prepared(database, "capture")
    factory = uow_factory(database)
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
    )
    recorder = EvidenceRecorder(factory, artifacts, FixedClock(), **kwargs)
    return task, run, step, message, factory, artifacts, recorder


@pytest.mark.parametrize(
    "kind,code",
    [
        ("missing", None),
        ("text", None),
        ("binary", "binary_file"),
        ("oversized", "file_size_limit"),
        ("symlink", "symlink"),
    ],
)
async def test_capture_is_complete_or_explicitly_unsupported(
    database: Database, tmp_path: Path, kind, code
):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path, limits=CaptureLimits(file_bytes=16)
    )
    target = tmp_path / "a.py"
    if kind == "text":
        target.write_text("old é\n")
    elif kind == "binary":
        target.write_bytes(b"a\0b")
    elif kind == "oversized":
        target.write_bytes(b"x" * 17)
    elif kind == "symlink":
        outside = tmp_path / "outside"
        outside.write_text("untouched")
        target.symlink_to(outside)
    # Create a durable call before recording linked evidence.
    dispatcher = ToolDispatcher(ToolRegistry((WriteFileTool(),)), factory, FixedClock())
    call = CompleteToolCall(new_id(), "write_file", {"path": "a.py", "content": "new"})
    await dispatcher._preflight(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (call,),
        call_budget=None,
        assistant_message=None,
    )
    admission = MutationAdmission(
        ("a.py",), WorkspaceAuthority(tmp_path, frozenset({"a.py"}), 1_000_000)
    )
    captured = await recorder.capture_preedit(task.id, run.id, call.id, admission)
    assert captured.complete is (code is None)
    assert captured.unsupported_code == code
    _, raw = await artifacts.read(captured.artifact_id)
    file = json.loads(raw)["files"][0]
    assert file["exists"] is (kind != "missing")
    if kind == "text":
        target.write_text("later")
        assert file["content"] == "old é\n"
    elif code is not None:
        assert "content" not in file
    async with factory() as uow:
        assert await uow.repair_evidence.get(captured.id, task_id=task.id) == captured
        assert len(await uow.repair_evidence.list_for_run(run.id, task_id=task.id)) == 1
    assert (
        await recorder.capture_preedit(task.id, run.id, call.id, admission)
    ).id == captured.id


@pytest.mark.parametrize(
    "count,size,code", [(33, 1, "path_count_limit"), (9, 1_048_576, "call_size_limit")]
)
async def test_call_path_and_total_limits(
    database: Database, tmp_path: Path, count, size, code
):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    paths = tuple(f"{count}-{i}.txt" for i in range(count))
    for path in paths:
        (tmp_path / path).write_bytes(b"x" * size)
    # Patch links use an existing registered call; source key still differs per call.
    patch = "".join(f"--- a/{p}\n+++ b/{p}\n@@ -1 +1 @@\n-x\n+y\n" for p in paths)
    from crucible.tools.git_tools import ApplyPatchTool

    dispatcher = ToolDispatcher(
        ToolRegistry((ApplyPatchTool(),)), factory, FixedClock()
    )
    call = CompleteToolCall(new_id(), "apply_patch", {"patch": patch})
    await dispatcher._preflight(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (call,),
        call_budget=None,
        assistant_message=None,
    )
    admission = MutationAdmission(
        paths, WorkspaceAuthority(tmp_path, frozenset(paths), 1_000_000)
    )
    result = await recorder.capture_preedit(task.id, run.id, call.id, admission)
    assert not result.complete and result.unsupported_code == code
    _, raw = await artifacts.read(result.artifact_id)
    assert len(raw) < 9 * 1_048_576 + 100_000


async def test_capture_failure_does_not_fail_permitted_edit(
    database: Database, tmp_path: Path, monkeypatch
):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    (tmp_path / "a.py").write_text("before")

    async def fail(*args, **kwargs):
        raise OSError("artifact disk unavailable")

    monkeypatch.setattr(artifacts, "put", fail)
    dispatcher = ToolDispatcher(
        ToolRegistry((WriteFileTool(),)), factory, FixedClock(), evidence=recorder
    )
    result = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (
            CompleteToolCall(
                new_id(), "write_file", {"path": "a.py", "content": "after"}
            ),
        ),
    )
    assert result[0].error_code is None
    assert (tmp_path / "a.py").read_text() == "after"
    async with factory() as uow:
        evidence = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
    assert (
        len(evidence) == 1 and evidence[0].unsupported_code == "artifact_write_failed"
    )


@pytest.mark.parametrize("failure", ["malformed", "provider_error", "raised"])
async def test_raw_compaction_survives_parse_error(
    database: Database, tmp_path: Path, failure
):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )

    class Gateway:
        async def stream(self, prepared):
            async with factory() as uow:
                prior = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
            assert [item.kind for item in prior] == ["compaction_request"]
            yield TextDelta("not json")
            if failure == "provider_error":
                yield ModelError("provider_down", "unavailable")
            elif failure == "raised":
                raise RuntimeError("stream broke")
            else:
                yield ModelStop(ModelStopReason.COMPLETE)

    source = replace(
        request(),
        task_id=task.id,
        run_id=run.id,
        step_id=step.id,
        capture_key="source",
        attempt_index=1,
    )
    with pytest.raises((ValueError, RuntimeError)):
        await ModelCompactionGateway(Gateway(), evidence=recorder).compact(source)
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
    response = next(item for item in captures if item.kind == "compaction_response")
    assert response.attempt_index == 1
    assert response.complete is (failure == "malformed")
    _, raw = await artifacts.read(response.artifact_id)
    assert json.loads(raw)["raw_text"] == "not json"
    _, raw = await artifacts.read(captures[0].artifact_id)
    payload = json.loads(raw)
    assert (
        "keep this objective"
        in payload["request"]["messages"][0]["parts"][0]["text_content"]
    )
    assert payload["request"]["model"] == "provider/model"


async def test_redaction_marks_evidence_altered(database: Database, tmp_path: Path):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path, secret_values=("private-provider-key",)
    )
    (tmp_path / "a.py").write_text('key = "private-provider-key"')
    dispatcher = ToolDispatcher(
        ToolRegistry((WriteFileTool(),)), factory, FixedClock(), evidence=recorder
    )
    await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (CompleteToolCall(new_id(), "write_file", {"path": "a.py", "content": "new"}),),
    )
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
    assert not captures[0].complete
    assert captures[0].unsupported_code == "secret_redacted"
    _, raw = await artifacts.read(captures[0].artifact_id)
    assert b"private-provider-key" not in raw


async def test_dispatcher_persists_preimage_before_mutation(
    database: Database, tmp_path: Path
):
    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    (tmp_path / "a.py").write_text("before\n")
    dispatcher = ToolDispatcher(
        ToolRegistry((WriteFileTool(),)), factory, FixedClock(), evidence=recorder
    )
    result = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, tmp_path),
        (
            CompleteToolCall(
                new_id(), "write_file", {"path": "a.py", "content": "after\n"}
            ),
        ),
    )
    assert result[0].error_code is None
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
    capture, raw = await recorder.read(captures[0].id, task_id=task.id)
    assert capture.complete
    assert json.loads(raw)["files"][0]["content"] == "before\n"
    assert (tmp_path / "a.py").read_text() == "after\n"
    other_task, other_run, _, _ = await prepared(database, "other-task")
    async with factory() as uow:
        assert await uow.repair_evidence.get(capture.id, task_id=other_task.id) is None
        with pytest.raises(ValueError):
            await uow.repair_evidence.list_for_run(run.id, task_id=other_task.id)
        with pytest.raises(ValueError):
            await uow.repair_evidence.add(
                replace(capture, id=new_id(), task_id=other_task.id)
            )
    with pytest.raises(ValueError):
        await recorder.read(capture.id, task_id=other_task.id)


async def test_oversized_compaction_is_not_saved_as_exact_evidence(
    database: Database, tmp_path: Path
):
    from crucible.engine.gateway import (
        ModelMessage,
        ModelPart,
        ModelRole,
        PreparedModelRequest,
    )

    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path, limits=CaptureLimits(compaction_bytes=64)
    )
    prepared = PreparedModelRequest(
        run.id,
        step.id,
        "fixture",
        (ModelMessage(ModelRole.SYSTEM, (ModelPart("text", "x" * 65),)),),
        (),
        20,
    )
    request_capture = await recorder.capture_request(
        task.id, run.id, 0, prepared, step_id=step.id
    )
    response = await recorder.capture_compaction(
        task.id, run.id, 0, prepared, "x" * 65, True, step_id=step.id
    )
    for capture in (request_capture, response):
        assert (
            not capture.complete and capture.unsupported_code == "compaction_size_limit"
        )
        _, raw = await recorder.read(capture.id, task_id=task.id)
        assert "x" * 65 not in raw.decode()


async def test_compaction_cancellation_keeps_partial_response(
    database: Database, tmp_path: Path
):
    import asyncio

    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    entered = asyncio.Event()

    class Gateway:
        async def stream(self, prepared):
            yield TextDelta("partial")
            entered.set()
            await asyncio.Event().wait()

    source = replace(
        request(), task_id=task.id, run_id=run.id, step_id=step.id, capture_key="cancel"
    )
    pending = asyncio.create_task(
        ModelCompactionGateway(Gateway(), evidence=recorder).compact(source)
    )
    await entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=task.id)
    response = next(c for c in captures if c.kind == "compaction_response")
    assert not response.complete and response.unsupported_code == "partial_response"
    _, raw = await recorder.read(response.id, task_id=task.id)
    assert json.loads(raw)["raw_text"] == "partial"


async def test_lifecycle_links_each_retry_to_its_original_run(
    database: Database, tmp_path: Path
):
    from crucible.context.compaction import (
        CompactionLifecycle,
        group_conversation_units,
        select_compaction_boundary,
    )
    from tests.integration.context.test_compaction_lifecycle import completed_messages

    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    # seed_exchange starts with an assistant, so use a normal submitted user run.
    from tests.integration.engine.conftest import queued_run

    submitted = await queued_run(database, tmp_path, "evidence-lifecycle")
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    messages = await completed_messages(factory, run)
    boundary = select_compaction_boundary(
        group_conversation_units(messages, {}), retain_complete_units=0
    )
    assert boundary is not None

    class Gateway:
        def __init__(self):
            self.calls = 0

        async def stream(self, prepared):
            self.calls += 1
            if self.calls == 1:
                yield TextDelta("malformed")
            else:
                yield TextDelta(
                    json.dumps(
                        {
                            field: "retained"
                            for field in (
                                "objective_and_constraints",
                                "decisions",
                                "repository_facts",
                                "changes",
                                "commands_and_validation",
                                "unresolved_problems",
                                "execution_state",
                                "important_paths_and_symbols",
                            )
                        }
                    )
                )
            yield ModelStop(ModelStopReason.COMPLETE)

    gateway = Gateway()
    lifecycle = CompactionLifecycle(
        factory,
        artifacts,
        ModelCompactionGateway(gateway, evidence=recorder),
        FixedClock(),
    )
    await lifecycle.compact(run, boundary)
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=run.task_id)
    assert len(captures) == 4
    for kind in ("compaction_request", "compaction_response"):
        selected = [item for item in captures if item.kind == kind]
        assert [item.attempt_index for item in selected] == [0, 1]
        assert len({item.source_key for item in selected}) == 1
        assert all(item.run_id == run.id for item in selected)


async def test_context_capture_contains_actual_instructions_and_schemas(
    database: Database, tmp_path: Path
):
    from crucible.context.manager import ContextManager, SimpleTokenEstimator
    from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
    from crucible.domain.repository import RepositorySettings
    from crucible.domain.steps import Step
    from crucible.engine.gateway import ModelToolDefinition
    from tests.integration.engine.conftest import queued_run

    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    submitted = await queued_run(database, tmp_path, "evidence-context")
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        command = CommandSpec(
            "python",
            ("-V",),
            ".",
            10,
            CommandNetwork.NONE,
            {"LANG": "en_US.UTF-8"},
            "python@sha256:" + "a" * 64,
            "validation",
            CommandLimits(1, 1024, 32, 1000),
        )
        run = replace(
            run, settings_snapshot=RepositorySettings(validation_commands=(command,))
        )
        await uow.runs.update(run)
        task = await uow.tasks.get(run.task_id)
        assert task is not None
        (task.workspace_path / "AGENTS.md").write_text("repository instruction")
        step = Step.preparing(new_id(), task.id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    schema = {"type": "object", "properties": {"path": {"type": "string"}}}
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="policy instruction",
        tool_contract="contract instruction",
        model="fixture",
        input_limit=10_000,
        output_reserve=100,
        tools=(ModelToolDefinition("fixture", "description", schema),),
        evidence=recorder,
    )
    prepared = await manager.prepare(run, step)
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=run.task_id)
    assert {item.kind for item in captures} == {"context_inputs", "model_context"}
    captures = tuple(item for item in captures if item.kind == "model_context")
    _, raw = await recorder.read(captures[0].id, task_id=run.task_id)
    payload = json.loads(raw)
    assert [
        m["parts"][0]["text_content"] for m in payload["request"]["messages"][:3]
    ] == ["policy instruction", "contract instruction", "repository instruction"]
    assert payload["request"]["tools"][0]["input_schema"] == schema
    assert payload["request"]["step_id"] == str(prepared.request.step_id)
    assert payload["context"]["input_limit"] == 10_000
    assert payload["context"]["estimator_version"] == SimpleTokenEstimator.version
    artifact, _ = await artifacts.read(captures[0].artifact_id)
    assert artifact.sensitivity == "private"
    assert (
        tmp_path / "artifacts" / artifact.storage_identity
    ).stat().st_mode & 0o077 == 0

    assert payload["context"]["settings"]["validation_commands"][0]["environment"] == {
        "LANG": "en_US.UTF-8"
    }


@pytest.mark.parametrize("capture_failure", ["none", "store", "secret", "oversize"])
async def test_context_inputs_are_captured_before_validation_without_interference(
    database, tmp_path, monkeypatch, capture_failure
):
    from crucible.context import selection
    from crucible.context.invariants import ContextInvariantError
    from crucible.context.manager import ContextManager, SimpleTokenEstimator
    from crucible.domain.steps import Step
    from tests.integration.engine.conftest import queued_run

    capture_broken = capture_failure != "none"
    _, _, _, _, factory, artifacts, recorder = await setup(
        database,
        tmp_path,
        limits=CaptureLimits(compaction_bytes=16)
        if capture_failure == "oversize"
        else CaptureLimits(),
        secret_values=("required policy",) if capture_failure == "secret" else (),
    )
    submitted = await queued_run(database, tmp_path, "early-context")
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        step = Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
        await uow.steps.add(step)
        await uow.commit()
    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="required policy",
        tool_contract="required contract",
        model="not-called",
        input_limit=10000,
        output_reserve=100,
        evidence=recorder,
    )
    if capture_failure == "store":

        async def unavailable(*args, **kwargs):
            raise OSError("artifact unavailable")

        monkeypatch.setattr(artifacts, "put", unavailable)
    original = selection.assemble_visible_messages
    with monkeypatch.context() as scoped:
        scoped.setattr(
            selection,
            "assemble_visible_messages",
            lambda *a, **kw: original(*a, **kw)[1:],
        )
        with pytest.raises(ContextInvariantError, match="Required instructions"):
            await manager.prepare(run, step)
    async with factory() as uow:
        captures = await uow.repair_evidence.list_for_run(run.id, task_id=run.task_id)
        assert await uow.context_manifests.get_for_step(step.id) is None
    assert len(captures) == 1 and captures[0].kind == "context_inputs"
    assert captures[0].complete is not capture_broken
    if not capture_broken:
        _, raw = await recorder.read(captures[0].id, task_id=run.task_id)
        payload = json.loads(raw)
        assert payload["context"]["required_instructions"] == [
            "required policy",
            "required contract",
        ]
        assert payload["context"]["message_ids"]
        assert payload["source_ids"]["step_id"] == str(step.id)
        await assert_context_input_replay(
            factory, artifacts, recorder, captures[0], run, step, tmp_path
        )
    await manager.prepare(run, step)


async def assert_context_input_replay(
    factory, artifacts, recorder, capture, run, step, tmp_path
):
    from uuid import uuid4

    from crucible.domain.repair import RepairRun, RepairStage, RepairStatus
    from crucible.domain.run import RunStatus
    from crucible.repair.cases import ContextSelectionCase, seal_case
    from crucible.repair.incident_context import IncidentContext
    from crucible.repair.trials import TrialService

    repair = RepairRun(
        uuid4(),
        run.task_id,
        RepairStatus.QUEUED,
        RepairStage.INTAKE,
        {},
        None,
        None,
        "a" * 40,
        None,
        FixedClock().now(),
        FixedClock().now(),
    )
    async with factory() as uow:
        await uow.runs.update(replace(run, status=RunStatus.FAILED))
        await uow.repairs.add(repair)
        await uow.commit()
    case = ContextSelectionCase(
        kind="context_selection",
        case_id="early-context",
        incident_ids={"task": run.task_id, "run": run.id, "step": step.id},
        replay_mode="direct",
        fixture_manifest_digest="controller-owned",
        settings={},
        expected_baseline_failure={
            "status": "error",
            "error_code": "ContextInvariantError",
        },
        candidate_assertions=({"op": "context_within_estimated_limit"},),
        expected_behavior="Preserve required instructions",
        evidence_ids=(capture.id,),
        source_artifact_ids=(capture.artifact_id,),
        correlation_links={},
    )
    incident = IncidentContext(factory, artifacts)
    await incident.authorize_case(repair.id, case)
    settings = await incident.replay_settings(repair.id, case)
    assert settings["required_instructions"] == ["required policy", "required contract"]
    assert settings["recent_complete_units"] == 2
    sealed = seal_case(case.model_copy(update={"settings": settings}), uuid4())
    service = TrialService(
        factory,
        None,
        source_root=tmp_path,
        fixtures={},
        artifacts=artifacts,
        clock=FixedClock(),
        invocation_id=uuid4(),
    )
    manifest = await service._materialize(sealed)
    assert [str(item["id"]) for item in manifest["messages"]] == (
        await _captured_message_ids(recorder, capture, run.task_id)
    )
    assert (
        manifest["settings"]["required_instructions"]
        == settings["required_instructions"]
    )
    with pytest.raises(ValueError, match="another preparation"):
        await incident.replay_settings(
            repair.id,
            case.model_copy(
                update={
                    "incident_ids": {**case.incident_ids, "step": uuid4()},
                }
            ),
        )


async def _captured_message_ids(recorder, capture, task_id):
    _, raw = await recorder.read(capture.id, task_id=task_id)
    return json.loads(raw)["context"]["message_ids"]


async def test_container_captures_runtime_identity_once(
    database: Database, tmp_path: Path, monkeypatch
):
    from crucible.application.container import ApplicationContainer
    from crucible.repair.identity import RuntimeIdentity
    from tests.integration.evals.test_runner import FakeDockerClient

    calls = []
    frozen = RuntimeIdentity("a" * 40, "source", "lock")

    async def identity(root):
        calls.append(root)
        return frozen

    monkeypatch.setattr(
        "crucible.application.container.capture_runtime_identity", identity
    )
    container = await ApplicationContainer.create(
        f"sqlite+aiosqlite:///{database.path}",
        tmp_path / "container",
        model_id="fake",
        docker_client=FakeDockerClient(),
    )
    try:
        await container.start()
        assert container.runtime_identity is frozen
        assert container.evidence.runtime_identity is frozen
        assert len(calls) == 1
        await container.start()
        assert len(calls) == 1
    finally:
        await container.close()


async def test_evidence_read_rejects_changed_artifact(
    database: Database, tmp_path: Path
):
    from crucible.engine.gateway import PreparedModelRequest

    task, run, step, message, factory, artifacts, recorder = await setup(
        database, tmp_path
    )
    prepared = PreparedModelRequest(run.id, step.id, "fixture", (), (), 10)
    capture = await recorder.capture_compaction(
        task.id, run.id, 0, prepared, "original", True, step_id=step.id
    )
    artifact, _ = await artifacts.read(capture.artifact_id)
    (tmp_path / "artifacts" / artifact.storage_identity).write_bytes(b"changed")
    with pytest.raises(ValueError, match="digest"):
        await recorder.read(capture.id, task_id=task.id)
