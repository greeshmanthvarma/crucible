"""Replay authority comes from immutable historical calls and their outcomes."""

from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from crucible.domain.tools import (
    ToolCall,
    ToolCallStatus,
    ToolExecutionMode,
    ToolResult,
    ToolResultStatus,
)
from crucible.repair.cases import IncidentCase
from crucible.repair.evidence import EvidenceCapture
from crucible.repair.identity import canonical_json, digest
from crucible.repair.trials import private_artifact
from tests.integration.repair.test_roles import role_environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


async def record_failed_tool(
    factory,
    task,
    name="read_file",
    arguments=None,
    *,
    code="missing",
    detail="Missing original file",
    status=ToolResultStatus.FAILED,
):
    async with factory() as uow:
        run = (await uow.runs.list_for_task(task.id))[0]
        step = (await uow.steps.list_for_run(run.id))[0]
        message = next(
            m for m in await uow.messages.list_for_task(task.id) if m.step_id == step.id
        )
        calls = await uow.tool_calls.list_for_step(step.id)
        results = await uow.tool_results.list_for_step(step.id)
        call = ToolCall(
            uuid4(),
            task.id,
            run.id,
            step.id,
            message.id,
            len(calls) + 1,
            name,
            arguments or {"path": "original.txt"},
            1,
            None,
            ToolExecutionMode.SEQUENTIAL,
            CLOCK.now(),
            ToolCallStatus.COMPLETED,
        )
        await uow.tool_calls.add(call)
        if status is not None:
            await uow.tool_results.add(
                ToolResult(
                    uuid4(),
                    task.id,
                    run.id,
                    step.id,
                    call.id,
                    status,
                    {},
                    1,
                    detail,
                    code,
                    len(results) + 1,
                    CLOCK.now(),
                    CLOCK.now(),
                )
            )
        await uow.commit()
    return run, step, call


async def binding_environment(
    database, tmp_path, *, mutation=False, status=ToolResultStatus.FAILED
):
    service, task, factory, _, _, incident, artifacts = await role_environment(
        database, tmp_path, []
    )
    repair = (await service.list_for_task(task.id))[0]
    arguments = (
        {"path": "note.txt", "content": "original bytes"}
        if mutation
        else {"path": "original.txt"}
    )
    run, step, call = await record_failed_tool(
        factory,
        task,
        "write_file" if mutation else "read_file",
        arguments,
        status=status,
    )
    case = TypeAdapter(IncidentCase).validate_python(
        {
            "schema_version": 1,
            "case_id": "historical",
            "incident_ids": {
                "task": task.id,
                "run": run.id,
                "step": step.id,
                "tool_call": call.id,
            },
            "kind": "tool",
            "replay_mode": "direct",
            "fixture_manifest_digest": "fixture",
            "settings": {},
            "expected_baseline_failure": {
                "status": "error",
                "error_code": "missing",
                "detail_contains": ["Missing"],
            },
            "candidate_assertions": [{"op": "error_code_equals", "value": None}],
            "expected_behavior": "Original operation succeeds",
            "evidence_ids": [],
            "tool_name": call.name,
            "arguments": arguments,
        }
    )
    return repair, task, factory, incident, artifacts, case, call


@pytest.mark.parametrize(
    "change", ["arguments", "missing_call", "error_code", "detail", "step"]
)
async def test_tool_replay_rejects_unbound_inputs_or_failure(
    database, tmp_path, change
):
    repair, task, factory, incident, artifacts, case, call = await binding_environment(
        database, tmp_path
    )
    await incident.authorize_case(repair.id, case)
    if change == "arguments":
        case = case.model_copy(update={"arguments": {"path": "unrelated.txt"}})
    elif change == "missing_call":
        case = case.model_copy(
            update={
                "incident_ids": {
                    k: v for k, v in case.incident_ids.items() if k != "tool_call"
                }
            }
        )
    elif change == "error_code":
        case = case.model_copy(
            update={
                "expected_baseline_failure": case.expected_baseline_failure.model_copy(
                    update={"error_code": "different_failure"}
                )
            }
        )
    elif change == "detail":
        case = case.model_copy(
            update={
                "expected_baseline_failure": case.expected_baseline_failure.model_copy(
                    update={"detail_contains": ("invented failure",)}
                )
            }
        )
    else:
        from crucible.domain.steps import Step

        other = Step.preparing(uuid4(), task.id, call.run_id, 2, CLOCK.now())
        async with factory() as uow:
            await uow.steps.add(other)
            await uow.commit()
        case = case.model_copy(
            update={"incident_ids": {**case.incident_ids, "step": other.id}}
        )
    with pytest.raises(ValueError):
        await incident.authorize_case(repair.id, case)


@pytest.mark.parametrize(
    "status", [ToolResultStatus.SUCCEEDED, ToolResultStatus.DENIED, None]
)
async def test_successful_call_in_failed_run_is_not_failure_provenance(
    database, tmp_path, status
):
    repair, _, _, incident, _, case, _ = await binding_environment(
        database, tmp_path, status=status
    )
    with pytest.raises(ValueError):
        await incident.authorize_case(repair.id, case)


@pytest.mark.parametrize("bound", ["other_call", "missing", "forged_body", "bound"])
async def test_mutation_preedit_capture_must_belong_to_exact_failed_call(
    database, tmp_path, bound
):
    repair, task, factory, incident, artifacts, case, call = await binding_environment(
        database, tmp_path, mutation=True
    )
    if bound == "missing":
        with pytest.raises(ValueError):
            await incident.authorize_case(repair.id, case)
        return
    if bound == "other_call":
        _, _, call = await record_failed_tool(
            factory, task, "write_file", dict(case.arguments)
        )
    source = {
        "task_id": str(task.id),
        "run_id": str(call.run_id),
        "tool_call_id": str(call.id),
    }
    body = {
        "schema_version": 1,
        "kind": "preedit",
        "source_ids": source,
        "complete": True,
        "files": [{"path": "note.txt", "exists": True, "content": "before"}],
    }
    if bound == "forged_body":
        body["source_ids"] = {**source, "tool_call_id": str(uuid4())}
    artifact = await private_artifact(artifacts, task.id, canonical_json(body))
    capture = EvidenceCapture(
        uuid4(),
        task.id,
        call.run_id,
        "preedit",
        artifact,
        True,
        None,
        source,
        digest(body),
        CLOCK.now(),
        str(call.id),
        tool_call_id=call.id,
    )
    async with factory() as uow:
        await uow.repair_evidence.add(capture)
        await uow.commit()
    case = case.model_copy(update={"evidence_ids": (capture.id,)})
    if bound == "bound":
        await incident.authorize_case(repair.id, case)
    else:
        with pytest.raises(ValueError):
            await incident.authorize_case(repair.id, case)
