"""Role runs use the real harness and a deterministic local provider boundary."""

import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.repair import RepairAttempt, RepairStatus
from crucible.engine.gateway import ModelStop, ModelStopReason, TextDelta
from crucible.engine.run_engine import RunEngine
from crucible.repair.incident_context import IncidentContext
from crucible.repair.roles import RoleExecutor, parse_role_result
from crucible.tools.dispatcher import ToolDispatcher
from tests.integration.repair.test_admission import environment
from tests.integration.storage.test_tool_loop_storage import CLOCK, NOW


class Provider:
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.requests = []

    async def stream(self, request):
        self.requests.append(request)
        output = self.outputs.pop(0)
        if callable(output):
            output = output(request)
        import inspect

        if inspect.isawaitable(output):
            output = await output
        if isinstance(output, list):
            for item in output:
                yield item
            return
        if isinstance(output, Exception):
            raise output
        yield TextDelta(json.dumps(output) if not isinstance(output, str) else output)
        yield ModelStop(ModelStopReason.COMPLETE)


async def role_environment(database, tmp_path, outputs):
    service, task, factory, git, github, scheduler = await environment(database)
    from crucible.application.repair_service import RepairService
    from tests.integration.repair.test_admission import IDENTITY

    service = RepairService(factory, CLOCK, git, github, IDENTITY)
    root = tmp_path / "source"
    root.mkdir()
    (root / "visible.txt").write_text("source fact")
    (root / "evals" / "held-out").mkdir(parents=True)
    (root / "evals" / "held-out" / "secret.txt").write_text("HELDOUT_SENTINEL")
    from crucible.repair.ports import WorktreeIdentity

    async def worktree(*args):
        return WorktreeIdentity(str(root), "repair/test")

    git.create_worktree = worktree
    result = await service.command(task.id, "/repair", "start")
    async with factory() as uow:
        run = await uow.repairs.get(result.repair_id)
        await uow.repairs.update(replace(run, status=RepairStatus.RUNNING))
        await uow.commit()
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )
    incident = IncidentContext(factory, artifacts)
    provider = Provider(outputs)

    def engine_factory(registry):
        context = ContextManager(
            factory,
            CLOCK,
            SimpleTokenEstimator(),
            harness_policy="Protected repair role execution.",
            tool_contract="Use supplied tools.",
            model="fake",
            input_limit=100000,
            output_reserve=4096,
            tools=registry.definitions,
        )
        return RunEngine(
            factory,
            CLOCK,
            provider,
            context_manager=context,
            dispatcher=ToolDispatcher(registry, factory, CLOCK),
        )

    roles = RoleExecutor(factory, CLOCK, artifacts, engine_factory, incident)
    return service, task, factory, roles, provider, incident, artifacts


async def test_role_has_separate_task_and_conversation_and_no_write_tools(
    database, tmp_path
):
    env = await role_environment(
        database,
        tmp_path,
        [
            {
                "decision": "no_change_needed",
                "reasons": ["No harness defect"],
                "fix_items": [],
            }
        ],
    )
    service, task, factory, roles, provider, incident, _ = env
    repair = (await service.list_for_task(task.id))[0]
    attempt = RepairAttempt(
        uuid4(), repair.id, None, "diagnosis", 1, None, "pending", False, NOW
    )
    async with factory() as uow:
        before = await uow.messages.list_for_task(task.id)
        await uow.repairs.add_attempt(attempt)
        await uow.commit()
    result = await roles.execute(repair.id, "diagnosis", None, attempt.id, ())
    assert result.decision == "no_change_needed"
    assert {t.name for t in provider.requests[0].tools} == {
        "list_files",
        "read_file",
        "search_files",
        "read_incident_evidence",
        "read_incident_messages",
    }
    async with factory() as uow:
        assert await uow.messages.list_for_task(task.id) == before
        stored = (await uow.repairs.list_attempts(repair.id))[0]
        assert stored.charged and stored.harness_run_id
        role_run = await uow.runs.get(stored.harness_run_id)
        internal = await uow.tasks.get(role_run.task_id)
        assert internal.internal_repair_id == repair.id
        assert internal.workspace_path == Path(repair.worktree_path)
        assert internal.id not in {item.id for item in await uow.tasks.list()}
    assert "HELDOUT_SENTINEL" not in repr(provider.requests)
    with pytest.raises(ValueError):
        await incident.read_evidence(repair.id, uuid4())


@pytest.mark.parametrize(
    "raw",
    [
        '{"decision":"invented"}',
        '{"decision":"no_change_needed","reasons":[],"fix_items":[],"auto_approve":true}',
        "```json\n{}\n```",
    ],
)
def test_strict_role_records_reject_unknown_output(raw):
    with pytest.raises(ValueError):
        parse_role_result("diagnosis", raw)


async def test_diagnosis_tools_reject_mutation_and_heldout_reads(database, tmp_path):
    from crucible.engine.gateway import CompleteToolCall

    outputs = [
        [
            CompleteToolCall(
                uuid4(), "write_file", {"path": "visible.txt", "content": "changed"}
            ),
            CompleteToolCall(
                uuid4(), "read_file", {"path": "evals/held-out/secret.txt"}
            ),
            ModelStop(ModelStopReason.TOOL_CALLS),
        ],
        {
            "decision": "no_change_needed",
            "reasons": ["Cannot change protected state"],
            "fix_items": [],
        },
    ]
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(database, tmp_path, outputs)
    repair = (await service.list_for_task(task.id))[0]
    attempt = RepairAttempt(
        uuid4(), repair.id, None, "diagnosis", 1, None, "pending", False, NOW
    )
    async with factory() as uow:
        await uow.repairs.add_attempt(attempt)
        await uow.commit()
    await roles.execute(repair.id, "diagnosis", None, attempt.id, ())
    assert (tmp_path / "source" / "visible.txt").read_text() == "source fact"
    assert "HELDOUT_SENTINEL" not in repr(provider.requests)
    assert "Unknown tool: write_file" in repr(provider.requests)
    assert "Source path is unavailable" in repr(provider.requests)


@pytest.mark.parametrize("valid_owner", [True, False])
async def test_proposal_commands_get_exact_durable_controller_approval(
    database, tmp_path, valid_owner
):
    from crucible.application.approval_service import ApprovalService
    from crucible.application.command_authority import CommandAuthority
    from crucible.domain.repair import RepairFix, RepairStage
    from crucible.engine.approval_broker import InMemoryApprovalBroker
    from crucible.engine.gateway import CompleteToolCall
    from crucible.sandbox.fake import FakeSandboxBackend
    from crucible.sandbox.protocol import SandboxOutcome, SandboxTermination
    from crucible.sandbox.resources import TaskResourceManager
    from crucible.tools.command import ExecuteCommandTool
    from tests.unit.sandbox.test_resources import FakeDockerClient

    fix_id = uuid4()
    outputs = [
        [
            CompleteToolCall(
                uuid4(),
                "execute_command",
                {
                    "executable": "python",
                    "arguments": ["-V"],
                    "reason": "Inspect runtime",
                    "environment": {"CI": "1"},
                },
            ),
            ModelStop(ModelStopReason.TOOL_CALLS),
        ],
        {
            "fix_id": str(fix_id),
            "hypothesis": "inspect",
            "mechanism": "inspect",
            "declared_paths": [],
            "regression_risks": [],
        },
    ]
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(database, tmp_path, outputs)
    repair = (await service.list_for_task(task.id))[0]
    import subprocess

    root = tmp_path / "source"
    for args in (
        ("init", "-qb", "repair/test"),
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.test"),
        ("add", "."),
        ("commit", "-qm", "fixture"),
    ):
        subprocess.run(["git", *args], cwd=root, check=True)
    parent = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    repair = replace(repair, last_accepted_commit=parent)
    broker = InMemoryApprovalBroker()
    approvals = ApprovalService(factory, CLOCK, broker)
    authority = CommandAuthority(
        approvals,
        broker,
        CLOCK,
        factory,
        lambda: service.execution_id if valid_owner else "wrong-execution",
    )
    resources = TaskResourceManager(FakeDockerClient(), factory, CLOCK)
    sandbox = FakeSandboxBackend(
        SandboxOutcome(
            0,
            SandboxTermination.COMPLETED,
            NOW,
            NOW,
            "sha256:" + "a" * 64,
            "container",
            0,
            0,
            False,
        ),
        (),
    )
    roles.command_tool = ExecuteCommandTool(authority, sandbox, resources, artifacts)
    roles.resources = resources
    async with factory() as uow:
        source = await uow.tasks.get(task.id)
        repository = await uow.repositories.get(source.repository_id)
        await uow.repositories.update(
            replace(
                repository,
                settings=replace(
                    repository.settings, sandbox_image="runner@sha256:" + "a" * 64
                ),
            )
        )
        await uow.repairs.update(
            replace(repair, stage=RepairStage.PROPOSAL, current_fix_id=fix_id)
        )
        await uow.repairs.add_fix(
            RepairFix(fix_id, repair.id, 0, "dx", "approved", "approved")
        )
        attempt = RepairAttempt(
            uuid4(), repair.id, fix_id, "proposal", 1, None, "pending", False, NOW
        )
        await uow.repairs.add_attempt(attempt)
        await uow.commit()
    result = await roles.execute(repair.id, "proposal", fix_id, attempt.id, ())
    assert result.fix_id == fix_id
    assert len(sandbox.requests) == (1 if valid_owner else 0)
    async with factory() as uow:
        recorded = (await uow.repairs.list_attempts(repair.id))[0]
        run = await uow.runs.get(recorded.harness_run_id)
        automatic = (await uow.approvals.list_for_run(run.id))[0]
        if not valid_owner:
            assert automatic.status == "pending" and automatic.decided_by is None
            return
        assert automatic.status == "approved"
        assert automatic.decided_by == f"repair-controller:{repair.id}"
        assert sandbox.requests[0].command == automatic.spec
        assert automatic.spec.executable == "python" and automatic.spec.arguments == (
            "-V",
        )
        assert automatic.spec.environment == {"CI": "1"}


async def completed_internal_role(database, tmp_path):
    env = await role_environment(
        database,
        tmp_path,
        [{"decision": "no_change_needed", "reasons": ["No defect"], "fix_items": []}],
    )
    service, task, factory, roles, provider, incident, artifacts = env
    repair = (await service.list_for_task(task.id))[0]
    attempt = RepairAttempt(
        uuid4(), repair.id, None, "diagnosis", 1, None, "pending", False, NOW
    )
    async with factory() as uow:
        await uow.repairs.add_attempt(attempt)
        await uow.commit()
    await roles.execute(repair.id, "diagnosis", None, attempt.id, ())
    async with factory() as uow:
        recorded = (await uow.repairs.list_attempts(repair.id))[0]
        run = await uow.runs.get(recorded.harness_run_id)
        internal = await uow.tasks.get(run.task_id)
    return env, repair, internal, run


async def test_startup_never_schedules_an_internal_role_even_without_attempt_link(
    database, tmp_path
):
    from crucible.domain.run import Run
    from crucible.engine.supervisor import LocalRunSupervisor

    env, repair, internal, run = await completed_internal_role(database, tmp_path)
    service, task, factory, roles, provider, incident, artifacts = env
    queued = Run.queued(
        run_id=uuid4(), task_id=internal.id, triggering_message_id=None, created_at=NOW
    )
    async with factory() as uow:
        await uow.runs.add(queued)
        await uow.commit()
    supervisor = LocalRunSupervisor(RunEngine(factory, CLOCK, provider), factory, CLOCK)
    await supervisor.reconcile()
    await supervisor.close()
    assert len(provider.requests) == 1
    async with factory() as uow:
        assert (await uow.runs.get(queued.id)).status == "queued"


async def test_authenticated_public_apis_cannot_read_or_steer_internal_tasks(
    database, tmp_path
):
    from httpx import ASGITransport, AsyncClient

    from crucible.api.app import create_app
    from crucible.application.auth_service import AuthService
    from crucible.application.event_service import TaskEventSource
    from crucible.application.message_service import MessageService
    from crucible.application.run_service import RunService
    from crucible.application.task_service import TaskService
    from crucible.engine.notifier import TaskEventNotifier
    from crucible.workspaces.git import SubprocessGitClient
    from crucible.workspaces.manager import WorkspaceManager
    from tests.contract.api.test_messages import RecordingSupervisor

    env, repair, internal, run = await completed_internal_role(database, tmp_path)
    service, task, factory, roles, provider, incident, artifacts = env
    supervisor = RecordingSupervisor()
    tasks = TaskService(
        WorkspaceManager(SubprocessGitClient(), tmp_path / "tasks"), factory, CLOCK
    )
    messages = MessageService(factory, CLOCK, supervisor, repairs=service)
    events = TaskEventSource(factory, TaskEventNotifier())
    app = create_app(
        task_service=tasks,
        message_service=messages,
        run_service=RunService(factory, CLOCK, supervisor),
        event_source=events,
        repair_service=service,
        auth_service=AuthService(factory, CLOCK, "bootstrap"),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        response = await client.post(
            "/api/auth/bootstrap", json={"secret": "bootstrap"}
        )
        headers = {
            "Origin": "http://127.0.0.1:8000",
            "X-CSRF-Token": response.json()["csrfToken"],
            "Idempotency-Key": "internal",
        }
        assert [entry["id"] for entry in (await client.get("/api/tasks")).json()] == [
            str(task.id)
        ]
        for suffix in ("", "/trace", "/messages", "/repairs", "/events"):
            denied = await client.get(f"/api/tasks/{internal.id}{suffix}")
            assert denied.status_code == 404
        for text in ("ordinary message", "/repair"):
            denied = await client.post(
                f"/api/tasks/{internal.id}/messages",
                json={"text": text},
                headers=headers,
            )
            assert denied.status_code == 404
        assert (
            await client.post(f"/api/runs/{run.id}/cancel", headers=headers)
        ).status_code == 404
    assert supervisor.submissions == []


async def test_source_inspection_redacts_known_credentials(
    database, tmp_path, monkeypatch
):
    from crucible.engine.gateway import CompleteToolCall

    secret = "KNOWN_PRIVATE_KEY_SENTINEL"
    monkeypatch.setenv("TEST_API_KEY", secret)
    outputs = [
        [
            CompleteToolCall(uuid4(), "read_file", {"path": "visible.txt"}),
            ModelStop(ModelStopReason.TOOL_CALLS),
        ],
        {"decision": "no_change_needed", "reasons": ["No defect"], "fix_items": []},
    ]
    (
        service,
        task,
        factory,
        roles,
        provider,
        incident,
        artifacts,
    ) = await role_environment(database, tmp_path, outputs)
    (tmp_path / "source" / "visible.txt").write_text(f"source credential={secret}")
    repair = (await service.list_for_task(task.id))[0]
    attempt = RepairAttempt(
        uuid4(), repair.id, None, "diagnosis", 1, None, "pending", False, NOW
    )
    async with factory() as uow:
        await uow.repairs.add_attempt(attempt)
        await uow.commit()
    await roles.execute(repair.id, "diagnosis", None, attempt.id, ())
    assert secret not in repr(provider.requests)
    assert "[redacted]" in repr(provider.requests)
