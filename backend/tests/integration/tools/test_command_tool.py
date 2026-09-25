import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from crucible.application.approval_service import ApprovalService
from crucible.application.artifact_service import ArtifactService
from crucible.application.command_authority import CommandAuthority
from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.ids import new_id
from crucible.domain.tools import ToolResultStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker
from crucible.engine.gateway import CompleteToolCall
from crucible.sandbox.fake import FakeSandboxBackend
from crucible.sandbox.protocol import (
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxTermination,
)
from crucible.sandbox.resources import TaskResourceManager
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.command import ExecuteCommandTool
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.registry import ToolRegistry
from tests.integration.storage.test_tool_loop_storage import seed_exchange
from tests.unit.sandbox.test_resources import FakeDockerClient

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


async def wait_for_pending(database: Database, run_id):
    for _ in range(100):
        async with SqlAlchemyUnitOfWork(database) as uow:
            pending = await uow.approvals.list_pending_for_run(run_id)
        if pending:
            return pending[0]
        await asyncio.sleep(0.01)
    raise AssertionError("Approval was not persisted")


async def test_command_waits_for_exact_approval_then_returns_bounded_evidence(
    database: Database, tmp_path: Path
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "command-tool")
        await uow.commit()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    broker = InMemoryApprovalBroker()
    approvals = ApprovalService(factory, CLOCK, broker)
    authority = CommandAuthority(approvals, broker, CLOCK)
    resources = TaskResourceManager(FakeDockerClient(), factory, CLOCK)
    await resources.ensure_dependency_volume(task.id)
    sandbox = FakeSandboxBackend(
        SandboxOutcome(
            0,
            SandboxTermination.COMPLETED,
            NOW,
            NOW,
            "sha256:" + "b" * 64,
            "container",
            8,
            2,
            True,
        ),
        (OutputChunk(OutputStream.STDOUT, 1, b"ok"),),
    )
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )
    tool = ExecuteCommandTool(authority, sandbox, resources, artifacts)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, CLOCK)
    call_id = new_id()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    execution = asyncio.create_task(
        dispatcher.execute_batch(
            DispatchContext(task.id, run.id, step.id, message.id, workspace),
            (
                CompleteToolCall(
                    call_id,
                    "execute_command",
                    {
                        "executable": "python",
                        "arguments": ["-V"],
                        "cwd": ".",
                        "timeoutSeconds": 30,
                        "network": "none",
                        "environment": {"CI": "1"},
                        "image": "runner@sha256:" + "a" * 64,
                        "reason": "Check Python",
                        "limits": {
                            "cpus": 1,
                            "memoryBytes": 1024**3,
                            "pids": 64,
                            "outputBytes": 100,
                        },
                    },
                ),
            ),
        )
    )
    pending = await wait_for_pending(database, run.id)
    assert sandbox.requests == []

    await approvals.decide(
        pending.id,
        ApprovalStatus.APPROVED,
        pending.spec_digest,
        None,
        "approve-command",
    )
    result = (await execution)[0]

    assert sandbox.requests[0].command == pending.spec
    assert result.status is ToolResultStatus.SUCCEEDED
    assert result.artifact_id is not None
    assert result.result["exit_code"] == 0
    assert result.result["original_bytes"] == 8
    assert result.result["truncated"] is True
    _, content = await artifacts.read(result.artifact_id)
    summary = json.loads(content.splitlines()[-1])
    assert summary == {
        "type": "summary",
        "original_bytes": 8,
        "retained_bytes": 2,
        "truncated": True,
    }


async def test_denial_never_reaches_sandbox(database: Database, tmp_path: Path) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "command-denied")
        await uow.commit()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    broker = InMemoryApprovalBroker()
    approvals = ApprovalService(factory, CLOCK, broker)
    resources = TaskResourceManager(FakeDockerClient(), factory, CLOCK)
    await resources.ensure_dependency_volume(task.id)
    sandbox = FakeSandboxBackend(
        SandboxOutcome(
            0,
            SandboxTermination.COMPLETED,
            NOW,
            NOW,
            "image",
            "container",
            0,
            0,
            False,
        )
    )
    tool = ExecuteCommandTool(
        CommandAuthority(approvals, broker, CLOCK),
        sandbox,
        resources,
        ArtifactService(LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory),
    )
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, CLOCK)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    execution = asyncio.create_task(
        dispatcher.execute_batch(
            DispatchContext(task.id, run.id, step.id, message.id, workspace),
            (
                CompleteToolCall(
                    new_id(),
                    "execute_command",
                    {
                        "executable": "true",
                        "image": "runner@sha256:" + "a" * 64,
                        "reason": "test",
                    },
                ),
            ),
        )
    )
    pending = await wait_for_pending(database, run.id)
    await approvals.decide(
        pending.id,
        ApprovalStatus.DENIED,
        pending.spec_digest,
        "not allowed",
        "deny-command",
    )

    result = (await execution)[0]
    assert result.status is ToolResultStatus.DENIED
    assert result.error_code == "approval_denied"
    assert sandbox.requests == []
