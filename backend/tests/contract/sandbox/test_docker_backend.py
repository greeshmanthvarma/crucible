import inspect
from datetime import UTC, datetime
from pathlib import Path

from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.tools import ToolCall, ToolExecutionMode
from crucible.sandbox.docker import DockerSandboxBackend
from crucible.sandbox.docker_client import ContainerInfo
from crucible.sandbox.protocol import (
    OutputChunk,
    OutputStream,
    SandboxRequest,
    SandboxTermination,
)
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


class FakeDocker:
    def __init__(self) -> None:
        self.create_args: tuple[str, ...] = ()
        self.cleaned: list[tuple[str, str]] = []
        self.chunks: tuple[OutputChunk, ...] = ()

    async def create_container(self, arguments: tuple[str, ...]) -> str:
        self.create_args = arguments
        return "container-id"

    async def start_attached(self, container_id, on_chunk) -> int:
        for chunk in self.chunks:
            result = on_chunk(chunk)
            if inspect.isawaitable(result):
                await result
        return 0

    async def inspect_container(self, container_id: str) -> ContainerInfo | None:
        return ContainerInfo(container_id, "sha256:" + "b" * 64, False, {})

    async def stop_container(self, container_id: str, grace_seconds: int) -> None:
        self.cleaned.append(("stop", container_id))

    async def kill_container(self, container_id: str) -> None:
        self.cleaned.append(("kill", container_id))

    async def remove_container(self, container_id: str) -> None:
        self.cleaned.append(("remove", container_id))

    async def list_containers(self, label: str):
        return ()


async def test_create_uses_exact_hardening_and_default_no_network(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "docker-policy")
        tool_call_id = new_id()
        await uow.tool_calls.add(
            ToolCall(
                tool_call_id,
                task.id,
                run.id,
                step.id,
                message.id,
                1,
                "execute_command",
                {},
                1,
                None,
                ToolExecutionMode.SEQUENTIAL,
                NOW,
            )
        )
        await uow.commit()
    command = CommandSpec(
        "python",
        ("-V",),
        ".",
        30,
        CommandNetwork.NONE,
        {"CI": "1"},
        "runner@sha256:" + "a" * 64,
        "version",
        CommandLimits(2, 2 * 1024**3, 256, 10_000),
    )
    request = SandboxRequest(
        task.id, run.id, tool_call_id, Path("/work/task"), command, ()
    )
    docker = FakeDocker()
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    backend = DockerSandboxBackend(docker, factory, CLOCK)

    outcome = await backend.execute(request, lambda chunk: None)

    args = docker.create_args
    assert args[0:3] == (
        "create",
        "--name",
        f"crucible-command-{tool_call_id}",
    )
    assert ("--read-only", "--user", "65532:65532") == args[3:6]
    assert args[args.index("--network") + 1] == "none"
    assert args[args.index("--cap-drop") + 1] == "ALL"
    assert args[args.index("--security-opt") + 1] == "no-new-privileges"
    assert args[args.index("--cpus") + 1] == "2.0"
    assert args[args.index("--memory") + 1] == str(2 * 1024**3)
    assert args[args.index("--pids-limit") + 1] == "256"
    assert f"type=bind,src={request.workspace_path},dst=/workspace" in args
    assert args[-3:] == (command.image, "python", "-V")
    assert outcome.termination is SandboxTermination.COMPLETED
    assert docker.cleaned == [("stop", "container-id"), ("remove", "container-id")]

    async with SqlAlchemyUnitOfWork(database) as uow:
        resources = await uow.external_resources.list_for_task(task.id)
    assert resources[0].external_identity == "container-id"
    assert resources[0].status == "removed"
    await backend.reconcile(resources)
    async with SqlAlchemyUnitOfWork(database) as uow:
        after_reconcile = await uow.external_resources.list_for_task(task.id)
    assert after_reconcile[0].status == "removed"


async def test_output_limit_stops_and_removes_container(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "docker-output-limit")
        tool_call_id = new_id()
        await uow.tool_calls.add(
            ToolCall(
                tool_call_id,
                task.id,
                run.id,
                step.id,
                message.id,
                1,
                "execute_command",
                {},
                1,
                None,
                ToolExecutionMode.SEQUENTIAL,
                NOW,
            )
        )
        await uow.commit()
    command = CommandSpec(
        "printf",
        ("overflow",),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        "runner@sha256:" + "a" * 64,
        "bounded output",
        CommandLimits(1, 1024, 16, 4),
    )
    docker = FakeDocker()
    docker.chunks = (OutputChunk(OutputStream.STDOUT, 1, b"overflow"),)
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    backend = DockerSandboxBackend(docker, factory, CLOCK)
    retained: list[bytes] = []

    outcome = await backend.execute(
        SandboxRequest(
            task.id,
            run.id,
            tool_call_id,
            Path("/work/task"),
            command,
            (),
        ),
        lambda chunk: retained.append(chunk.data),
    )

    assert retained == [b"over"]
    assert outcome.termination is SandboxTermination.OUTPUT_LIMIT
    assert outcome.truncated
    assert docker.cleaned[-1] == ("remove", "container-id")
