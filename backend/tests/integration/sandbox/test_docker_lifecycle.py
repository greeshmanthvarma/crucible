import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.domain.tools import ToolCall, ToolExecutionMode
from crucible.sandbox.docker import DockerSandboxBackend
from crucible.sandbox.docker_client import SubprocessDockerClient
from crucible.sandbox.protocol import SandboxRequest, SandboxTermination
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


@pytest.mark.docker
async def test_real_docker_container_is_removed_after_harmless_command(
    database: Database, tmp_path: Path
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is unavailable")
    info = subprocess.run(
        ["docker", "info"], capture_output=True, text=True, check=False
    )
    if info.returncode != 0:
        pytest.skip(f"Docker daemon is unavailable: {info.stderr.strip()[:200]}")
    image = os.environ.get("CRUCIBLE_DOCKER_TEST_IMAGE")
    if not image:
        pytest.skip("CRUCIBLE_DOCKER_TEST_IMAGE is not configured")
    if "@sha256:" not in image:
        pytest.fail("CRUCIBLE_DOCKER_TEST_IMAGE must be pinned by sha256 digest")
    inspected = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        text=True,
        check=False,
    )
    if inspected.returncode != 0:
        pytest.skip(f"Configured Docker image is unavailable: {image}")

    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "docker-lifecycle")
        call_id = new_id()
        await uow.tool_calls.add(
            ToolCall(
                call_id,
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
    workspace = tmp_path / "workspace"
    workspace.mkdir(mode=0o755)
    factory = lambda: SqlAlchemyUnitOfWork(database)  # noqa: E731
    docker = SubprocessDockerClient()
    backend = DockerSandboxBackend(docker, factory, CLOCK)
    outcome = await backend.execute(
        SandboxRequest(
            task.id,
            run.id,
            call_id,
            workspace,
            CommandSpec(
                "/bin/true",
                (),
                ".",
                30,
                CommandNetwork.NONE,
                {},
                image,
                "Docker lifecycle smoke",
                CommandLimits(1, 256 * 1024**2, 32, 1000),
            ),
            (),
        ),
        lambda _chunk: None,
    )

    assert outcome.termination is SandboxTermination.COMPLETED
    assert outcome.exit_code == 0
    assert await docker.inspect_container(outcome.container_id) is None
