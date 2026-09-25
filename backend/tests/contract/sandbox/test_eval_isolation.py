from pathlib import Path

from crucible.domain.ids import new_id
from crucible.sandbox.docker import DockerSandboxBackend
from crucible.sandbox.protocol import EvalSandboxRequest
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.contract.sandbox.test_docker_backend import CLOCK, FakeDocker
from tests.integration.storage.test_tool_loop_storage import seed_exchange


async def test_evaluator_mounts_are_separate_read_only_and_no_network(
    database: Database, tmp_path: Path
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, _, _ = await seed_exchange(uow, "eval-isolation")
        await uow.commit()
    source = tmp_path / "source"
    tests = tmp_path / "hidden-tests"
    source.mkdir()
    tests.mkdir()
    docker = FakeDocker()
    backend = DockerSandboxBackend(
        docker, lambda: SqlAlchemyUnitOfWork(database), CLOCK
    )
    request = EvalSandboxRequest(
        task.id,
        run.id,
        new_id(),
        source,
        tests,
        "python@sha256:" + "a" * 64,
        "python",
        ("/tests/check.py",),
        30,
    )
    await backend.evaluate(request, lambda chunk: None)
    args = docker.create_args
    assert args[args.index("--network") + 1] == "none"
    assert "--read-only" in args
    assert f"type=bind,src={source},dst=/source,readonly" in args
    assert f"type=bind,src={tests},dst=/tests,readonly" in args
    assert not any("dst=/workspace" in item for item in args)
    assert docker.cleaned[-1] == ("remove", "container-id")
