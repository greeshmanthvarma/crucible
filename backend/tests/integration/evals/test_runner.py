import asyncio
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from alembic import command
from alembic.config import Config

from crucible.application.container import ApplicationContainer
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelStop,
    ModelStopReason,
    TextDelta,
)
from crucible.evals.evaluators import EvaluatorRegistry
from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import (
    EvalPartition,
    EvaluatorDefinition,
    load_suite,
)
from crucible.evals.runner import (
    ApprovalChoice,
    EvalRunner,
    TrialRequest,
    configuration_digest,
)
from crucible.sandbox.docker_client import VolumeInfo


class FakeDockerClient:
    def __init__(self) -> None:
        self.volumes: dict[str, VolumeInfo] = {}

    async def create_volume(self, name, labels):
        value = VolumeInfo(name, labels)
        self.volumes[name] = value
        return value

    async def inspect_volume(self, identity):
        return self.volumes.get(identity)

    async def list_volumes(self, label):
        return tuple(self.volumes.values())

    async def list_containers(self, label):
        return ()


class CommandRequestGateway:
    def __init__(self) -> None:
        self.requests = 0

    async def stream(self, request):
        self.requests += 1
        if self.requests == 1:
            yield CompleteToolCall(
                new_id(),
                "execute_command",
                {
                    "executable": "python",
                    "arguments": ["-V"],
                    "cwd": ".",
                    "image": "python@sha256:" + "a" * 64,
                    "reason": "Verify interpreter",
                    "network": "none",
                },
            )
            yield ModelStop(ModelStopReason.TOOL_CALLS)
        else:
            yield TextDelta("Done")
            yield ModelStop(ModelStopReason.COMPLETE)


class ErrorGateway:
    async def stream(self, request):
        yield ModelError("provider_down", "unavailable")


async def test_runner_uses_ordinary_task_message_run_and_fresh_fixture(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    database_url = f"sqlite+aiosqlite:///{data_dir / 'crucible.db'}"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    suite = load_suite(EvalPartition.DEVELOPMENT, "smoke")
    case = replace(
        suite.cases[0],
        evaluators=(EvaluatorDefinition("required_file", {"path": "README.md"}),),
    )
    suite = replace(suite, cases=(case,))
    container = await ApplicationContainer.create(
        database_url, data_dir, docker_client=FakeDockerClient()
    )
    await container.start()
    try:
        runner = EvalRunner(
            container,
            suite,
            FixturePreparer(data_dir),
            EvaluatorRegistry(data_dir=data_dir, artifacts=container.artifact_service),
        )
        request = TrialRequest(
            "smoke",
            "tiny",
            1,
            EvalPartition.DEVELOPMENT,
            configuration_digest(case),
            uuid4(),
        )
        result = await runner.run_trial(request)
        assert result.verdict == "passed"
        async with container.unit_of_work() as uow:
            trial = (await uow.evals.list_trials(request.invocation_id))[0]
            run = await uow.runs.get(trial.run_id)
            task = await uow.tasks.get(trial.task_id)
            messages = await uow.messages.list_for_task(trial.task_id)
            summary = await uow.evals.get_summary(trial.run_id)
        assert run is not None and run.status == "completed"
        assert task is not None and task.repository_id == trial.repository_id
        assert messages[0].role == "user"
        assert summary is not None and summary.projection["raw_trace_ids"][
            "run"
        ] == str(run.id)
        assert trial.fixture_commit == case.fixture_revision
    finally:
        await container.close()


async def test_pending_command_requires_human_denial_and_records_tool_result(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    database_url = f"sqlite+aiosqlite:///{data_dir / 'crucible.db'}"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    suite = load_suite(EvalPartition.DEVELOPMENT, "smoke")
    case = replace(
        suite.cases[0],
        evaluators=(EvaluatorDefinition("required_file", {"path": "README.md"}),),
    )
    suite = replace(suite, cases=(case,))
    gateway = CommandRequestGateway()
    container = await ApplicationContainer.create(
        database_url,
        data_dir,
        docker_client=FakeDockerClient(),
        gateway_factory=lambda: gateway,
    )
    await container.start()
    decisions = []

    async def deny(approval):
        async with container.unit_of_work() as uow:
            assert not any(
                item.tool_call_id == approval.tool_call_id
                for item in await uow.tool_results.list_for_step(approval.step_id)
            )
        decisions.append(approval)
        return ApprovalChoice(ApprovalStatus.DENIED, "Not authorized", "human-denial")

    try:
        runner = EvalRunner(
            container,
            suite,
            FixturePreparer(data_dir),
            EvaluatorRegistry(data_dir=data_dir, artifacts=container.artifact_service),
            deny,
        )
        request = TrialRequest(
            "smoke",
            "tiny",
            1,
            EvalPartition.DEVELOPMENT,
            configuration_digest(case),
            uuid4(),
        )
        result = await runner.run_trial(request)
        async with container.unit_of_work() as uow:
            trial = (await uow.evals.list_trials(request.invocation_id))[0]
            trace = await container.task_service.trace(trial.task_id)
        assert result.verdict == "passed"
        assert len(decisions) == 1
        assert decisions[0].spec_digest == decisions[0].spec.digest
        assert any(item.status == "denied" for step in trace for item in step.results)
    finally:
        await container.close()


async def test_model_failure_keeps_failed_trial_and_trace(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    database_url = f"sqlite+aiosqlite:///{data_dir / 'crucible.db'}"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    suite = load_suite(EvalPartition.DEVELOPMENT, "smoke")
    container = await ApplicationContainer.create(
        database_url,
        data_dir,
        docker_client=FakeDockerClient(),
        gateway_factory=ErrorGateway,
    )
    await container.start()
    try:
        runner = EvalRunner(
            container,
            suite,
            FixturePreparer(data_dir),
            EvaluatorRegistry(data_dir=data_dir, artifacts=container.artifact_service),
        )
        request = TrialRequest(
            "smoke",
            "tiny",
            1,
            EvalPartition.DEVELOPMENT,
            configuration_digest(suite.cases[0]),
            uuid4(),
        )
        result = await runner.run_trial(request)
        async with container.unit_of_work() as uow:
            trial = (await uow.evals.list_trials(request.invocation_id))[0]
            summary = await uow.evals.get_summary(trial.run_id)
        assert result.verdict == "failed"
        assert trial.status == "failed"
        assert summary is not None and summary.projection["outcome"] == "failed"
    finally:
        await container.close()
