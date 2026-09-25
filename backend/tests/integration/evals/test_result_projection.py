import asyncio
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from alembic import command
from alembic.config import Config

from crucible.application.container import ApplicationContainer
from crucible.evals.evaluators import EvaluatorRegistry
from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import EvalPartition, EvaluatorDefinition, load_suite
from crucible.evals.runner import EvalRunner, TrialRequest, configuration_digest
from tests.integration.evals.test_runner import FakeDockerClient


async def test_result_report_artifact_survives_container_reopen(tmp_path: Path) -> None:
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
    invocation_id = uuid4()
    try:
        runner = EvalRunner(
            container,
            suite,
            FixturePreparer(data_dir),
            EvaluatorRegistry(data_dir=data_dir, artifacts=container.artifact_service),
        )
        result = await runner.run_trial(
            TrialRequest(
                "smoke",
                "tiny",
                1,
                EvalPartition.DEVELOPMENT,
                configuration_digest(case),
                invocation_id,
            )
        )
        assert result.report_artifact_id is not None
    finally:
        await container.close()
    restored = await ApplicationContainer.create(
        database_url, data_dir, docker_client=FakeDockerClient()
    )
    try:
        async with restored.unit_of_work() as uow:
            trial = (await uow.evals.list_trials(invocation_id))[0]
            stored = await uow.evals.get_result(trial.id)
        assert (
            stored is not None
            and stored.report_artifact_id == result.report_artifact_id
        )
        _, content = await restored.artifact_service.read(stored.report_artifact_id)
        report = json.loads(content)
        assert report["verdict"] == "passed"
        assert report["estimated_cost"] is None
        assert report["tokens"]["source"] == "estimated"
        assert report["raw_trace_ids"]["run"] == str(trial.run_id)
    finally:
        await restored.close()
