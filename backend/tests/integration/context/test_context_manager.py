from pathlib import Path

from crucible.context.manager import ContextManager, SimpleTokenEstimator
from crucible.domain.ids import new_id
from crucible.domain.steps import Step
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def test_context_precedence_and_agents_digest_are_evidenced_per_step(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        task = await uow.tasks.get(run.task_id)
        assert task is not None
        (task.workspace_path / "AGENTS.md").write_text("repository instruction v1")
        first = Step.preparing(new_id(), task.id, run.id, 1, FixedClock().now())
        await uow.steps.add(first)
        await uow.commit()

    manager = ContextManager(
        factory,
        FixedClock(),
        SimpleTokenEstimator(),
        harness_policy="harness policy",
        tool_contract="tool contract",
        model="fixture",
        input_limit=10_000,
        output_reserve=500,
    )
    prepared_first = await manager.prepare(run, first)
    assert [
        message.parts[0].text_content for message in prepared_first.request.messages[:3]
    ] == [
        "harness policy",
        "tool contract",
        "repository instruction v1",
    ]

    (task.workspace_path / "AGENTS.md").write_text("repository instruction v2")
    async with factory() as uow:
        second = Step.preparing(new_id(), task.id, run.id, 2, FixedClock().now())
        await uow.steps.add(second)
        await uow.commit()
    prepared_second = await manager.prepare(run, second)

    assert (
        prepared_first.manifest.instruction_digests["repository"]
        != prepared_second.manifest.instruction_digests["repository"]
    )
    async with factory() as uow:
        assert await uow.context_manifests.get_for_step(first.id) is not None
        assert await uow.context_manifests.get_for_step(second.id) is not None
