from pathlib import Path

from crucible.application.artifact_service import ArtifactService
from crucible.artifacts.store import LocalArtifactStore
from crucible.context.compaction import (
    CompactionLifecycle,
    CompactionSummary,
    ScriptedCompactionGateway,
    group_conversation_units,
    select_compaction_boundary,
)
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.storage.database import Database
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


async def completed_messages(factory, run, assistant_text: str = "done"):
    async with factory() as uow:
        await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                None,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (MessagePart(new_id(), 1, MessagePartKind.TEXT, assistant_text),),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.commit()
    async with factory() as uow:
        return await uow.messages.list_for_task(run.task_id)


def summary(objective: str) -> CompactionSummary:
    return CompactionSummary(
        objective_and_constraints=objective,
        decisions="none",
        repository_facts="fixture",
        changes="none",
        commands_and_validation="none",
        unresolved_problems="none",
        execution_state="active",
        important_paths_and_symbols="README.md",
        input_tokens=10,
        output_tokens=5,
    )


async def test_successful_compaction_persists_private_artifact_and_lineage(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "compaction-success")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    messages = await completed_messages(factory, run)
    unit = group_conversation_units(messages, {})
    gateway = ScriptedCompactionGateway((summary("explain the change"),))
    lifecycle = CompactionLifecycle(
        factory,
        ArtifactService(
            LocalArtifactStore(tmp_path / "artifacts", FixedClock()), factory
        ),
        gateway,
        FixedClock(),
    )
    boundary = select_compaction_boundary(unit, retain_complete_units=0)
    assert boundary is not None

    compacted = await lifecycle.compact(run, boundary)

    async with factory() as uow:
        stored = await uow.compactions.get(compacted.id)
        artifact = await uow.artifacts.get(compacted.summary_artifact_id)
        events = await uow.events.list_after(run.task_id, 0)
    assert stored == compacted
    assert artifact is not None and artifact.sensitivity == "private"
    assert compacted.rendered_summary.startswith("## User objective and constraints")
    assert gateway.requests[0].previous_compaction_id is None
    assert [event.type for event in events][-2:] == [
        "compaction.started",
        "compaction.completed",
    ]


async def test_compaction_retries_within_budget(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "compaction-retry")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    messages = await completed_messages(factory, run)
    boundary = select_compaction_boundary(
        group_conversation_units(messages, {}), retain_complete_units=0
    )
    assert boundary is not None
    gateway = ScriptedCompactionGateway(
        (RuntimeError("temporary"), summary("recovered"))
    )
    lifecycle = CompactionLifecycle(
        factory,
        ArtifactService(
            LocalArtifactStore(tmp_path / "retry-artifacts", FixedClock()), factory
        ),
        gateway,
        FixedClock(),
        attempt_limit=2,
    )

    compacted = await lifecycle.compact(run, boundary)

    assert "recovered" in compacted.rendered_summary
    assert len(gateway.requests) == 2


async def test_later_compaction_records_previous_summary_lineage(
    database: Database, tmp_path: Path
) -> None:
    submitted = await queued_run(database, tmp_path, "compaction-lineage")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
    messages = await completed_messages(factory, run)
    first_boundary = select_compaction_boundary(
        group_conversation_units(messages, {}), retain_complete_units=0
    )
    assert first_boundary is not None
    gateway = ScriptedCompactionGateway((summary("first"), summary("second")))
    lifecycle = CompactionLifecycle(
        factory,
        ArtifactService(
            LocalArtifactStore(tmp_path / "lineage-artifacts", FixedClock()), factory
        ),
        gateway,
        FixedClock(),
    )
    first = await lifecycle.compact(run, first_boundary)
    async with factory() as uow:
        await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                None,
                0,
                MessageRole.USER,
                MessageStatus.COMPLETED,
                (MessagePart(new_id(), 1, MessagePartKind.TEXT, "continue"),),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                None,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (MessagePart(new_id(), 1, MessagePartKind.TEXT, "continued"),),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.commit()
        messages = await uow.messages.list_for_task(run.task_id)
    second_boundary = select_compaction_boundary(
        group_conversation_units(messages, {}), retain_complete_units=0
    )
    assert second_boundary is not None

    second = await lifecycle.compact(run, second_boundary)

    assert second.previous_compaction_id == first.id
    assert gateway.requests[-1].previous_compaction_id == first.id
    assert (
        "## User objective"
        in gateway.requests[-1].source_units[0].messages[0].parts[0].text_content
    )
    assert len(gateway.requests[-1].source_units) == 2
