from dataclasses import replace
from pathlib import Path

from crucible.application.validation_service import ValidationService
from crucible.domain.approvals import Approval
from crucible.domain.artifacts import Artifact
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.ids import new_id
from crucible.domain.repository import RepositorySettings
from crucible.domain.steps import Step
from crucible.domain.tools import ToolResultStatus
from crucible.domain.validation import CompletionProposal, ValidationStatus
from crucible.engine.gateway import ModelToolDefinition
from crucible.storage.database import Database
from crucible.tools.command import ExecuteCommandArguments
from crucible.tools.definitions import ToolContext, ToolOutcome
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import ToolRegistry
from tests.integration.engine.conftest import FixedClock, queued_run, uow_factory


class ScriptedValidationTool:
    parallel_safe = False
    argument_model = ExecuteCommandArguments
    definition = ModelToolDefinition(
        "execute_command", "validation", ExecuteCommandArguments.model_json_schema()
    )

    def __init__(self, statuses: tuple[ToolResultStatus, ...], factory) -> None:
        self.statuses = list(statuses)
        self.factory = factory
        self.arguments: list[object] = []

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        self.arguments.append(arguments)
        status = self.statuses.pop(0)
        assert (
            context.task_id
            and context.run_id
            and context.step_id
            and context.tool_call_id
        )
        approval_id, artifact_id = new_id(), new_id()
        approval = Approval.requested(
            approval_id,
            context.task_id,
            context.run_id,
            context.step_id,
            context.tool_call_id,
            CommandSpec.from_dict(arguments),
            FixedClock().now(),
        ).approve("tester", FixedClock().now())
        artifact = Artifact(
            artifact_id,
            context.task_id,
            artifact_id.hex.ljust(64, "0"),
            "text/plain",
            0,
            f"fake/{artifact_id}",
            "private",
            {},
            FixedClock().now(),
        )
        async with self.factory() as uow:
            await uow.approvals.add(approval)
            await uow.artifacts.add(artifact)
            await uow.commit()
        return ToolOutcome(
            {
                "approval_id": str(approval_id),
                "artifact_id": str(artifact_id),
                "exit_code": 0 if status is ToolResultStatus.SUCCEEDED else 1,
            },
            status.value,
            status=status,
            error_code=None
            if status is ToolResultStatus.SUCCEEDED
            else "validation_failed",
        )


def command(name: str) -> CommandSpec:
    return CommandSpec(
        "python",
        ("-m", name),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        "runner@sha256:" + "a" * 64,
        f"Run {name}",
        CommandLimits(1, 1024**3, 64, 100_000),
    )


async def prepared_run(
    database: Database, tmp_path: Path, name: str, commands: tuple[CommandSpec, ...]
):
    submitted = await queued_run(database, tmp_path, name)
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        run = replace(
            run, settings_snapshot=RepositorySettings(validation_commands=commands)
        )
        await uow.runs.update(run)
        task = await uow.tasks.get(run.task_id)
        assert task is not None
        repository = await uow.repositories.get(task.repository_id)
        assert repository is not None
        await uow.repositories.update(
            replace(
                repository,
                settings=RepositorySettings(
                    validation_commands=(command("configured-after-run-created"),)
                ),
            )
        )
        step = (
            Step.preparing(new_id(), run.task_id, run.id, 1, FixedClock().now())
            .activate_model(FixedClock().now())
            .complete(FixedClock().now())
        )
        await uow.steps.add(step)
        message = await uow.messages.add(
            Message(
                new_id(),
                run.task_id,
                run.id,
                step.id,
                0,
                MessageRole.ASSISTANT,
                MessageStatus.COMPLETED,
                (
                    MessagePart(
                        new_id(), 1, MessagePartKind.TEXT, "model claims success"
                    ),
                ),
                FixedClock().now(),
                FixedClock().now(),
            )
        )
        await uow.commit()
    return factory, run, step, message


async def test_validation_uses_ordered_snapshotted_commands_and_harness_results(
    database: Database, tmp_path: Path
) -> None:
    commands = (command("pytest"), command("mypy"))
    factory, run, step, message = await prepared_run(
        database, tmp_path, "validation-pass", commands
    )
    tool = ScriptedValidationTool(
        (ToolResultStatus.SUCCEEDED, ToolResultStatus.FAILED), factory
    )
    service = ValidationService(
        factory,
        FixedClock(),
        ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock()),
    )
    await service.record_proposal(
        CompletionProposal(
            new_id(),
            run.id,
            message.id,
            "model claims success",
            (),
            None,
            FixedClock().now(),
        )
    )

    attempt = await service.validate(run.id)

    assert attempt.status is ValidationStatus.FAILED
    assert [item["arguments"] for item in tool.arguments] == [
        ["-m", "pytest"],
        ["-m", "mypy"],
    ]
    async with factory() as uow:
        results = await uow.validation_command_results.list_for_attempt(attempt.id)
    assert [result.status for result in results] == [
        ValidationStatus.PASSED,
        ValidationStatus.FAILED,
    ]
    assert all(
        result.approval_id and result.tool_call_id and result.artifact_id
        for result in results
    )
    assert len({result.approval_id for result in results}) == 2


async def test_empty_validation_configuration_is_explicit(
    database: Database, tmp_path: Path
) -> None:
    factory, run, _step, message = await prepared_run(
        database, tmp_path, "validation-empty", ()
    )
    service = ValidationService(
        factory, FixedClock(), ToolDispatcher(ToolRegistry(()), factory, FixedClock())
    )
    await service.record_proposal(
        CompletionProposal(
            new_id(), run.id, message.id, "done", (), None, FixedClock().now()
        )
    )

    attempt = await service.validate(run.id)

    assert attempt.status is ValidationStatus.NOT_CONFIGURED
