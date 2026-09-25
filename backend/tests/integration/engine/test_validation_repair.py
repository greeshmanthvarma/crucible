import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest

from crucible.application.message_service import MessageService, MessageSubmissionKind
from crucible.application.validation_service import ValidationService
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.repository import RepositorySettings
from crucible.domain.tools import ToolResultStatus
from crucible.domain.validation import ValidationStatus
from crucible.engine.gateway import (
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)
from crucible.engine.run_engine import RunEngine
from crucible.storage.database import Database
from crucible.tools.definitions import ToolContext, ToolOutcome
from crucible.tools.dispatcher import ToolDispatcher
from crucible.tools.registry import ToolRegistry
from tests.integration.application.test_validation_service import (
    ScriptedValidationTool,
)
from tests.integration.engine.conftest import (
    FixedClock,
    PassiveSupervisor,
    queued_run,
    uow_factory,
)


class RepairGateway:
    def __init__(
        self,
        replies: tuple[str, ...],
        *,
        usage: tuple[int, ...] = (),
        repair_delay: float = 0,
    ) -> None:
        self.replies = replies
        self.usage = usage
        self.repair_delay = repair_delay
        self.requests: list[PreparedModelRequest] = []

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        reply = self.replies[len(self.requests)]
        self.requests.append(request)
        if len(self.requests) > 1 and self.repair_delay:
            await asyncio.sleep(self.repair_delay)
        yield TextDelta(reply)
        if self.usage:
            yield ModelUsage(0, self.usage[len(self.requests) - 1])
        yield ModelStop(ModelStopReason.COMPLETE)


class CancellingValidationTool(ScriptedValidationTool):
    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        raise asyncio.CancelledError


def validation_command() -> CommandSpec:
    return CommandSpec(
        "python",
        ("-m", "pytest"),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        "runner@sha256:" + "a" * 64,
        "Authoritative tests",
        CommandLimits(1, 1024**3, 64, 100_000),
    )


async def configured_run(database: Database, tmp_path: Path, *, repairs: int):
    submitted = await queued_run(database, tmp_path, f"validation-repair-{repairs}")
    factory = uow_factory(database)
    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        assert run is not None
        await uow.runs.update(
            replace(
                run,
                settings_snapshot=RepositorySettings(
                    validation_commands=(validation_command(),),
                    validation_repair_limit=repairs,
                ),
            )
        )
        await uow.commit()
    return submitted, factory


async def test_failed_validation_is_repaired_inside_the_same_run(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=1)
    tool = ScriptedValidationTool(
        (ToolResultStatus.FAILED, ToolResultStatus.SUCCEEDED), factory
    )
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())
    gateway = RepairGateway(("first completion", "repaired completion"))

    assert await RunEngine(
        factory,
        FixedClock(),
        gateway,
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
        runs = await uow.runs.list_for_task(run.task_id) if run else ()
    assert run is not None and run.status.value == "completed"
    assert [attempt.status for attempt in attempts] == [
        ValidationStatus.FAILED,
        ValidationStatus.PASSED,
    ]
    assert len(runs) == 1
    repair_context = gateway.requests[1].messages
    assert any(
        message.role.value == "system"
        and "Validation repair evidence" in (message.parts[0].text_content or "")
        and "failed" in (message.parts[0].text_content or "")
        for message in repair_context
    )


async def test_validation_passes_without_entering_repair(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=2)
    tool = ScriptedValidationTool((ToolResultStatus.SUCCEEDED,), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())
    gateway = RepairGateway(("complete",))

    assert await RunEngine(
        factory,
        FixedClock(),
        gateway,
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.status.value == "completed"
    assert [attempt.status for attempt in attempts] == [ValidationStatus.PASSED]
    assert len(gateway.requests) == 1


async def test_repeated_validation_failure_exhausts_repairs_and_next_message_starts_run(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=1)
    tool = ScriptedValidationTool(
        (ToolResultStatus.FAILED, ToolResultStatus.FAILED), factory
    )
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())

    assert await RunEngine(
        factory,
        FixedClock(),
        RepairGateway(("first completion", "still broken")),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.outcome_code == "validation_failed"
    assert str(attempts[-1].id) in (run.outcome_detail or "")
    assert [attempt.status for attempt in attempts] == [
        ValidationStatus.FAILED,
        ValidationStatus.FAILED,
    ]

    follow_up = await MessageService(factory, FixedClock(), PassiveSupervisor()).submit(
        run.task_id, "Try a different fix", "post-validation-failure"
    )
    assert follow_up.kind is MessageSubmissionKind.NEW_RUN
    assert follow_up.run_id != run.id


async def test_denied_validation_is_terminal_without_repair(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=3)
    tool = ScriptedValidationTool((ToolResultStatus.DENIED,), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())
    gateway = RepairGateway(("complete",))

    assert await RunEngine(
        factory,
        FixedClock(),
        gateway,
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.outcome_code == "validation_failed"
    assert [attempt.status for attempt in attempts] == [ValidationStatus.DENIED]
    assert len(gateway.requests) == 1


async def test_model_token_budget_exhaustion_during_repair_is_terminal(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=1)
    tool = ScriptedValidationTool((ToolResultStatus.FAILED,), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())

    assert await RunEngine(
        factory,
        FixedClock(),
        RepairGateway(("complete", "repair"), usage=(5, 6)),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
        max_model_tokens=10,
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
    assert run is not None and run.outcome_code == "budget_exhausted"


async def test_active_time_budget_exhaustion_during_repair_is_terminal(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=1)
    tool = ScriptedValidationTool((ToolResultStatus.FAILED,), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())

    assert await RunEngine(
        factory,
        FixedClock(),
        RepairGateway(("complete", "repair"), repair_delay=0.02),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
        max_active_seconds=0.01,
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
    assert run is not None and run.outcome_code == "budget_exhausted"


async def test_tool_budget_exhaustion_during_repair_is_terminal(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=2)
    tool = ScriptedValidationTool(
        (ToolResultStatus.FAILED, ToolResultStatus.SUCCEEDED), factory
    )
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())

    assert await RunEngine(
        factory,
        FixedClock(),
        RepairGateway(("complete", "repair")),
        dispatcher=dispatcher,
        validation_service=ValidationService(factory, FixedClock(), dispatcher),
        max_tool_calls=1,
    ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.outcome_code == "budget_exhausted"
    assert [attempt.status for attempt in attempts] == [ValidationStatus.FAILED]


async def test_cancellation_during_validation_terminalizes_attempt_and_run(
    database: Database, tmp_path: Path
) -> None:
    submitted, factory = await configured_run(database, tmp_path, repairs=1)
    tool = CancellingValidationTool((), factory)
    dispatcher = ToolDispatcher(ToolRegistry((tool,)), factory, FixedClock())

    with pytest.raises(asyncio.CancelledError):
        await RunEngine(
            factory,
            FixedClock(),
            RepairGateway(("complete",)),
            dispatcher=dispatcher,
            validation_service=ValidationService(factory, FixedClock(), dispatcher),
        ).execute(submitted.run_id)

    async with factory() as uow:
        run = await uow.runs.get(submitted.run_id)
        attempts = await uow.validation_attempts.list_for_run(submitted.run_id)
    assert run is not None and run.outcome_code == "cancelled"
    assert attempts[-1].status is ValidationStatus.CANCELLED
