from collections.abc import Callable
from typing import cast
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.run import RunStatus
from crucible.domain.tools import ToolResultStatus
from crucible.domain.validation import (
    CompletionProposal,
    ValidationAttempt,
    ValidationCommandResult,
    ValidationStatus,
)
from crucible.engine.gateway import CompleteToolCall
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher


class ValidationService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        dispatcher: ToolDispatcher,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._dispatcher = dispatcher
        self._events = EventFactory()

    async def record_proposal(self, proposal: CompletionProposal) -> None:
        async with self._unit_of_work() as uow:
            await uow.completion_proposals.add(proposal)
            await uow.commit()

    async def validate(self, run_id: UUID) -> ValidationAttempt:
        async with self._unit_of_work() as uow:
            run = await uow.runs.get(run_id)
            if run is None:
                raise ValueError(f"Run not found: {run_id}")
            task = await uow.tasks.get(run.task_id)
            proposal = await uow.completion_proposals.get_for_run(run.id)
            prior = await uow.validation_attempts.list_for_run(run.id)
        if task is None or proposal is None:
            raise ValueError("Validation requires a Task and Completion Proposal")
        message = None
        async with self._unit_of_work() as uow:
            message = await uow.messages.get(proposal.assistant_message_id)
        if message is None or message.step_id is None:
            raise ValueError("Completion Proposal message must belong to a Step")
        now = self._clock.now()
        attempt = ValidationAttempt(
            new_id(), run.id, len(prior) + 1, ValidationStatus.RUNNING, now, None
        )
        commands = run.settings_snapshot.validation_commands
        async with self._unit_of_work() as uow:
            await uow.validation_attempts.add(attempt)
            if run.status is RunStatus.RUNNING:
                await uow.runs.update(run.start_validation())
            await uow.events.append(
                self._events.create(
                    task_id=run.task_id,
                    run_id=run.id,
                    type=EventType.VALIDATION_STARTED,
                    payload={"attempt_id": str(attempt.id)},
                    created_at=now,
                )
            )
            await uow.commit()
        if not commands:
            return await self._finish(
                attempt, run.task_id, ValidationStatus.NOT_CONFIGURED
            )
        async with self._unit_of_work() as uow:
            for sequence, _command in enumerate(commands, 1):
                await uow.events.append(
                    self._events.create(
                        task_id=run.task_id,
                        run_id=run.id,
                        type=EventType.VALIDATION_PENDING_APPROVAL,
                        payload={
                            "attempt_id": str(attempt.id),
                            "command_sequence": sequence,
                        },
                        created_at=self._clock.now(),
                    )
                )
            await uow.commit()
        calls = tuple(
            CompleteToolCall(new_id(), "execute_command", command.as_dict())
            for command in commands
        )
        results = await self._dispatcher.execute_batch(
            DispatchContext(
                run.task_id,
                run.id,
                message.step_id,
                message.id,
                task.workspace_path,
            ),
            calls,
            call_budget=len(calls),
        )
        statuses: list[ValidationStatus] = []
        for sequence, result in enumerate(results, 1):
            status = _validation_status(result.status)
            statuses.append(status)
            approval_value = result.result.get("approval_id")
            artifact_value = result.result.get("artifact_id")
            record = ValidationCommandResult(
                new_id(),
                attempt.id,
                sequence,
                status,
                UUID(str(approval_value)) if approval_value else None,
                result.tool_call_id,
                result.artifact_id
                or (UUID(str(artifact_value)) if artifact_value else None),
                int(cast(int | str, result.result["exit_code"]))
                if result.result.get("exit_code") is not None
                else None,
                result.display_text,
                result.created_at,
                result.completed_at,
            )
            async with self._unit_of_work() as uow:
                await uow.validation_command_results.add(record)
                await uow.events.append(
                    self._events.create(
                        task_id=run.task_id,
                        run_id=run.id,
                        type=EventType.VALIDATION_COMMAND_FINISHED,
                        payload={
                            "attempt_id": str(attempt.id),
                            "command_sequence": sequence,
                            "status": status.value,
                        },
                        created_at=self._clock.now(),
                    )
                )
                await uow.commit()
        final = (
            ValidationStatus.PASSED
            if all(status is ValidationStatus.PASSED for status in statuses)
            else ValidationStatus.DENIED
            if any(status is ValidationStatus.DENIED for status in statuses)
            else ValidationStatus.FAILED
        )
        return await self._finish(attempt, run.task_id, final)

    async def _finish(
        self, attempt: ValidationAttempt, task_id: UUID, status: ValidationStatus
    ) -> ValidationAttempt:
        completed = attempt.finish(status, self._clock.now())
        async with self._unit_of_work() as uow:
            await uow.validation_attempts.update(completed)
            await uow.events.append(
                self._events.create(
                    task_id=task_id,
                    run_id=attempt.run_id,
                    type=EventType.VALIDATION_COMPLETED,
                    payload={"attempt_id": str(attempt.id), "status": status.value},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        return completed


def _validation_status(status: ToolResultStatus) -> ValidationStatus:
    if status is ToolResultStatus.SUCCEEDED:
        return ValidationStatus.PASSED
    if status is ToolResultStatus.DENIED:
        return ValidationStatus.DENIED
    if status is ToolResultStatus.CANCELLED:
        return ValidationStatus.CANCELLED
    if status is ToolResultStatus.INTERRUPTED:
        return ValidationStatus.INTERRUPTED
    return ValidationStatus.FAILED
