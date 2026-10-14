from collections.abc import AsyncIterable, AsyncIterator, Callable
from dataclasses import dataclass, replace
from typing import Any, Mapping, Protocol
from uuid import UUID

from crucible.application.ports import UnitOfWork
from crucible.context import summary_behavior
from crucible.context.selection import CompactionBoundary as CompactionBoundary
from crucible.context.selection import ConversationUnit as ConversationUnit
from crucible.context.selection import (
    group_conversation_units as group_conversation_units,
)
from crucible.context.selection import (
    select_compaction_boundary as select_compaction_boundary,
)
from crucible.domain.clock import Clock
from crucible.domain.compaction import Compaction
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import CompactionId, TaskId, new_id
from crucible.domain.run import Run


@dataclass(frozen=True)
class CompactionRequest:
    task_id: TaskId
    source_units: tuple[ConversationUnit, ...]
    previous_compaction_id: CompactionId | None
    model: str
    prompt_version: str
    run_id: UUID | None = None
    step_id: UUID | None = None
    capture_key: str = ""
    attempt_index: int = 0
    context_snapshot: Mapping[str, object] | None = None


@dataclass(frozen=True)
class CompactionSummary:
    objective_and_constraints: str
    decisions: str
    repository_facts: str
    changes: str
    commands_and_validation: str
    unresolved_problems: str
    execution_state: str
    important_paths_and_symbols: str
    input_tokens: int = 0
    output_tokens: int = 0

    def render(self) -> str:
        return summary_behavior.render_summary(self)


class CompactionGateway(Protocol):
    async def compact(self, request: CompactionRequest) -> CompactionSummary: ...


class ArtifactWriter(Protocol):
    async def put(
        self,
        task_id: TaskId,
        media_type: str,
        sensitivity: str,
        stream: AsyncIterable[bytes],
        *,
        hard_limit: int | None = None,
    ) -> Any: ...


class ScriptedCompactionGateway:
    def __init__(self, outcomes: tuple[CompactionSummary | Exception, ...]) -> None:
        self._outcomes = outcomes
        self.requests: list[CompactionRequest] = []

    async def compact(self, request: CompactionRequest) -> CompactionSummary:
        index = len(self.requests)
        if index >= len(self._outcomes):
            raise AssertionError("Unexpected Compaction request")
        self.requests.append(request)
        outcome = self._outcomes[index]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class CompactionLifecycle:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        artifacts: ArtifactWriter,
        gateway: CompactionGateway,
        clock: Clock,
        *,
        attempt_limit: int | None = None,
        model: str = "coding-model",
        prompt_version: str | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._artifacts = artifacts
        self._gateway = gateway
        self._clock = clock
        self._attempt_limit = attempt_limit
        self._model = model
        self._prompt_version = prompt_version
        self._events = EventFactory()

    async def compact(
        self,
        run: Run,
        boundary: CompactionBoundary,
        *,
        step_id: UUID | None = None,
        context_snapshot: Mapping[str, object] | None = None,
    ) -> Compaction:
        async with self._unit_of_work() as uow:
            previous = await uow.compactions.latest_for_task(run.task_id)
        source_units = boundary.source_units
        if previous is not None:
            summary_message = Message(
                id=new_id(),
                task_id=run.task_id,
                run_id=None,
                step_id=None,
                conversation_sequence=previous.source_end_sequence,
                role=MessageRole.SYSTEM,
                status=MessageStatus.COMPLETED,
                parts=(
                    MessagePart(
                        id=new_id(),
                        part_sequence=1,
                        kind=MessagePartKind.TEXT,
                        text_content=previous.rendered_summary,
                    ),
                ),
                created_at=previous.created_at,
                completed_at=previous.created_at,
            )
            source_units = (
                ConversationUnit((summary_message,), True),
                *(
                    unit
                    for unit in boundary.source_units
                    if unit.end_sequence > previous.source_end_sequence
                ),
            )
        model = run.settings_snapshot.compaction_model or self._model
        prompt_version = (
            self._prompt_version or run.settings_snapshot.compaction_prompt_version
        )
        attempt_limit = (
            self._attempt_limit
            if self._attempt_limit is not None
            else run.settings_snapshot.compaction_attempt_limit
        )
        if attempt_limit <= 0:
            raise ValueError("Compaction attempt limit must be positive")
        request = CompactionRequest(
            run.task_id,
            source_units,
            previous.id if previous else None,
            model,
            prompt_version,
            run_id=run.id,
            step_id=step_id,
            capture_key=str(new_id()),
            context_snapshot=context_snapshot,
        )
        async with self._unit_of_work() as uow:
            await uow.events.append(
                self._events.create(
                    task_id=run.task_id,
                    run_id=run.id,
                    type=EventType.COMPACTION_STARTED,
                    payload={
                        "source_start_sequence": boundary.source_start_sequence,
                        "source_end_sequence": boundary.source_end_sequence,
                    },
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()
        last_error: Exception | None = None
        for _attempt in range(attempt_limit):
            try:
                summary = await self._gateway.compact(
                    replace(request, attempt_index=_attempt)
                )
                break
            except Exception as error:
                last_error = error
        else:
            assert last_error is not None
            await self._record_failed(run, last_error)
            raise last_error
        try:
            rendered = summary.render()
            artifact = await self._artifacts.put(
                run.task_id, "text/markdown", "private", _one_chunk(rendered.encode())
            )
            compacted = Compaction(
                new_id(),
                run.task_id,
                boundary.source_start_sequence,
                boundary.source_end_sequence,
                boundary.retained_tail_start_sequence,
                artifact.id,
                rendered,
                previous.id if previous else None,
                model,
                {},
                prompt_version,
                summary.input_tokens,
                summary.output_tokens,
                len(rendered) // 4,
                self._clock.now(),
            )
            async with self._unit_of_work() as uow:
                await uow.compactions.add(compacted)
                await uow.events.append(
                    self._events.create(
                        task_id=run.task_id,
                        run_id=run.id,
                        type=EventType.COMPACTION_COMPLETED,
                        payload={
                            "compaction_id": str(compacted.id),
                            "summary_artifact_id": str(compacted.summary_artifact_id),
                        },
                        created_at=self._clock.now(),
                    )
                )
                await uow.commit()
            return compacted
        except Exception as error:
            await self._record_failed(run, error)
            raise

    async def _record_failed(self, run: Run, error: Exception) -> None:
        async with self._unit_of_work() as uow:
            await uow.events.append(
                self._events.create(
                    task_id=run.task_id,
                    run_id=run.id,
                    type=EventType.COMPACTION_FAILED,
                    payload={"error_type": type(error).__name__},
                    created_at=self._clock.now(),
                )
            )
            await uow.commit()


async def _one_chunk(value: bytes) -> AsyncIterator[bytes]:
    yield value
