from collections.abc import AsyncIterable, AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from crucible.application.ports import UnitOfWork
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
from crucible.domain.ids import CompactionId, TaskId, ToolCallId, ToolResultId, new_id
from crucible.domain.run import Run


@dataclass(frozen=True)
class ConversationUnit:
    messages: tuple[Message, ...]
    complete: bool

    @property
    def start_sequence(self) -> int:
        return self.messages[0].conversation_sequence

    @property
    def end_sequence(self) -> int:
        return self.messages[-1].conversation_sequence


@dataclass(frozen=True)
class CompactionBoundary:
    source_start_sequence: int
    source_end_sequence: int
    retained_tail_start_sequence: int
    source_units: tuple[ConversationUnit, ...]


@dataclass(frozen=True)
class CompactionRequest:
    task_id: TaskId
    source_units: tuple[ConversationUnit, ...]
    previous_compaction_id: CompactionId | None
    model: str
    prompt_version: str


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
        sections = (
            ("User objective and constraints", self.objective_and_constraints),
            ("Settled decisions", self.decisions),
            ("Repository facts", self.repository_facts),
            ("Changes made", self.changes),
            ("Commands and Validation", self.commands_and_validation),
            ("Unresolved problems", self.unresolved_problems),
            ("Current execution state", self.execution_state),
            ("Important paths and symbols", self.important_paths_and_symbols),
        )
        return "\n\n".join(f"## {heading}\n{body}" for heading, body in sections)


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

    async def compact(self, run: Run, boundary: CompactionBoundary) -> Compaction:
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
                summary = await self._gateway.compact(request)
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


def group_conversation_units(
    messages: tuple[Message, ...], result_to_call: Mapping[ToolResultId, ToolCallId]
) -> tuple[ConversationUnit, ...]:
    grouped: list[list[Message]] = []
    for message in sorted(messages, key=lambda item: item.conversation_sequence):
        if message.role is MessageRole.USER or not grouped:
            grouped.append([])
        grouped[-1].append(message)
    return tuple(
        ConversationUnit(tuple(group), _is_complete(group, result_to_call))
        for group in grouped
    )


def _is_complete(
    messages: list[Message], result_to_call: Mapping[ToolResultId, ToolCallId]
) -> bool:
    if not messages or messages[0].role is not MessageRole.USER:
        return False
    if any(message.status is not MessageStatus.COMPLETED for message in messages):
        return False
    calls = {
        part.tool_call_id
        for message in messages
        for part in message.parts
        if part.kind is MessagePartKind.TOOL_CALL and part.tool_call_id is not None
    }
    results: set[ToolCallId] = set()
    for message in messages:
        for part in message.parts:
            result_id = part.tool_result_id
            if part.kind is MessagePartKind.TOOL_RESULT and result_id is not None:
                call_id = result_to_call.get(result_id)
                if call_id is not None:
                    results.add(call_id)
    has_assistant = any(message.role is MessageRole.ASSISTANT for message in messages)
    return has_assistant and calls <= results


def select_compaction_boundary(
    units: tuple[ConversationUnit, ...], *, retain_complete_units: int
) -> CompactionBoundary | None:
    if retain_complete_units < 0:
        raise ValueError("retained unit count cannot be negative")
    complete_prefix: list[ConversationUnit] = []
    for unit in units:
        if not unit.complete:
            break
        complete_prefix.append(unit)
    compact_count = len(complete_prefix) - retain_complete_units
    if compact_count <= 0:
        return None
    source = tuple(complete_prefix[:compact_count])
    retained_index = compact_count
    retained_start = (
        units[retained_index].start_sequence
        if retained_index < len(units)
        else source[-1].end_sequence + 1
    )
    return CompactionBoundary(
        source[0].start_sequence, source[-1].end_sequence, retained_start, source
    )
