import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from crucible.application.ports import UnitOfWork
from crucible.context.compaction import (
    CompactionBoundary,
    CompactionLifecycle,
    group_conversation_units,
    select_compaction_boundary,
)
from crucible.context.instructions import load_root_instructions
from crucible.context.manifests import ContextManifest
from crucible.domain.clock import Clock
from crucible.domain.compaction import Compaction
from crucible.domain.conversation import Message, MessagePart
from crucible.domain.events import EventType
from crucible.domain.ids import ToolCallId, ToolResultId, new_id
from crucible.domain.run import Run
from crucible.domain.steps import Step
from crucible.domain.tools import ToolCall, ToolResult
from crucible.engine.gateway import (
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelToolDefinition,
    PreparedModelRequest,
)
from crucible.engine.journal import EventSpec, JournalMutation, RunJournal


class ContextLimitExceeded(Exception):
    pass


class CompactionNeeded(Exception):
    def __init__(
        self, boundary: CompactionBoundary, estimated_tokens: int, capacity: int
    ) -> None:
        super().__init__(
            f"Prepared context estimate {estimated_tokens} exceeds capacity "
            f"{capacity}; Compaction required"
        )
        self.boundary = boundary
        self.estimated_tokens = estimated_tokens
        self.capacity = capacity


class TokenEstimator(Protocol):
    def estimate(self, messages: tuple[ModelMessage, ...]) -> int: ...


class SimpleTokenEstimator:
    def estimate(self, messages: tuple[ModelMessage, ...]) -> int:
        characters = sum(
            len(part.text_content or "")
            for message in messages
            for part in message.parts
        )
        return max(1, (characters + 3) // 4)


@dataclass(frozen=True)
class PreparedContext:
    request: PreparedModelRequest
    manifest: ContextManifest


class ContextManager:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        estimator: TokenEstimator,
        *,
        harness_policy: str,
        tool_contract: str,
        model: str,
        input_limit: int,
        output_reserve: int,
        threshold: float = 0.8,
        tools: tuple[ModelToolDefinition, ...] = (),
        max_output_tokens: int | None = None,
        journal: RunJournal | None = None,
        recent_complete_units: int = 2,
        compaction_lifecycle: CompactionLifecycle | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._estimator = estimator
        self._harness_policy = harness_policy
        self._tool_contract = tool_contract
        self._model = model
        self._input_limit = input_limit
        self._output_reserve = output_reserve
        self._threshold = threshold
        self._tools = tools
        self._max_output_tokens = max_output_tokens or output_reserve
        self._journal = journal
        self._recent_complete_units = recent_complete_units
        self._compaction_lifecycle = compaction_lifecycle

    async def prepare(self, run: Run, step: Step) -> PreparedContext:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(run.task_id)
            messages = await uow.messages.list_for_task(run.task_id)
            calls = {
                call.id: call
                for candidate in await uow.runs.list_for_task(run.task_id)
                for candidate_step in await uow.steps.list_for_run(candidate.id)
                for call in await uow.tool_calls.list_for_step(candidate_step.id)
            }
            results = {
                result.id: result
                for candidate in await uow.runs.list_for_task(run.task_id)
                for candidate_step in await uow.steps.list_for_run(candidate.id)
                for result in await uow.tool_results.list_for_step(candidate_step.id)
            }
            latest_compaction = await uow.compactions.latest_for_task(run.task_id)
        if task is None:
            raise ValueError(f"Task not found: {run.task_id}")

        repository = load_root_instructions(task.workspace_path)
        instruction_texts = [self._harness_policy, self._tool_contract]
        instruction_digests = {
            "harness": _digest_text(self._harness_policy),
            "tool_contract": _digest_text(self._tool_contract),
        }
        if repository is not None:
            instruction_texts.append(repository.text)
            instruction_digests["repository"] = repository.digest

        visible_messages = (
            messages
            if latest_compaction is None
            else tuple(
                message
                for message in messages
                if message.conversation_sequence
                >= latest_compaction.retained_tail_start_sequence
            )
        )
        compacted_prefix = (
            []
            if latest_compaction is None
            else [
                ModelMessage(
                    ModelRole.SYSTEM,
                    (
                        ModelPart(
                            "text",
                            "Compacted Conversation context:\n"
                            + latest_compaction.rendered_summary,
                        ),
                    ),
                )
            ]
        )
        model_messages = tuple(
            [
                ModelMessage(ModelRole.SYSTEM, (ModelPart("text", text),))
                for text in instruction_texts
            ]
            + compacted_prefix
            + [
                ModelMessage(
                    ModelRole(message.role.value),
                    tuple(_model_part(part, calls, results) for part in message.parts),
                )
                for message in visible_messages
            ]
        )
        estimate = self._estimator.estimate(model_messages)
        capacity = int(self._input_limit * self._threshold) - self._output_reserve
        if estimate > capacity:
            result_to_call = {
                result.id: result.tool_call_id for result in results.values()
            }
            boundary = select_compaction_boundary(
                group_conversation_units(messages, result_to_call),
                retain_complete_units=self._recent_complete_units,
            )
            if boundary is not None:
                if self._compaction_lifecycle is not None:
                    try:
                        await self._compaction_lifecycle.compact(run, boundary)
                    except Exception as error:
                        absolute_capacity = self._input_limit - self._output_reserve
                        if estimate <= absolute_capacity:
                            return await self._persist_prepared(
                                run,
                                step,
                                model_messages,
                                messages,
                                instruction_digests,
                                estimate,
                                latest_compaction,
                            )
                        raise ContextLimitExceeded(
                            f"Compaction failed and context estimate {estimate} "
                            f"exceeds limit {absolute_capacity}: {error}"
                        ) from error
                    return await self.prepare(run, step)
                raise CompactionNeeded(boundary, estimate, capacity)
            raise ContextLimitExceeded(
                f"Prepared context estimate {estimate} exceeds capacity {capacity}"
            )
        return await self._persist_prepared(
            run,
            step,
            model_messages,
            messages,
            instruction_digests,
            estimate,
            latest_compaction,
        )

    async def _persist_prepared(
        self,
        run: Run,
        step: Step,
        model_messages: tuple[ModelMessage, ...],
        all_messages: tuple[Message, ...],
        instruction_digests: dict[str, str],
        estimate: int,
        latest_compaction: Compaction | None,
    ) -> PreparedContext:
        request = PreparedModelRequest(
            run_id=run.id,
            step_id=step.id,
            model=self._model,
            messages=model_messages,
            tools=self._tools,
            max_output_tokens=self._max_output_tokens,
        )
        manifest = ContextManifest(
            id=new_id(),
            task_id=run.task_id,
            run_id=run.id,
            step_id=step.id,
            model=self._model,
            parameters={"max_output_tokens": self._max_output_tokens},
            input_limit=self._input_limit,
            output_reserve=self._output_reserve,
            threshold=self._threshold,
            estimated_tokens=estimate,
            message_ids=[message.id for message in all_messages],
            part_ids=[part.id for message in all_messages for part in message.parts],
            instruction_digests=instruction_digests,
            tool_schema_digest=_tool_digest(self._tools),
            created_at=self._clock.now(),
            compaction_id=latest_compaction.id if latest_compaction else None,
        )
        if self._journal is None:
            async with self._unit_of_work() as uow:
                await uow.context_manifests.add(manifest)
                await uow.commit()
        else:

            async def add_manifest(uow: UnitOfWork) -> None:
                await uow.context_manifests.add(manifest)

            await self._journal.record(
                JournalMutation(
                    run.task_id,
                    run.id,
                    add_manifest,
                    (
                        EventSpec(
                            EventType.CONTEXT_PREPARED,
                            {"step_id": str(step.id), "manifest_id": str(manifest.id)},
                        ),
                    ),
                )
            )
        return PreparedContext(request, manifest)


def _digest_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _model_part(
    part: MessagePart,
    calls: Mapping[ToolCallId, ToolCall],
    results: Mapping[ToolResultId, ToolResult],
) -> ModelPart:
    call = calls.get(part.tool_call_id) if part.tool_call_id is not None else None
    result = (
        results.get(part.tool_result_id) if part.tool_result_id is not None else None
    )
    return ModelPart(
        part.kind.value,
        part.text_content or part.reasoning_content,
        tool_call_id=(
            call.id if call is not None else result.tool_call_id if result else None
        ),
        tool_name=call.name if call is not None else None,
        arguments=call.arguments if call is not None else None,
    )


def _tool_digest(tools: tuple[ModelToolDefinition, ...]) -> str:
    value = [
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": dict(tool.input_schema),
            "schema_version": tool.schema_version,
        }
        for tool in tools
    ]
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
