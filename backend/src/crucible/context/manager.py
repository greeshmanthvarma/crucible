import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from crucible.application.ports import UnitOfWork
from crucible.context.instructions import load_root_instructions
from crucible.context.manifests import ContextManifest
from crucible.domain.clock import Clock
from crucible.domain.ids import new_id
from crucible.domain.run import Run
from crucible.domain.steps import Step
from crucible.engine.gateway import (
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelToolDefinition,
    PreparedModelRequest,
)


class ContextLimitExceeded(Exception):
    pass


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

    async def prepare(self, run: Run, step: Step) -> PreparedContext:
        async with self._unit_of_work() as uow:
            task = await uow.tasks.get(run.task_id)
            messages = await uow.messages.list_for_task(run.task_id)
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

        model_messages = tuple(
            [
                ModelMessage(ModelRole.SYSTEM, (ModelPart("text", text),))
                for text in instruction_texts
            ]
            + [
                ModelMessage(
                    ModelRole(message.role.value),
                    tuple(
                        ModelPart(
                            part.kind.value,
                            part.text_content or part.reasoning_content,
                            part.tool_call_id,
                        )
                        for part in message.parts
                    ),
                )
                for message in messages
            ]
        )
        estimate = self._estimator.estimate(model_messages)
        capacity = int(self._input_limit * self._threshold) - self._output_reserve
        if estimate > capacity:
            raise ContextLimitExceeded(
                f"Prepared context estimate {estimate} exceeds capacity {capacity}"
            )
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
            message_ids=[message.id for message in messages],
            part_ids=[part.id for message in messages for part in message.parts],
            instruction_digests=instruction_digests,
            tool_schema_digest=_tool_digest(self._tools),
            created_at=self._clock.now(),
        )
        async with self._unit_of_work() as uow:
            await uow.context_manifests.add(manifest)
            await uow.commit()
        return PreparedContext(request, manifest)


def _digest_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


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
