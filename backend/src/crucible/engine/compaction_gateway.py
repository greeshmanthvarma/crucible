import json
import logging
from typing import cast

from crucible.context import summary_behavior
from crucible.context.compaction import CompactionRequest, CompactionSummary
from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    ModelError,
    ModelGateway,
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelStop,
    ModelUsage,
    PreparedModelRequest,
    TextDelta,
)
from crucible.repair.evidence import EvidenceRecorder

logger = logging.getLogger(__name__)

_FIELDS = (
    "objective_and_constraints",
    "decisions",
    "repository_facts",
    "changes",
    "commands_and_validation",
    "unresolved_problems",
    "execution_state",
    "important_paths_and_symbols",
)


class ModelCompactionGateway:
    """Translate the structured Compaction contract through a ModelGateway."""

    def __init__(
        self,
        gateway: ModelGateway,
        *,
        max_output_tokens: int = 2048,
        evidence: EvidenceRecorder | None = None,
    ) -> None:
        self._gateway = gateway
        self._max_output_tokens = max_output_tokens
        self._evidence = evidence

    async def compact(self, request: CompactionRequest) -> CompactionSummary:
        prompt = summary_behavior.build_compaction_prompt(request)
        prepared = PreparedModelRequest(
            run_id=new_id(),
            step_id=new_id(),
            model=request.model,
            messages=(ModelMessage(ModelRole.USER, (ModelPart("text", prompt),)),),
            tools=(),
            max_output_tokens=self._max_output_tokens,
        )
        if self._evidence is not None and request.run_id is not None:
            context = dict(request.context_snapshot or {})
            context.update(
                {
                    "prompt_version": request.prompt_version,
                    "previous_compaction_id": str(request.previous_compaction_id)
                    if request.previous_compaction_id
                    else None,
                    "source_units": list(request.source_units),
                }
            )
            # The protected recorder serializes the actual immutable DTOs,
            # including UUIDs/datetimes, without reconstructing them from IDs.
            try:
                await self._evidence.capture_request(
                    request.task_id,
                    request.run_id,
                    request.attempt_index,
                    prepared,
                    source_key=request.capture_key or str(prepared.step_id),
                    step_id=request.step_id,
                    context=context,
                )
            except Exception:
                logger.warning(
                    "Compaction request evidence unavailable",
                    extra={"run_id": str(request.run_id)},
                )
        chunks: list[str] = []
        input_tokens = output_tokens = 0
        stopped = False
        stream_failed = False
        try:
            async for item in self._gateway.stream(prepared):
                if isinstance(item, TextDelta):
                    chunks.append(item.text)
                elif isinstance(item, ModelUsage):
                    input_tokens += item.input_tokens or 0
                    output_tokens += item.output_tokens or 0
                elif isinstance(item, ModelError):
                    raise RuntimeError(f"{item.code}: {item.detail}")
                elif isinstance(item, ModelStop):
                    stopped = True
        except BaseException:
            stream_failed = True
            raise
        finally:
            if self._evidence is not None and request.run_id is not None:
                try:
                    await self._evidence.capture_compaction(
                        request.task_id,
                        request.run_id,
                        request.attempt_index,
                        prepared,
                        "".join(chunks),
                        stopped and not stream_failed,
                        source_key=request.capture_key or str(prepared.step_id),
                        step_id=request.step_id,
                    )
                except Exception:
                    logger.warning(
                        "Compaction response evidence unavailable",
                        extra={"run_id": str(request.run_id)},
                    )
        if not stopped:
            raise RuntimeError("Compaction model ended without a terminal stop item")
        try:
            value = json.loads("".join(chunks))
        except json.JSONDecodeError as error:
            raise ValueError(
                "Compaction response does not match the structured summary schema"
            ) from error
        if not isinstance(value, dict) or any(
            not isinstance(value.get(field), str) for field in _FIELDS
        ):
            raise ValueError(
                "Compaction response does not match the structured summary schema"
            )
        fields = cast(dict[str, str], value)
        return CompactionSummary(
            objective_and_constraints=fields["objective_and_constraints"],
            decisions=fields["decisions"],
            repository_facts=fields["repository_facts"],
            changes=fields["changes"],
            commands_and_validation=fields["commands_and_validation"],
            unresolved_problems=fields["unresolved_problems"],
            execution_state=fields["execution_state"],
            important_paths_and_symbols=fields["important_paths_and_symbols"],
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
