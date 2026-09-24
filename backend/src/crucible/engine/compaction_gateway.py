import json
from typing import cast

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

    def __init__(self, gateway: ModelGateway, *, max_output_tokens: int = 2048) -> None:
        self._gateway = gateway
        self._max_output_tokens = max_output_tokens

    async def compact(self, request: CompactionRequest) -> CompactionSummary:
        source = [
            {
                "role": message.role.value,
                "parts": [
                    part.text_content or part.reasoning_content or ""
                    for part in message.parts
                ],
            }
            for unit in request.source_units
            for message in unit.messages
        ]
        schema = ", ".join(_FIELDS)
        prompt = (
            "Summarize these complete Conversation units without inventing facts. "
            f"Return only one JSON object with string fields: {schema}. "
            f"Prompt version: {request.prompt_version}.\n"
            + json.dumps(source, separators=(",", ":"))
        )
        prepared = PreparedModelRequest(
            run_id=new_id(),
            step_id=new_id(),
            model=request.model,
            messages=(ModelMessage(ModelRole.USER, (ModelPart("text", prompt),)),),
            tools=(),
            max_output_tokens=self._max_output_tokens,
        )
        chunks: list[str] = []
        input_tokens = output_tokens = 0
        stopped = False
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
