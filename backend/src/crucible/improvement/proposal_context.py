"""Reduce repeated reads in the proposer without changing its saved trace."""

from dataclasses import replace
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from crucible.context.invariants import TokenEstimator
from crucible.domain.ids import ToolCallId
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelMessage,
    ModelPart,
    ModelRole,
    PreparedModelRequest,
)
from crucible.engine.run_engine import RunBudgetExceeded
from crucible.improvement.contracts import EDITABLE_PATHS, Contract
from crucible.improvement.role_contracts import Identifier, ProposalResult
from crucible.models.token_counting import estimate_input_tokens

_READ_TOOLS = frozenset({"list_files", "read_file", "read_file_chunk", "search_files"})
_OMITTED = "[Earlier read-only output omitted. Repeat the original call if needed.]"
_PREVIEW_LIMIT = 1024
_PREVIEW_NOTICE = "\n[Read-only output shortened. Repeat a narrower read for details.]"
INVESTIGATION_STEPS = 6
INVESTIGATION_TOKENS = 50_000
_REPORT_RESERVE = 24_000
_REPORT = (
    "Final report phase. Return the proposal JSON specified by the original task. "
    "Report actual edits and checks, including unfinished work."
)

_DECIDE = (
    'Return only JSON with keys decision ("edit" or "decline"), '
    "selected_weakness_id, reason, and paths (repository-relative paths). "
    "For an edit, explain the concrete mechanism and name the files to edit. "
    "For a decline, explain what evidence is missing or why no safe edit is "
    "justified and use paths=[]."
)


class ProposalDecision(Contract):
    decision: Literal["edit", "decline"]
    selected_weakness_id: Identifier
    reason: Annotated[str, Field(min_length=1, max_length=2000)]
    paths: Annotated[tuple[str, ...], Field(max_length=6)]

    @model_validator(mode="after")
    def validate_paths(self) -> Self:
        if bool(self.paths) != (self.decision == "edit") or not set(self.paths) <= set(
            EDITABLE_PATHS
        ):
            raise ValueError(
                "An edit decision requires allowed paths; a decline uses []"
            )
        return self


class ProposalStepPolicy:
    def __init__(self, model_budget: int, context_limit: int) -> None:
        self._model_budget = model_budget
        self._context_limit = context_limit
        self._report_reserve = min(_REPORT_RESERVE, model_budget // 3)
        self._decision: ProposalDecision | None = None
        self._phase: Literal["investigate", "decide", "implement", "report"] = (
            "investigate"
        )
        self._has_followup_step = True

    def prepare_request(
        self, request: PreparedModelRequest, observation: BehaviorObservation
    ) -> PreparedModelRequest:
        remaining = observation.remaining_model_tokens
        self._has_followup_step = observation.remaining_steps > 0
        can_use_tools = self._has_followup_step and observation.remaining_tool_calls > 0
        report_reserve = max(
            self._report_reserve,
            _reservation(self._answer_base(request, _REPORT)),
        )
        if self._decision is None:
            investigation = _with_instruction(
                replace(
                    request,
                    tools=tuple(t for t in request.tools if t.name in _READ_TOOLS),
                ),
                "Investigation phase. Use focused reads. To decide early, " + _DECIDE,
            )
            spent = self._model_budget - remaining
            investigation_budget = min(
                INVESTIGATION_TOKENS, self._model_budget - 2 * report_reserve
            )
            if (
                can_use_tools
                and observation.step_number <= INVESTIGATION_STEPS
                and _reservation(investigation)
                <= min(investigation_budget - spent, self._context_limit)
            ):
                self._phase = "investigate"
                return investigation
            self._phase = "decide"
            instruction = (
                "Decision phase. Use the evidence already gathered. " + _DECIDE
            )
            decision_budget = min(
                max(
                    _REPORT_RESERVE,
                    _reservation(self._answer_base(request, instruction)),
                ),
                remaining - report_reserve if can_use_tools else remaining,
            )
            return self._answer_request(
                request,
                instruction,
                decision_budget,
            )

        implementation = _with_instruction(
            request,
            "Implementation and verification phase. Carry out the recorded "
            "edit decision, use targeted reads, and check the change.",
        )
        if can_use_tools and _reservation(implementation) <= min(
            remaining - report_reserve, self._context_limit
        ):
            self._phase = "implement"
            return implementation
        self._phase = "report"
        return self._answer_request(request, _REPORT, remaining)

    def _keep_for_answer(self, message: ModelMessage) -> bool:
        if message.role in (ModelRole.SYSTEM, ModelRole.USER):
            return True
        return (
            message.role is ModelRole.ASSISTANT
            and self._decision is not None
            and any(
                part.text_content == self._decision.model_dump_json()
                for part in message.parts
            )
        )

    def _answer_base(
        self, request: PreparedModelRequest, instruction: str
    ) -> PreparedModelRequest:
        return _with_instruction(
            replace(
                request,
                tools=(),
                messages=tuple(
                    message
                    for message in request.messages
                    if self._keep_for_answer(message)
                ),
            ),
            instruction,
        )

    def _answer_request(
        self, request: PreparedModelRequest, instruction: str, budget: int
    ) -> PreparedModelRequest:
        request = _with_instruction(replace(request, tools=()), instruction)
        limit = min(budget, self._context_limit)
        if _reservation(request) <= limit:
            return request

        # Drop complete assistant/tool exchanges as needed. Keep the original
        # brief, instructions and recorded decision, with newest work first.
        messages = list(request.messages)
        index = 0
        while index < len(messages):
            message = messages[index]
            if message.role is not ModelRole.ASSISTANT or self._keep_for_answer(
                message
            ):
                index += 1
                continue
            end = index + 1
            while end < len(messages) and messages[end].role is ModelRole.TOOL:
                end += 1
            del messages[index:end]
            reduced = replace(request, messages=tuple(messages))
            if _reservation(reduced) <= limit:
                return reduced
        raise RunBudgetExceeded(
            "Proposer budget cannot fit the required answer context"
        )

    def finish_response(
        self, text: str, calls: tuple[CompleteToolCall, ...]
    ) -> tuple[str, bool]:
        if calls:
            return text, False
        if self._phase in ("investigate", "decide"):
            decision = ProposalDecision.model_validate_json(text)
            if decision.decision == "edit" and self._has_followup_step:
                self._decision = decision
                return decision.model_dump_json(), True
            summary = decision.reason
            if decision.decision == "edit":
                summary += " The run reached its step limit before applying the edit."
            return ProposalResult(
                schema_version=1,
                selected_weakness_id=decision.selected_weakness_id,
                summary=summary,
                claimed_paths=(),
                risks=(),
            ).model_dump_json(), False
        return text, False


def _reservation(request: PreparedModelRequest) -> int:
    return (
        estimate_input_tokens(request.messages, request.tools)
        + request.max_output_tokens
    )


def _with_instruction(
    request: PreparedModelRequest, instruction: str
) -> PreparedModelRequest:
    return replace(
        request,
        messages=(
            ModelMessage(ModelRole.SYSTEM, (ModelPart("text", instruction),)),
            *request.messages,
        ),
    )


def prune_proposal_tool_results(
    messages: tuple[ModelMessage, ...],
    observation: BehaviorObservation,
    estimated_input_tokens: int,
    max_output_tokens: int,
    estimator: TokenEstimator,
) -> tuple[ModelMessage, ...]:
    # Leave room for editing, checking the change and returning the final report.
    turns = min(4, 1 + observation.remaining_steps)
    if (estimated_input_tokens + max_output_tokens) * turns <= (
        observation.remaining_model_tokens
    ):
        return messages

    batches = [
        {part.tool_call_id for part in message.parts if part.kind == "tool_call"}
        for message in messages
        if any(part.kind == "tool_call" for part in message.parts)
    ]
    projected = _shorten_outputs(
        messages, {call for batch in batches[:-2] for call in batch}
    )
    # Preserve recent batches when the budget can cover a follow-up call.
    for index, batch in enumerate(batches[-2:]):
        if (estimator.estimate(projected) + max_output_tokens) * min(2, turns) <= (
            observation.remaining_model_tokens
        ):
            break
        projected = _shorten_outputs(
            projected, batch, preview=index == min(2, len(batches)) - 1
        )
    return projected


def _shorten_outputs(
    messages: tuple[ModelMessage, ...],
    calls: set[ToolCallId | None],
    *,
    preview: bool = False,
) -> tuple[ModelMessage, ...]:
    if not calls:
        return messages
    limit = _PREVIEW_LIMIT + len(_PREVIEW_NOTICE) if preview else len(_OMITTED)

    def shorten(text: str | None) -> str:
        return (text or "")[:_PREVIEW_LIMIT] + _PREVIEW_NOTICE if preview else _OMITTED

    return tuple(
        replace(
            message,
            parts=tuple(
                replace(part, text_content=shorten(part.text_content))
                if part.kind == "tool_result"
                and part.tool_call_id in calls
                and part.tool_name in _READ_TOOLS
                and len(part.text_content or "") > limit
                else part
                for part in message.parts
            ),
        )
        for message in messages
    )
