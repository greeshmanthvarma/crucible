"""Run a bounded, one-case diagnosis exchange."""

import json
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelGateway,
    ModelStop,
    ModelStopReason,
    PreparedModelRequest,
    TextDelta,
)
from crucible.improvement.case_evidence import (
    INITIAL_CAP,
    CasePacketError,
    PreparedCasePacket,
    expand_case_packet,
)
from crucible.models.token_counting import estimate_input_tokens

_MAX_RESPONSE_BYTES = 16 * 1024
CASE_TOKEN_CAP = 24_000


class CaseDiagnosisError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Diagnosis(_ResponseModel):
    observed_behavior: str
    relationship: Literal["linked", "contributing", "recovered", "unknown"]
    evidence_refs: tuple[str, ...]
    uncertainty: str


class DetailRequest(_ResponseModel):
    refs: tuple[str, ...]


class _DiagnosisEnvelope(_ResponseModel):
    diagnosis: Diagnosis


class _DetailEnvelope(_ResponseModel):
    request_details: DetailRequest


class _InitialResponse(_ResponseModel):
    result: _DiagnosisEnvelope | _DetailEnvelope


class _FinalResponse(_ResponseModel):
    result: _DiagnosisEnvelope


def _response_format(*, followup: bool) -> Mapping[str, object]:
    model = _FinalResponse if followup else _InitialResponse
    return {
        "type": "json_schema",
        "name": "case_diagnosis_final" if followup else "case_diagnosis_initial",
        "strict": True,
        "schema": model.model_json_schema(),
    }


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("Duplicate JSON key")
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("Nonstandard JSON constant")


def _valid_refs(refs: tuple[str, ...], allowed: frozenset[str]) -> bool:
    return len(refs) == len(set(refs)) and allowed.issuperset(refs)


def parse_case_response(
    text: str,
    *,
    visible_refs: frozenset[str],
    detail_refs: frozenset[str],
) -> Diagnosis | DetailRequest:
    try:
        encoded = text.encode()
        if len(encoded) > _MAX_RESPONSE_BYTES:
            raise ValueError("Oversized response")
        # Check syntax and duplicate keys before strict validation of JSON arrays.
        json.loads(
            encoded, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        envelope = _InitialResponse.model_validate_json(encoded).result
    except (ValueError, RecursionError):
        raise CaseDiagnosisError("invalid_response") from None

    if isinstance(envelope, _DetailEnvelope):
        details = envelope.request_details
        if not 1 <= len(details.refs) <= 3 or not _valid_refs(
            details.refs, detail_refs
        ):
            raise CaseDiagnosisError("invalid_disclosure")
        return details

    diagnosis = envelope.diagnosis
    if not diagnosis.observed_behavior.strip() or not diagnosis.uncertainty.strip():
        raise CaseDiagnosisError("invalid_response")
    if not _valid_refs(diagnosis.evidence_refs, visible_refs) or (
        not diagnosis.evidence_refs and diagnosis.relationship != "unknown"
    ):
        raise CaseDiagnosisError("invalid_citations")
    return diagnosis


@dataclass(frozen=True)
class CaseDiagnosisResult:
    diagnosis: Diagnosis | None
    error_code: str | None
    requests_sent: int
    total_reserved_tokens: int
    disclosed_refs: tuple[str, ...]


async def _read_response(gateway: ModelGateway, request: PreparedModelRequest) -> str:
    chunks: list[str] = []
    size = 0
    try:
        stream = gateway.stream(request)
        try:
            async for item in stream:
                if isinstance(item, TextDelta) and item.text:
                    remaining = _MAX_RESPONSE_BYTES - size
                    if len(item.text) > remaining:
                        raise CaseDiagnosisError("output_limit")
                    try:
                        size += len(item.text.encode())
                    except UnicodeEncodeError:
                        raise CaseDiagnosisError("invalid_response") from None
                    if size > _MAX_RESPONSE_BYTES:
                        raise CaseDiagnosisError("output_limit")
                    chunks.append(item.text)
                elif isinstance(item, CompleteToolCall):
                    raise CaseDiagnosisError("unexpected_tool_call")
                elif isinstance(item, ModelError):
                    raise CaseDiagnosisError("gateway_error")
                elif isinstance(item, ModelStop):
                    if item.reason == ModelStopReason.COMPLETE:
                        return "".join(chunks)
                    if item.reason == ModelStopReason.LENGTH:
                        raise CaseDiagnosisError("output_limit")
                    raise CaseDiagnosisError("unexpected_tool_call")
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                with suppress(Exception):
                    await close()
    except CaseDiagnosisError:
        raise
    except Exception:
        raise CaseDiagnosisError("gateway_error") from None
    raise CaseDiagnosisError("incomplete_response")


async def run_case_diagnosis(
    prepared: PreparedCasePacket,
    *,
    gateway: ModelGateway,
    model: str,
    total_token_cap: int = CASE_TOKEN_CAP,
) -> CaseDiagnosisResult:
    """Send at most two pre-budgeted requests through the supplied gateway."""
    if type(total_token_cap) is not int or not 1 <= total_token_cap <= CASE_TOKEN_CAP:
        return CaseDiagnosisResult(None, "invalid_limit", 0, 0, ())

    visible_refs = frozenset(event.id for event in prepared.packet.events)
    detail_refs = frozenset(prepared.packet.available_detail_refs)
    preview = prepared.initial
    run_id = new_id()
    requests_sent = 0
    reserved = 0
    requested_refs: tuple[str, ...] = ()
    disclosed_refs: tuple[str, ...] = ()
    try:
        response_format = _response_format(followup=False)
        try:
            input_tokens = estimate_input_tokens(
                preview.messages, (), response_format=response_format
            )
        except (ValueError, OSError):
            raise CaseDiagnosisError("tokenizer_unavailable") from None
        if input_tokens > INITIAL_CAP:
            raise CaseDiagnosisError("budget_exceeded")
        while True:
            reservation = input_tokens + preview.output_reserve_tokens
            if reserved + reservation > total_token_cap:
                raise CaseDiagnosisError("budget_exceeded")
            request = PreparedModelRequest(
                run_id=run_id,
                step_id=new_id(),
                model=model,
                messages=preview.messages,
                tools=(),
                max_output_tokens=preview.output_reserve_tokens,
                response_format=response_format,
            )
            requests_sent += 1
            reserved += reservation
            disclosed_refs = requested_refs
            response = parse_case_response(
                await _read_response(gateway, request),
                visible_refs=visible_refs,
                detail_refs=detail_refs,
            )
            if isinstance(response, Diagnosis):
                return CaseDiagnosisResult(
                    response, None, requests_sent, reserved, disclosed_refs
                )
            if requests_sent == 2:
                raise CaseDiagnosisError("disclosure_exhausted")
            requested_refs = response.refs
            response_format = _response_format(followup=True)
            preview = expand_case_packet(
                prepared, requested_refs, response_format=response_format
            )
            input_tokens = preview.input_tokens
    except (CaseDiagnosisError, CasePacketError) as error:
        return CaseDiagnosisResult(
            None, error.code, requests_sent, reserved, disclosed_refs
        )
