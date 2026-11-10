"""Validate a model's diagnosis or bounded evidence request."""

import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, TypeAdapter

_MAX_RESPONSE_BYTES = 16 * 1024


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


_RESPONSE: TypeAdapter[_DiagnosisEnvelope | _DetailEnvelope] = TypeAdapter(
    _DiagnosisEnvelope | _DetailEnvelope
)


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
        envelope = _RESPONSE.validate_json(encoded)
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
