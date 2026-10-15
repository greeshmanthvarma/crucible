"""Protected declarative incident cases and reviewer-sealed expectations."""

import os
from typing import Annotated, Literal, Never, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from crucible.repair.identity import digest

SUMMARY_FIELDS = frozenset(
    (
        "objective_and_constraints",
        "decisions",
        "repository_facts",
        "changes",
        "commands_and_validation",
        "unresolved_problems",
        "execution_state",
        "important_paths_and_symbols",
    )
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class FailureMatcher(StrictModel):
    status: str
    error_code: str | None
    detail_contains: tuple[str, ...] = ()

    @model_validator(mode="after")
    def failure_only(self) -> Self:
        if self.status in {"completed", "success", "unsupported"}:
            raise ValueError("Baseline matcher must describe an observed failure")
        return self


def relative_path(value: str) -> str:
    if (
        not value
        or value.startswith("/")
        or "\\" in value
        or ":" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or "\x00" in value
    ):
        raise ValueError("Expected a contained relative fixture path")
    return value


Operator = Literal[
    "error_code_equals",
    "status_equals",
    "output_text_equals",
    "output_text_contains",
    "result_field_equals",
    "file_exists",
    "file_absent",
    "file_text_equals",
    "file_text_contains",
    "unrelated_files_unchanged",
    "summary_schema_valid",
    "tool_pairs_complete",
    "retained_messages_ordered",
    "context_within_estimated_limit",
    "required_literal_fact_present",
]


class Assertion(StrictModel):
    op: Operator
    path: str | None = None
    value: JsonValue = None
    fields: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_operands(self) -> Self:
        if self.op.startswith("file_"):
            relative_path(self.path or "")
        elif self.op == "result_field_equals":
            pointer = self.path
            if pointer is None or not pointer.startswith("/"):
                raise ValueError("Expected a literal JSON pointer")
            for segment in pointer[1:].split("/"):
                if segment in {".", ".."} or "*" in segment:
                    raise ValueError("Recursive/wildcard pointers are unsupported")
                stripped = segment.replace("~0", "").replace("~1", "")
                if "~" in stripped:
                    raise ValueError("Invalid JSON pointer escape")
        elif self.path is not None:
            raise ValueError("This operator does not accept a path")
        text_ops = {
            "status_equals",
            "output_text_equals",
            "output_text_contains",
            "file_text_equals",
            "file_text_contains",
            "required_literal_fact_present",
        }
        if self.op in text_ops and not isinstance(self.value, str):
            raise ValueError("Expected a literal string")
        if (
            self.op == "error_code_equals"
            and self.value is not None
            and not isinstance(self.value, str)
        ):
            raise ValueError("Expected a string or null error code")
        if self.op == "required_literal_fact_present":
            if (
                not self.value
                or not self.fields
                or not set(self.fields) <= SUMMARY_FIELDS
            ):
                raise ValueError(
                    "Facts require literal text and explicit summary fields"
                )
        elif self.fields:
            raise ValueError("Only literal fact checks accept summary fields")
        if (
            self.op not in text_ops | {"error_code_equals", "result_field_equals"}
            and self.value is not None
        ):
            raise ValueError("This operator does not accept a value")
        return self


class CaseBase(StrictModel):
    schema_version: Literal[1] = 1
    case_id: str = Field(min_length=1)
    incident_ids: dict[str, UUID]
    replay_mode: Literal["direct", "recorded", "fresh_call"]
    fixture_manifest_digest: str
    settings: dict[str, JsonValue]
    expected_baseline_failure: FailureMatcher
    candidate_assertions: tuple[Assertion, ...] = Field(min_length=1)
    expected_behavior: str
    evidence_ids: tuple[UUID, ...]

    @field_validator("schema_version", mode="before")
    @classmethod
    def integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("Schema version must be an integer")
        return value


class ToolCase(CaseBase):
    kind: Literal["tool"]
    replay_mode: Literal["direct"]
    tool_name: Literal[
        "list_files",
        "search_files",
        "read_file",
        "write_file",
        "apply_patch",
        "workspace_status",
        "workspace_diff",
        "execute_command",
    ]
    arguments: dict[str, JsonValue]


class ContextSelectionCase(CaseBase):
    kind: Literal["context_selection"]
    replay_mode: Literal["direct"]
    source_artifact_ids: tuple[UUID, ...]
    correlation_links: dict[str, str]
    prior_compaction_artifact_id: UUID | None = None


class CompactionCase(CaseBase):
    kind: Literal["compaction"]
    replay_mode: Literal["recorded", "fresh_call"]
    source_artifact_ids: tuple[UUID, ...]
    correlation_links: dict[str, str]
    prior_compaction_artifact_id: UUID | None = None
    request_artifact_id: UUID
    raw_response_artifact_id: UUID | None = None

    @model_validator(mode="after")
    def recorded_response_required(self) -> Self:
        if self.replay_mode == "recorded" and self.raw_response_artifact_id is None:
            raise ValueError("Recorded replay requires a response artifact")
        return self


IncidentCase = Annotated[
    ToolCase | ContextSelectionCase | CompactionCase, Field(discriminator="kind")
]


class _FrozenDict(dict[str, object]):
    def _deny(self, *args: object, **kwargs: object) -> Never:
        raise TypeError("Sealed JSON is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = (
        __ior__
    ) = _deny


class _FrozenList(list[object]):
    def _deny(self, *args: object, **kwargs: object) -> Never:
        raise TypeError("Sealed JSON is immutable")

    __setitem__ = __delitem__ = append = clear = extend = insert = pop = remove = (
        reverse
    ) = sort = __iadd__ = __imul__ = _deny


def _freeze(value: object) -> object:
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            object.__setattr__(value, name, _freeze(getattr(value, name)))
        return value
    if isinstance(value, dict):
        return _FrozenDict({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return _FrozenList(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


class ApprovedCase(StrictModel):
    case_digest: str
    case: IncidentCase
    review_id: UUID

    @model_validator(mode="after")
    def validate_digest(self) -> Self:
        if digest(self.case.model_dump(mode="json")) != self.case_digest:
            raise ValueError("Approved case content differs from its sealed digest")
        _freeze(self.case)
        return self


def seal_case(case: IncidentCase, review_id: UUID) -> ApprovedCase:
    # Round-trip rather than sharing caller-owned dictionaries or nested models.
    payload = case.model_dump(mode="json")
    secrets = tuple(
        value
        for name, value in os.environ.items()
        if value
        and any(word in name.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD"))
    )
    for assertion in case.candidate_assertions:
        if isinstance(assertion.value, str) and any(
            secret in assertion.value for secret in secrets
        ):
            raise ValueError("Required assertion contains a configured credential")
    return ApprovedCase.model_validate(
        {
            "case_digest": digest(payload),
            "case": payload,
            "review_id": review_id,
        }
    )
