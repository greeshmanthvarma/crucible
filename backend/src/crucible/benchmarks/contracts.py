"""Versioned benchmark inputs and solver-only immutable exports."""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Commit = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Platform = Literal["linux/amd64", "linux/arm64"]
Split = Literal["development", "selection-validation", "final-test", "calibration"]


class BenchmarkCase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    case_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")]
    split: Split
    repo: Annotated[str, Field(min_length=1)]
    base_commit: Commit
    fixture_path: Path
    fixture_digest: Sha256
    prompt_path: Path
    prompt_digest: Sha256
    image: Annotated[str, Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")]
    platform: Platform
    grader_input_path: Path
    grader_input_digest: Sha256


@dataclass(frozen=True)
class SolverCase:
    case_id: str
    base_commit: str
    workspace: Path
    prompt: str
    image: str
    platform: Platform


@dataclass(frozen=True)
class SolutionPatch:
    path: Path
    sha256: str
    base_commit: str
    byte_count: int


class GraderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    driver_python: Path
    driver_source: Path
    driver_commit: Commit
    driver_digest: Sha256
    timeout_seconds: Annotated[int, Field(gt=0)]
    memory_bytes: Annotated[int, Field(gt=0)]
    cpus: Annotated[int, Field(gt=0)]
    output_limit_bytes: Annotated[int, Field(gt=0)]
    network: Literal["none"]

    @field_validator("driver_python", "driver_source")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("absolute driver paths without parent traversal required")
        return value


@dataclass(frozen=True)
class GradeResult:
    verdict: Literal["pass", "fail", "error"]
    code: str
    evaluation_id: str
    resolved: bool | None
    patch_sha256: str
    report_path: Path | None
    log_path: Path
    driver_commit: str
    elapsed_seconds: float


class TrialLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    max_steps: Annotated[int, Field(gt=0)]
    max_tool_calls: Annotated[int, Field(gt=0)]
    max_model_tokens: Annotated[int, Field(gt=0)]
    max_active_seconds: Annotated[int, Field(gt=0)]
    wall_timeout_seconds: Annotated[int, Field(gt=0)]


class TrialIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    trial_id: UUID
    case_digest: Sha256
    harness_commit: Commit
    harness_tree_digest: Sha256
    model_config_digest: Sha256
    limits_digest: Sha256
    repeat_index: Annotated[int, Field(ge=0, le=2)]


@dataclass(frozen=True)
class SolverResult:
    termination: Literal[
        "completed",
        "budget_exhausted",
        "behavior_error",
        "cancelled",
        "provider_error",
        "infrastructure_error",
    ]
    task_id: UUID
    run_id: UUID
    workspace: Path
    trace_path: Path
    usage_path: Path


@dataclass(frozen=True)
class TrialResult:
    identity: TrialIdentity
    solver: SolverResult | None
    patch: SolutionPatch | None
    grade: GradeResult | None
    status: Literal["complete", "error", "interrupted"]
    error_code: str | None


class ModelSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    model_id: Annotated[str, Field(min_length=1)]
    input_limit: Annotated[int, Field(gt=0)]
    output_reserve: Annotated[int, Field(gt=0)]
    provider_defaults: Literal["unspecified"]

    @field_validator("model_id")
    @classmethod
    def real_model(cls, value: str) -> str:
        if value in {"fake", "deterministic"}:
            raise ValueError(
                "production benchmark commands require an explicit real model"
            )
        return value
