"""Data-only contracts for isolated experiments."""

from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from crucible.benchmarks.contracts import (
    Commit,
    GraderConfig,
    ModelSettings,
    Platform,
    Sha256,
    TrialLimits,
)

EDITABLE_PATHS = (
    "backend/src/crucible/tools/filesystem.py",
    "backend/src/crucible/tools/git_tools.py",
    "backend/src/crucible/tools/behavior_contracts.py",
    "backend/src/crucible/context/selection.py",
    "backend/src/crucible/context/summary_behavior.py",
    "backend/src/crucible/engine/coding_behavior.py",
)
Positive = Annotated[int, Field(gt=0, le=2**53)]
Nonnegative = Annotated[int, Field(ge=0, le=2**53)]
Image = Annotated[str, Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$", max_length=512)]


class Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, allow_inf_nan=False
    )


class FrozenCaseRef(Contract):
    case_id: Annotated[
        str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$", max_length=200)
    ]
    split: Literal["development", "selection-validation", "final-test"]
    case_digest: Sha256
    admission_digest: Sha256


class ExperimentCommandPolicy(Contract):
    mode: Literal["interactive", "preauthorized-disposable"]
    max_calls_per_trial: Positive
    max_seconds_per_call: Positive
    max_output_bytes_per_call: Positive
    allow_network: Literal[False]


class ExperimentBudget(Contract):
    max_reported_and_reserved_tokens: Positive
    max_model_requests: Positive
    max_trial_starts: Positive
    max_active_seconds: Positive
    max_worker_wall_seconds: Positive
    max_total_output_bytes: Positive


class UsageAmounts(Contract):
    tokens: Nonnegative = 0
    requests: Nonnegative = 0
    active_seconds: Nonnegative = 0
    output_bytes: Nonnegative = 0
    trial_starts: Nonnegative = 0
    worker_wall_seconds: Nonnegative = 0


class ExecutionPolicy(Contract):
    """Authority required by a worker; does not admit a measured experiment."""

    schema_version: Literal[1, 2]
    worker_protocol_version: Literal[2] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    experiment_id: UUID
    initial_commit: Commit
    initial_source_digest: Sha256
    controller_digest: Sha256
    dependency_lock_digest: Sha256
    worker_image: Image
    worker_platform: Platform
    relay_image: Image
    grader: GraderConfig
    model: ModelSettings
    trial_limits: TrialLimits
    command_policy: ExperimentCommandPolicy
    budget: ExperimentBudget
    network_policy: Literal["broker-only"]


class CalibrationManifest(ExecutionPolicy):
    kind: Literal["isolated-calibration"]
    schema_version: Literal[1]
    worker_protocol_version: Literal[2]
    authorization: Literal["one-isolated-calibration"]
    approval_mode: Literal["interactive"]
    case_manifest: Path
    admission_record: Path
    case_digest: Sha256
    admission_digest: Sha256
    trial_id: UUID

    @model_validator(mode="after")
    def interactive_only(self) -> Self:
        if self.command_policy.mode != "interactive":
            raise ValueError("calibration requires interactive commands")
        if self.model.output_reserve >= self.model.input_limit:
            raise ValueError("invalid context limits")
        return self


class ExperimentManifest(ExecutionPolicy):
    cases: Annotated[tuple[FrozenCaseRef, ...], Field(min_length=3, max_length=4096)]
    role_limits: dict[Literal["mining", "proposal"], TrialLimits]
    repeat_count: Literal[3]
    max_rounds: Literal[3]
    editable_paths: tuple[str, ...]
    boundary_suite_digest: Sha256
    scoring_version: Literal["per-case-count-v1"]
    disposable_commands_authorized: bool = False

    @model_validator(mode="after")
    def frozen_policy(self) -> Self:
        if (self.schema_version == 2) != (self.worker_protocol_version == 2):
            raise ValueError("manifest and worker protocol versions disagree")
        if {c.split for c in self.cases} != {
            "development",
            "selection-validation",
            "final-test",
        }:
            raise ValueError("every split requires admitted cases")
        if len({c.case_id for c in self.cases}) != len(self.cases):
            raise ValueError("duplicate case")
        if self.editable_paths != EDITABLE_PATHS:
            raise ValueError("editable paths differ from protected policy")
        if set(self.role_limits) != {"mining", "proposal"}:
            raise ValueError("explicit role limits required")
        if (
            self.command_policy.mode == "preauthorized-disposable"
            and not self.disposable_commands_authorized
        ):
            raise ValueError("disposable commands require operator authorization")
        return self

    @property
    def baseline_trial_count(self) -> int:
        return 3 * sum(c.split != "final-test" for c in self.cases)

    @property
    def planned_trial_upper_bound(self) -> int:
        return self.baseline_trial_count * 4 + 6 * sum(
            c.split == "final-test" for c in self.cases
        )


ExecutionManifest = ExperimentManifest | CalibrationManifest


class ExperimentPaths(Contract):
    root: Path
    registry_root: Path
    repository: Path


class HarnessSnapshot(Contract):
    snapshot_id: UUID
    parent_commit: Commit
    source_digest: Sha256
    export_digest: Sha256
    patch_digest: Sha256 | None
    kind: Literal["baseline", "candidate"]
