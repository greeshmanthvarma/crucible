"""Only current solver inputs cross this bounded JSON boundary."""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from crucible.benchmarks.contracts import (
    Commit,
    ModelSettings,
    Platform,
    Sha256,
    TrialLimits,
)
from crucible.improvement.contracts import (
    Contract,
    ExecutionManifest,
    ExperimentManifest,
    Image,
    Positive,
)
from crucible.repair.gateway import TrustedUsage
from crucible.sandbox.protocol import SandboxOutcome


class SolverCaseWire(Contract):
    case_id: Annotated[str, Field(min_length=1, max_length=200)]
    base_commit: Commit
    base_tree: Commit
    prompt: Annotated[str, Field(max_length=1_048_576)]
    image: Image
    platform: Platform
    workspace: Literal["/workspace"]


class WorkerJob(Contract):
    schema_version: Literal[1, 2]
    job_kind: Literal["trial"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    job_id: UUID
    experiment_id: UUID
    trial_id: UUID
    repeat_index: Annotated[int, Field(ge=0, le=2)]
    harness_digest: Sha256
    export_digest: Sha256
    solver_case: SolverCaseWire
    model: ModelSettings
    limits: TrialLimits
    output_limit_bytes: Positive
    wall_deadline_seconds: Positive
    protocol_version: Literal[1, 2]

    @model_validator(mode="after")
    def versioned_trial(self) -> Self:
        if self.schema_version != self.protocol_version or (
            self.schema_version == 2
        ) != (self.job_kind == "trial"):
            raise ValueError("trial protocol version mismatch")
        return self


class RoleJob(Contract):
    schema_version: Literal[2]
    protocol_version: Literal[2]
    job_id: UUID
    role_id: UUID
    round_id: UUID
    experiment_id: UUID
    job_kind: Literal["mining", "format_correction", "proposal"]
    harness_digest: Sha256
    export_digest: Sha256
    model: ModelSettings
    limits: TrialLimits
    input_digest: Sha256
    workspace: Literal["/workspace"]
    command_image: Image
    command_platform: Platform
    output_limit_bytes: Positive
    wall_deadline_seconds: Positive

    @property
    def trial_id(self) -> None:
        return None


WorkerRequest = WorkerJob | RoleJob


def read_worker_job(raw: bytes) -> WorkerRequest:
    import json

    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("job exceeds cap")
    kind = json.loads(raw).get("job_kind")
    return (
        RoleJob.model_validate_json(raw)
        if kind in ("mining", "format_correction", "proposal")
        else WorkerJob.model_validate_json(raw)
    )


def validate_job_manifest(job: WorkerRequest, manifest: ExecutionManifest) -> None:
    if isinstance(job, RoleJob) and not isinstance(manifest, ExperimentManifest):
        raise ValueError("roles require an experiment manifest")
    limits = (
        manifest.trial_limits
        if isinstance(job, WorkerJob)
        else _role_limits(manifest)[
            "proposal" if job.job_kind == "proposal" else "mining"
        ]
    )
    if (job.experiment_id, job.model, job.limits, job.protocol_version) != (
        manifest.experiment_id,
        manifest.model,
        limits,
        manifest.worker_protocol_version or 1,
    ):
        raise ValueError("job manifest limits or identity mismatch")
    if isinstance(job, RoleJob) and (job.command_image, job.command_platform) != (
        manifest.worker_image,
        manifest.worker_platform,
    ):
        raise ValueError("role command environment mismatch")
    if (
        job.wall_deadline_seconds != limits.wall_timeout_seconds
        or job.output_limit_bytes
        > min(manifest.command_policy.max_output_bytes_per_call, 8 * 1024 * 1024)
    ):
        raise ValueError("job exceeds frozen output or wall limits")


def job_environment(job: WorkerRequest, manifest: ExecutionManifest) -> tuple[str, str]:
    if isinstance(job, WorkerJob):
        return job.solver_case.image, job.solver_case.platform
    return manifest.worker_image, manifest.worker_platform


def _role_limits(manifest: ExecutionManifest) -> dict[str, TrialLimits]:
    if not isinstance(manifest, ExperimentManifest):
        raise ValueError("roles require an experiment manifest")
    return {key: value for key, value in manifest.role_limits.items()}


class WorkerObservation(Contract):
    schema_version: Literal[1, 2]
    job_id: UUID
    trial_id: UUID
    export_digest: Sha256
    termination: Literal[
        "completed",
        "budget_exhausted",
        "behavior_error",
        "cancelled",
        "provider_error",
        "infrastructure_error",
    ]
    trace_ref: Annotated[str, Field(min_length=1, max_length=256)]
    usage_ref: Annotated[str, Field(min_length=1, max_length=256)]
    detail_code: Annotated[str, Field(max_length=200)] | None

    @field_validator("trace_ref", "usage_ref")
    @classmethod
    def relative_reference(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in value
            or ":" in value
            or not path.parts
        ):
            raise ValueError("invalid relative artifact reference")
        return value


def read_worker_observation(raw: bytes, job: WorkerJob) -> WorkerObservation:
    if len(raw) > 1_048_576:
        raise ValueError("worker observation exceeds limit")
    observation = WorkerObservation.model_validate_json(raw)
    if (observation.job_id, observation.trial_id, observation.export_digest) != (
        job.job_id,
        job.trial_id,
        job.export_digest,
    ) or observation.schema_version != job.schema_version:
        raise ValueError("worker observation identity mismatch")
    return observation


@dataclass(frozen=True)
class CommandRecord:
    command_id: UUID
    spec_digest: str
    approved: bool
    outcome: SandboxOutcome | None


@dataclass(frozen=True)
class IsolatedExecution:
    observation: WorkerObservation
    workspace: Path
    state_root: Path
    usage: TrustedUsage
    commands: tuple[CommandRecord, ...]
    cleanup_confirmed: bool
    elapsed_seconds: float


class RoleObservation(Contract):
    schema_version: Literal[2]
    job_id: UUID
    role_id: UUID
    round_id: UUID
    experiment_id: UUID
    export_digest: Sha256
    termination: Literal[
        "completed",
        "budget_exhausted",
        "behavior_error",
        "cancelled",
        "provider_error",
        "infrastructure_error",
    ]
    assistant_output_ref: Annotated[str, Field(min_length=1, max_length=256)]
    trace_ref: Annotated[str, Field(min_length=1, max_length=256)]
    detail_code: Annotated[str, Field(max_length=200)] | None

    @field_validator("assistant_output_ref", "trace_ref")
    @classmethod
    def relative_reference(cls, value: str) -> str:
        return WorkerObservation.relative_reference(value)


def read_role_observation(raw: bytes, job: RoleJob) -> RoleObservation:
    if len(raw) > 1_048_576:
        raise ValueError("role observation exceeds cap")
    result = RoleObservation.model_validate_json(raw)
    if (
        result.job_id,
        result.role_id,
        result.round_id,
        result.experiment_id,
        result.export_digest,
    ) != (job.job_id, job.role_id, job.round_id, job.experiment_id, job.export_digest):
        raise ValueError("role observation identity mismatch")
    return result


@dataclass(frozen=True)
class IsolatedRoleExecution:
    observation: RoleObservation
    workspace: Path
    state_root: Path
    usage: TrustedUsage
    commands: tuple[CommandRecord, ...]
    cleanup_confirmed: bool
    elapsed_seconds: float
