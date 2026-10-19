"""Only current solver inputs cross this bounded JSON boundary."""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator

from crucible.benchmarks.contracts import (
    Commit,
    ModelSettings,
    Platform,
    Sha256,
    TrialLimits,
)
from crucible.improvement.contracts import Contract, Image, Positive
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
    schema_version: Literal[1]
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
    protocol_version: Literal[1]


class WorkerObservation(Contract):
    schema_version: Literal[1]
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
    ):
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
