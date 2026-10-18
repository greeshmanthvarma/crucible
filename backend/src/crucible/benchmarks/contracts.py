"""Versioned benchmark inputs and solver-only immutable exports."""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

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
