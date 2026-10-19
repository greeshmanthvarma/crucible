"""Incident trial data shared by storage; no evaluator or worker orchestration."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from crucible.repair.cases import _freeze


@dataclass(frozen=True)
class TrialJob:
    id: UUID
    invocation_id: UUID
    candidate_generation: int
    case_id: str
    case_digest: str
    source_digest: str
    fingerprint: str
    side: str
    repeat_index: int
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    observation_artifact_id: UUID | None = None
    repair_id: UUID | None = None


@dataclass(frozen=True)
class TrialEvidence:
    id: UUID
    fingerprint: str
    case_id: str
    repeat_index: int
    side: str
    source_digest: str
    observation_artifact_id: UUID | None
    verdict: str
    assertion_results: tuple[dict[str, Any], ...]
    usage: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        from crucible.domain.clock import require_utc

        require_utc(self.created_at)
        if self.verdict not in {"pass", "fail", "error", "unsupported"}:
            raise ValueError("Invalid protected verdict")
        object.__setattr__(
            self,
            "assertion_results",
            tuple(_freeze(dict(a)) for a in self.assertion_results),
        )
        object.__setattr__(self, "usage", _freeze(dict(self.usage)))
