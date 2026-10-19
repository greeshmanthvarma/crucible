"""Editable, advisory coding strategy. Enforcement lives in trusted callers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ToolObservation:
    name: str
    status: str
    error_code: str | None


@dataclass(frozen=True)
class BehaviorObservation:
    step_number: int
    remaining_steps: int
    remaining_tool_calls: int
    remaining_model_tokens: int
    remaining_active_seconds: float
    recent_tools: tuple[ToolObservation, ...]
    completion_proposed: bool


def coding_guidance() -> str:
    return ""


def step_guidance(observation: BehaviorObservation) -> str:
    return ""
