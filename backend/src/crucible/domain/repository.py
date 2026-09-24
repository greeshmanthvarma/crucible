from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crucible.domain.clock import require_utc
from crucible.domain.commands import CommandSpec
from crucible.domain.ids import RepositoryId


@dataclass(frozen=True)
class RepositorySettings:
    validation_commands: tuple[CommandSpec, ...] = ()
    sandbox_image: str = ""
    sandbox_network: str = "none"
    validation_repair_limit: int = 0
    default_cwd: str = "."
    compaction_threshold: float = 0.8
    compaction_model: str | None = None
    compaction_prompt_version: str = "v1"
    compaction_attempt_limit: int = 2
    model_input_limit: int = 128_000
    model_output_reserve: int = 8_000
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not 0 < self.compaction_threshold <= 1:
            raise ValueError("Compaction threshold must be in (0, 1]")
        if min(self.validation_repair_limit, self.compaction_attempt_limit) < 0:
            raise ValueError("Attempt limits cannot be negative")
        if self.model_input_limit <= 0 or self.model_output_reserve < 0:
            raise ValueError("Model limits are invalid")


@dataclass(frozen=True)
class Repository:
    id: RepositoryId
    root_path: Path
    created_at: datetime
    settings: RepositorySettings = RepositorySettings()

    def __post_init__(self) -> None:
        require_utc(self.created_at)
