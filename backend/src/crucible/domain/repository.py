from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crucible.domain.clock import require_utc
from crucible.domain.ids import RepositoryId


@dataclass(frozen=True)
class Repository:
    id: RepositoryId
    root_path: Path
    created_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.created_at)
