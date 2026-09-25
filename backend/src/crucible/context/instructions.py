import hashlib
from dataclasses import dataclass
from pathlib import Path

from crucible.tools.filesystem import WorkspacePathResolver


@dataclass(frozen=True)
class LoadedInstructions:
    text: str
    digest: str


def load_root_instructions(workspace: Path) -> LoadedInstructions | None:
    path = workspace / "AGENTS.md"
    if not path.exists() and not path.is_symlink():
        return None
    resolved = WorkspacePathResolver(workspace).resolve("AGENTS.md")
    content = resolved.read_bytes()
    return LoadedInstructions(
        text=content.decode("utf-8"),
        digest=hashlib.sha256(content).hexdigest(),
    )
