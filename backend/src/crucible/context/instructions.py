import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LoadedInstructions:
    text: str
    digest: str


def load_root_instructions(workspace: Path) -> LoadedInstructions | None:
    root = workspace.resolve(strict=True)
    path = workspace / "AGENTS.md"
    if not path.exists() and not path.is_symlink():
        return None
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ValueError("Root AGENTS.md escapes the Task Workspace")
    content = resolved.read_bytes()
    return LoadedInstructions(
        text=content.decode("utf-8"),
        digest=hashlib.sha256(content).hexdigest(),
    )
