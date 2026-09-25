"""Create one clean, pinned Repository source per Trial."""

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from crucible.evals.manifests import EvalCaseDefinition


@dataclass(frozen=True)
class PreparedFixture:
    root: Path
    source_commit: str
    content_digest: str


class FixturePreparer:
    def __init__(self, data_dir: Path) -> None:
        self._root = data_dir / "eval-fixtures"

    def prepare(self, case: EvalCaseDefinition, trial_id: UUID) -> PreparedFixture:
        destination = self._root / str(trial_id)
        self._root.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise ValueError(f"Trial fixture already exists: {trial_id}")
        try:
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--quiet",
                    "--no-checkout",
                    str(case.fixture_path),
                    str(destination),
                ],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(destination),
                    "checkout",
                    "--quiet",
                    "--detach",
                    "--force",
                    case.fixture_revision,
                ],
                check=True,
                capture_output=True,
            )
            commit = _git(destination, "rev-parse", "HEAD")
            if commit != case.fixture_revision:
                raise ValueError("fixture checkout did not match pinned revision")
            digest = _content_digest(destination)
            if _git(destination, "status", "--porcelain"):
                raise ValueError("fresh fixture checkout is not clean")
            return PreparedFixture(destination, commit, digest)
        except (OSError, subprocess.CalledProcessError, ValueError):
            # Leave the failed copy for diagnosis; the Trial records a failed state.
            raise


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.DEVNULL
    ).strip()


def _content_digest(root: Path) -> str:
    files = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"])
    digest = hashlib.sha256()
    for raw in filter(None, files.split(b"\0")):
        relative = raw.decode("utf-8", errors="surrogateescape")
        path = root / relative
        if path.is_symlink():
            raise ValueError("fixture symlink is forbidden")
        if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("fixture contains an invalid tracked path")
        digest.update(raw)
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()
