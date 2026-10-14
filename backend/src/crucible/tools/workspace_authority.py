"""Controller-owned workspace access and mutation admission."""

import os
import re
import tempfile
from pathlib import Path, PurePosixPath

from crucible.workspaces.git import GitClient, GitResult, SubprocessGitClient


class WorkspacePathResolver:
    def __init__(self, workspace: Path) -> None:
        self.root = workspace.resolve(strict=True)

    def resolve(self, value: str, *, allow_missing: bool = False) -> Path:
        logical = PurePosixPath(value)
        if logical.is_absolute() or ".." in logical.parts:
            raise ValueError("Workspace path must be relative and cannot traverse")
        candidate = self.root.joinpath(*logical.parts)
        if candidate.is_symlink() and not candidate.exists():
            raise ValueError("Workspace path contains a dangling symlink")
        if candidate.exists():
            resolved = candidate.resolve(strict=True)
            self._require_contained(resolved)
            return resolved
        if not allow_missing:
            raise ValueError(f"Workspace path does not exist: {value}")
        parent = candidate.parent
        missing: list[str] = [candidate.name]
        while not parent.exists():
            if parent.is_symlink():
                raise ValueError("Workspace path contains a dangling symlink")
            missing.append(parent.name)
            parent = parent.parent
        resolved_parent = parent.resolve(strict=True)
        self._require_contained(resolved_parent)
        return resolved_parent.joinpath(*reversed(missing))

    def _require_contained(self, path: Path) -> None:
        if not path.is_relative_to(self.root):
            raise ValueError("Workspace path escapes the Task Workspace")


def normalize_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError("Workspace path must be relative and cannot traverse")
    if any(part.casefold() == ".git" for part in path.parts) or "\x00" in value:
        raise ValueError("Mutation cannot target Git metadata")
    return str(path)


def patch_paths(patch: str) -> tuple[str, ...]:
    """Parse targets independently of editable tool behavior; fail closed.

    Count hunk lines so a removed line beginning with -- cannot become a header.
    Quoted paths, binary patches, renames/copies and symlink modes are unsupported.
    """
    paths: list[str] = []
    old_remaining = new_remaining = 0
    pending_old = False
    pairs = 0
    for line in patch.splitlines():
        if old_remaining or new_remaining:
            if line.startswith(r"\ No newline"):
                continue
            if not line or line[0] not in " +-":
                raise ValueError("Malformed patch hunk")
            if line[0] in " -":
                old_remaining -= 1
            if line[0] in " +":
                new_remaining -= 1
            if old_remaining < 0 or new_remaining < 0:
                raise ValueError("Malformed patch hunk counts")
            continue
        if line.startswith("@@ "):
            match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if match is None or pending_old or not pairs:
                raise ValueError("Malformed patch hunk header")
            old_remaining = int(match[2] or "1")
            new_remaining = int(match[4] or "1")
            continue
        if line.startswith(("--- ", "+++ ")):
            is_old = line.startswith("--- ")
            if is_old == pending_old:
                raise ValueError("Patch requires paired old/new headers")
            pending_old = is_old
            if not is_old:
                pairs += 1
            value = line[4:].split("\t", 1)[0]
            if value == "/dev/null":
                continue
            if (
                value.startswith('"')
                or "\\" in value
                or any(char.isspace() for char in value)
            ):
                raise ValueError("Quoted/escaped patch paths are unsupported")
            expected_prefix = "a/" if is_old else "b/"
            if not value.startswith(expected_prefix):
                raise ValueError("Patch paths require standard a/ and b/ prefixes")
            value = normalize_path(value[2:])
            if value not in paths:
                paths.append(value)
            continue
        if line.startswith("diff --git "):
            names = line[11:].split()
            if len(names) != 2 or any('"' in name or "\\" in name for name in names):
                raise ValueError("Unsupported diff header")
            for name in names:
                if not name.startswith(("a/", "b/")):
                    raise ValueError("Unsupported diff path")
                value = normalize_path(name[2:])
                if value not in paths:
                    paths.append(value)
        elif line.startswith(
            (
                "rename ",
                "copy ",
                "GIT binary",
                "Binary files",
                "***",
                "new mode",
                "old mode",
            )
        ):
            raise ValueError("Unsupported patch metadata")
        elif line.startswith(("new file mode ", "deleted file mode ", "index ")):
            if "120000" in line or "160000" in line:
                raise ValueError("Patch cannot create symlinks or submodules")
        elif line and not line.startswith(r"\ No newline"):
            raise ValueError("Unsupported patch header")
    if not paths or not pairs or pending_old or old_remaining or new_remaining:
        raise ValueError("Patch does not contain complete file headers/hunks")
    return tuple(paths)


class PatchApplicationFailed(RuntimeError):
    pass


class WorkspaceAuthority:
    def __init__(
        self,
        workspace: Path,
        admitted_paths: frozenset[str],
        max_write_bytes: int,
        *,
        git: GitClient | None = None,
    ) -> None:
        self._resolver = WorkspacePathResolver(workspace)
        self._paths = frozenset(normalize_path(path) for path in admitted_paths)
        self._max_write_bytes = max_write_bytes
        self._git = git or SubprocessGitClient()

    def read_text(self, path: str) -> str:
        return self._resolver.resolve(path).read_text(encoding="utf-8")

    def validate_target(self, path: str) -> Path:
        normalized = normalize_path(path)
        if normalized not in self._paths:
            raise ValueError("Mutation path was not admitted by the controller")
        logical = self._resolver.root / normalized
        for component in (logical, *logical.parents):
            if component == self._resolver.root:
                break
            if component.is_symlink():
                raise ValueError("Mutation rejects symlink paths")
        return self._resolver.resolve(normalized, allow_missing=True)

    def write_text(self, path: str, content: str) -> int:
        encoded = content.encode()
        if len(encoded) > self._max_write_bytes:
            raise ValueError("Content exceeds limit")
        target = self.validate_target(path)
        parent = target.parent.resolve(strict=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            if (
                self.validate_target(path) != target
                or target.parent.resolve(strict=True) != parent
            ):
                raise ValueError("Write parent changed during operation")
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return len(encoded)

    async def apply_patch(self, patch: str) -> GitResult:
        if len(patch.encode()) > self._max_write_bytes:
            raise ValueError("Patch exceeds limit")
        paths = patch_paths(patch)
        for path in paths:
            self.validate_target(path)
        checked = await self._git.apply_patch(self._resolver.root, patch, check=True)
        if checked.returncode != 0:
            return checked
        for path in paths:
            self.validate_target(path)
        applied = await self._git.apply_patch(self._resolver.root, patch, check=False)
        if applied.returncode != 0:
            raise PatchApplicationFailed(applied.stderr[:4000])
        return applied
