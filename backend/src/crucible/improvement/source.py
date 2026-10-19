"""Bounded byte-only harness inspection. No candidate imports or subprocesses."""

import hashlib
import os
import re
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from crucible.improvement.contracts import EDITABLE_PATHS, HarnessSnapshot
from crucible.repair.identity import canonical_json

MAX_FILE = 2 * 1024 * 1024
MAX_EXPORT = 32 * 1024 * 1024
MAX_FILES = 4096


class ScopeRejected(ValueError):
    def __init__(self, reason: str, paths: tuple[str, ...] = ()) -> None:
        self.reason, self.paths = reason, paths
        super().__init__(reason + ": " + ", ".join(paths))


def git_bytes(root: Path, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.check_output(
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "diff.external=",
            "-c",
            "core.attributesFile=/dev/null",
            "-C",
            str(root),
            *args,
        ],
        env={
            "PATH": os.defpath,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        },
        stderr=subprocess.PIPE,
        timeout=30,
        input=data,
    )


def safe_path(path: Path) -> None:
    if (
        not path.is_absolute()
        or ".." in path.parts
        or any(p.is_symlink() for p in (path, *path.parents))
    ):
        raise ScopeRejected("symlink or invalid path")


def _name(raw: bytes) -> str:
    name = raw.decode("utf-8")
    parts = Path(name).parts
    if (
        not parts
        or Path(name).is_absolute()
        or ".." in parts
        or any(
            p.lower() == ".git" or (p.startswith(".env") and p != ".env.example")
            for p in parts
        )
    ):
        raise ScopeRejected("unsafe source path", (name,))
    return name


@dataclass(frozen=True)
class SourceInspection:
    source_digest: str
    changed_paths: tuple[str, ...]
    files: dict[str, tuple[int, bytes]]
    base_files: dict[str, tuple[int, bytes]]


def _digest(files: dict[str, tuple[int, bytes]]) -> str:
    digest = hashlib.sha256()
    for name, (mode, data) in sorted(files.items()):
        digest.update(canonical_json([name, mode, hashlib.sha256(data).hexdigest()]))
    return digest.hexdigest()


def inspect_source(root: Path, baseline_commit: str) -> SourceInspection:
    safe_path(root)
    if not re.fullmatch("[0-9a-f]{40}", baseline_commit):
        raise ScopeRejected("invalid baseline commit")
    base = {}
    entries = git_bytes(root, "ls-tree", "-rz", baseline_commit).split(b"\0")
    if len(entries) > MAX_FILES + 1:
        raise ScopeRejected("base file count exceeds limit")
    for entry in filter(None, entries):
        meta, raw = entry.split(b"\t", 1)
        mode, _, blob = meta.split()
        name = _name(raw)
        if mode not in (b"100644", b"100755"):
            raise ScopeRejected("unsupported base type", (name,))
        size = int(git_bytes(root, "cat-file", "-s", blob.decode()).strip())
        if size > MAX_FILE:
            raise ScopeRejected("source file exceeds limit", (name,))
        base[name] = (int(mode, 8), git_bytes(root, "cat-file", "blob", blob.decode()))
    raw_names = git_bytes(root, "ls-files", "-co", "--exclude-standard", "-z").split(
        b"\0"
    )
    names = set(base) | {_name(raw) for raw in raw_names if raw}
    ignored = git_bytes(
        root, "ls-files", "--others", "--ignored", "--exclude-standard", "-z"
    ).split(b"\0")
    if any(
        raw.startswith(b"backend/src/") and b"__pycache__" not in raw.split(b"/")
        for raw in ignored
        if raw
    ):
        raise ScopeRejected("ignored secret in source export")
    if len(names) > MAX_FILES:
        raise ScopeRejected("source file count exceeds limit")
    files = {}
    total = 0
    for name in sorted(names):
        path = root / name
        safe_path(path)
        if not path.exists():
            continue
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_FILE:
                raise ScopeRejected("unsupported source type or file limit", (name,))
            data = source.read(MAX_FILE + 1)
            after = os.fstat(source.fileno())
            if len(data) > MAX_FILE or (before.st_size, before.st_mtime_ns) != (
                after.st_size,
                after.st_mtime_ns,
            ):
                raise ScopeRejected("source changed during inspection", (name,))
        total += len(data)
        if total > MAX_EXPORT:
            raise ScopeRejected("source byte limit exceeded")
        files[name] = (0o100755 if before.st_mode & 0o111 else 0o100644, data)
    changed = tuple(sorted(n for n in names if files.get(n) != base.get(n)))
    return SourceInspection(_digest(files), changed, files, base)


def stage_harness(
    worktree: Path, baseline_commit: str, output: Path, editable_paths: tuple[str, ...]
) -> HarnessSnapshot:
    safe_path(output)
    if output.resolve().is_relative_to(
        worktree.resolve()
    ) or worktree.resolve().is_relative_to(output.resolve()):
        raise ScopeRejected("snapshot overlaps source")
    if editable_paths != EDITABLE_PATHS:
        raise ScopeRejected("editable policy mismatch")
    inspection = inspect_source(worktree, baseline_commit)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    patch = _patch(inspection)
    if len(patch) > MAX_EXPORT:
        raise ScopeRejected("patch exceeds limit")
    (output / "candidate.diff").write_bytes(patch)
    protected = tuple(p for p in inspection.changed_paths if p not in editable_paths)
    if protected:
        (output / "rejected.json").write_bytes(
            canonical_json({"reason": "protected change", "paths": protected})
        )
        raise ScopeRejected("protected change", protected)
    exported = {
        name: value
        for name, value in inspection.files.items()
        if solver_runtime_path(name)
    }
    harness = output / "harness"
    harness.mkdir()
    for name, (mode, data) in exported.items():
        path = harness / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(0o755 if mode == 0o100755 else 0o644)
    result = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=baseline_commit,
        source_digest=inspection.source_digest,
        export_digest=_digest(exported),
        patch_digest=hashlib.sha256(patch).hexdigest()
        if inspection.changed_paths
        else None,
        kind="candidate" if inspection.changed_paths else "baseline",
    )
    (output / "snapshot.json").write_text(result.model_dump_json())
    # Re-read to detect concurrent source changes before admitting immutable bytes.
    if inspect_source(worktree, baseline_commit).source_digest != result.source_digest:
        raise ScopeRejected("source changed during export")
    return result


def _patch(inspection: SourceInspection) -> bytes:
    # Build both trees from inspected bytes in new controller-owned metadata.
    with tempfile.TemporaryDirectory(prefix="crucible-source-") as temporary:
        root = Path(temporary)
        git_bytes(root, "init", "-q")
        trees = []
        for files in (inspection.base_files, inspection.files):
            git_bytes(root, "read-tree", "--empty")
            for name, (mode, data) in sorted(files.items()):
                blob = (
                    git_bytes(root, "hash-object", "-w", "--stdin", data=data)
                    .decode()
                    .strip()
                )
                git_bytes(
                    root,
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"{mode:o}",
                    blob,
                    name,
                )
            trees.append(git_bytes(root, "write-tree").decode().strip())
        return git_bytes(
            root, "diff", "--binary", "--no-ext-diff", "--no-textconv", *trees, "--"
        )


# Frozen positive runtime mapping. Host orchestration/evaluators are absent.
_SOLVER_PACKAGES = (
    "artifacts",
    "context",
    "domain",
    "engine",
    "models",
    "sandbox",
    "storage",
    "tools",
    "workspaces",
)
_SOLVER_FILES = frozenset(
    {
        "__init__.py",
        "benchmarks/__init__.py",
        "benchmarks/contracts.py",
        "application/__init__.py",
        "application/ports.py",
        "application/idempotency.py",
        "application/approval_service.py",
        "application/artifact_service.py",
        "application/validation_service.py",
        "application/worker_container.py",
        "repair/__init__.py",
        "repair/identity.py",
        "repair/evidence.py",
        "repair/cases.py",
        "repair/gateway.py",
        "repair/worker_protocol.py",
        "repair/trial_contracts.py",
        "improvement/__init__.py",
        "improvement/contracts.py",
        "improvement/worker_protocol.py",
        "improvement/worker.py",
        "improvement/worker_commands.py",
        "improvement/worker_gateway.py",
    }
)


def solver_runtime_path(name: str) -> bool:
    if name in ("backend/pyproject.toml", "backend/uv.lock"):
        return True
    prefix = "backend/src/crucible/"
    if not name.startswith(prefix):
        return False
    relative = name[len(prefix) :]
    return relative in _SOLVER_FILES or any(
        relative.startswith(p + "/") for p in _SOLVER_PACKAGES
    )
