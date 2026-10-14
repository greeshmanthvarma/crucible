"""Startup source identity; never follows later edits or reads private state."""

import asyncio
import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID


def _json_value(value: object) -> object:
    if isinstance(value, (UUID, Path)):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    raise TypeError("Unsupported evidence value")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_value,
    ).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


@dataclass(frozen=True)
class RuntimeIdentity:
    commit: str
    source_digest: str
    lock_digest: str
    schema_version: str = "1"

    @property
    def manifest_digest(self) -> str:
        return digest({**asdict(self), "schema_version": 1})


async def _git(root: Path, *args: str) -> bytes:
    process = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await process.communicate()
    if process.returncode:
        raise ValueError("Cannot identify the running source checkout")
    return stdout


def _read_tracked(root: Path, name: str) -> dict[str, object]:
    path = root / name
    # Hash link text instead of following a tracked symlink into private data.
    for parent in path.parents:
        if parent == root:
            break
        if parent.is_symlink():
            return {"path": name, "unsupported": "symlink_parent"}
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {"path": name, "exists": False}
    except OSError:
        if path.is_symlink():
            return {"path": name, "symlink": os.readlink(path)}
        return {"path": name, "unsupported": "unreadable"}
    with os.fdopen(descriptor, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            return {"path": name, "unsupported": "not_regular"}
        hashed = hashlib.sha256()
        while chunk := source.read(64 * 1024):
            hashed.update(chunk)
        after = os.fstat(source.fileno())
    return {
        "path": name,
        "digest": hashed.hexdigest(),
        "mode": stat.S_IMODE(before.st_mode),
        "stable": (before.st_size, before.st_mtime_ns)
        == (after.st_size, after.st_mtime_ns),
    }


async def capture_runtime_identity(root: Path) -> RuntimeIdentity:
    root = Path(
        (await _git(root, "rev-parse", "--show-toplevel")).decode().strip()
    ).resolve()
    commit = (await _git(root, "rev-parse", "HEAD")).decode().strip()
    names = (await _git(root, "ls-files", "--cached", "-z")).decode().split("\0")
    # Ignore private/ignored files even if someone previously tracked them.
    candidates = [
        name
        for name in names
        if name
        and not any(
            part == ".env" or part.startswith(".env.") for part in Path(name).parts
        )
    ]
    ignored: set[str] = set()
    if candidates:
        process = await asyncio.create_subprocess_exec(
            "git",
            "check-ignore",
            "--no-index",
            "-z",
            "--stdin",
            cwd=root,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await process.communicate(
            "\0".join(candidates).encode() + b"\0"
        )
        if process.returncode not in (0, 1):
            raise ValueError("Cannot filter private source inputs")
        ignored = set(stdout.decode().split("\0"))
    files = await asyncio.to_thread(
        lambda: [
            _read_tracked(root, name) for name in sorted(set(candidates) - ignored)
        ]
    )
    # Including index identity distinguishes staged dirty state from the same files
    # staged differently. No untracked/ignored contents enter the identity.
    index_rows = (await _git(root, "ls-files", "--stage", "-z")).decode().split("\0")
    included = set(candidates) - ignored
    index = digest(
        [row for row in index_rows if "\t" in row and row.split("\t", 1)[1] in included]
    )
    lock_names = {
        "uv.lock",
        "pnpm-lock.yaml",
        "package-lock.json",
        "poetry.lock",
        "yarn.lock",
    }
    locks = [file for file in files if Path(str(file["path"])).name in lock_names]
    return RuntimeIdentity(
        commit,
        digest({"schema_version": 1, "files": files, "index": index}),
        digest({"schema_version": 1, "locks": locks}),
    )
