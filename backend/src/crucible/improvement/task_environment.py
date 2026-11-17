"""Trial-container bootstrap; keep syntax and stdlib calls Python 3.6 compatible.

Runs in the same network-none, resource-limited command container as the solver
command. The image supplies dependencies; existing workspace files always win.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List


def _git(root: Path, args: List[str]) -> bytes:
    return subprocess.run(
        ["git", "-c", "safe.directory=" + str(root), "-C", str(root)] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout


def _checked(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts or ".git" in path.parts:
        raise ValueError("unsafe runtime artifact path")
    if root.resolve() != root:
        raise ValueError("symlink runtime artifact root")
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("symlink runtime artifact path")
    return current


def _runtime_file(path: Path) -> bool:
    return (
        path.suffix in (".so", ".pyd", ".dll", ".dylib", ".py")
        or ".so." in path.name
        or any(p.endswith((".egg-info", ".dist-info")) for p in path.parts[:-1])
    ) and not any(p in ("build", "__pycache__") for p in path.parts)


def prepare_runtime_artifacts(image_root: Path, workspace: Path) -> None:
    """Copy missing ignored runtime files; never overwrite or restore source."""
    candidates = [
        os.fsdecode(name)
        for name in _git(
            image_root,
            ["ls-files", "--others", "--ignored", "--exclude-standard", "-z"],
        ).split(b"\0")
        if name and _runtime_file(Path(os.fsdecode(name)))
    ]
    if not candidates:
        return
    for relative in candidates:
        _checked(image_root, relative)
        _checked(workspace, relative)
    ignored = subprocess.run(
        [
            "git",
            "-c",
            "safe.directory=" + str(workspace),
            "-C",
            str(workspace),
            "check-ignore",
            "--stdin",
            "-z",
        ],
        input=b"\0".join(os.fsencode(p) for p in candidates) + b"\0",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if ignored.returncode not in (0, 1):
        raise ValueError("cannot verify workspace runtime artifacts are ignored")
    allowed = set(ignored.stdout.split(b"\0"))
    for relative in candidates:
        source = _checked(image_root, relative)
        target = _checked(workspace, relative)
        if os.fsencode(relative) not in allowed or target.exists():
            continue
        if not source.is_file():
            raise ValueError("runtime artifact is not a regular file")
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with source.open("rb") as incoming, target.open("xb") as outgoing:
                shutil.copyfileobj(incoming, outgoing)
        except FileExistsError:
            # Another command may already have hydrated this dependency.
            continue


def main(argv: List[str]) -> None:
    if len(argv) < 3:
        raise ValueError("image root, workspace and command are required")
    prepare_runtime_artifacts(Path(argv[0]), Path(argv[1]))
    os.execvpe(argv[2], argv[2:], os.environ)


if __name__ == "__main__":
    main(sys.argv[1:])
