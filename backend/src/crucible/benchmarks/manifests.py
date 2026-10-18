"""Validate local curator inputs and export source without inherited history."""

import hashlib
import json
import os
import subprocess
from pathlib import Path

from crucible.benchmarks.contracts import BenchmarkCase, SolverCase


def _git(root: Path, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.check_output(
        ["git", "-c", "core.hooksPath=/dev/null", "-C", str(root), *args],
        env={
            "PATH": os.defpath,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        },
        stderr=subprocess.PIPE,
        input=data,
    )


def _files(root: Path) -> list[Path]:
    if root.is_symlink() or not (root / ".git").is_dir():
        raise ValueError("fixture must be a local repository")
    paths = []
    for entry in _git(root, "ls-tree", "-rz", "HEAD").split(b"\0"):
        if not entry:
            continue
        metadata, raw = entry.split(b"\t", 1)
        mode = metadata.split()[0]
        relative = Path(raw.decode("utf-8"))
        path = root / relative
        if (
            mode not in (b"100644", b"100755")
            or relative.is_absolute()
            or ".." in relative.parts
            or any(part.lower() == ".git" for part in relative.parts)
            or any(p.is_symlink() for p in [path, *path.parents])
            or not path.is_file()
            or not path.resolve().is_relative_to(root.resolve())
        ):
            raise ValueError("unsupported fixture path or symlink")
        paths.append(relative)
    return paths


def fixture_digest(root: Path) -> str:
    """Hash committed file names, executable modes and actual bytes."""
    digest = hashlib.sha256()
    for relative in _files(root):
        path = root / relative
        digest.update(relative.as_posix().encode() + b"\0")
        digest.update(b"x" if path.stat().st_mode & 0o111 else b"-")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _validate(case: BenchmarkCase) -> None:
    root = case.fixture_path
    for path in (root, case.prompt_path, case.grader_input_path):
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError("inputs require absolute paths without parent traversal")
        if any(p.is_symlink() for p in [path, *path.parents]):
            raise ValueError("symlink inputs are forbidden")
    _files(root)
    for protected in (case.prompt_path, case.grader_input_path):
        if protected.resolve().is_relative_to(root.resolve()):
            raise ValueError("protected input inside fixture")
    if _git(root, "rev-parse", "HEAD").decode().strip() != case.base_commit:
        raise ValueError("fixture commit mismatch")
    if _git(root, "status", "--porcelain", "--untracked-files=all", "--ignored"):
        raise ValueError("fixture is not clean")
    for entry in _git(root, "ls-tree", "-rz", "HEAD").split(b"\0"):
        if not entry:
            continue
        metadata, raw = entry.split(b"\t", 1)
        mode, _, blob = metadata.split()
        path = root / raw.decode("utf-8")
        if path.is_symlink() or not path.is_file():
            raise ValueError("unsupported pinned fixture path")
        content = path.read_bytes()
        actual = hashlib.sha1(
            b"blob " + str(len(content)).encode() + b"\0" + content
        ).hexdigest()
        executable = bool(path.stat().st_mode & 0o111)
        if actual != blob.decode() or executable != (mode == b"100755"):
            raise ValueError("fixture bytes or mode differ from pinned commit")
    if fixture_digest(root) != case.fixture_digest:
        raise ValueError("fixture digest mismatch")
    for path, expected in (
        (case.prompt_path, case.prompt_digest),
        (case.grader_input_path, case.grader_input_digest),
    ):
        if (
            not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("input digest mismatch")
    if case.prompt_path.stat().st_size > 1_048_576:
        raise ValueError("prompt exceeds byte limit")
    case.prompt_path.read_text(encoding="utf-8")
    for relative in _files(root):
        if relative.name in {case.grader_input_path.name, "private-grader.json"}:
            raise ValueError("protected file in fixture")


def load_case(path: Path) -> BenchmarkCase:
    case = BenchmarkCase.model_validate_json(path.read_bytes())
    _validate(case)
    return case


def case_digest(case: BenchmarkCase) -> str:
    _validate(case)
    semantic = case.model_dump(
        mode="json",
        exclude={
            "fixture_path",
            "prompt_path",
            "grader_input_path",
        },
    )
    return hashlib.sha256(json.dumps(semantic, sort_keys=True).encode()).hexdigest()


def export_solver_case(case: BenchmarkCase, destination: Path) -> SolverCase:
    _validate(case)
    if not destination.is_absolute() or ".." in destination.parts:
        raise ValueError("invalid destination")
    if any(p.is_symlink() for p in [destination, *destination.parents]):
        raise ValueError("symlink destination")
    target = destination.resolve()
    for source in (case.fixture_path, case.prompt_path, case.grader_input_path):
        if source.resolve().is_relative_to(target) or target.is_relative_to(
            source.resolve()
        ):
            raise ValueError("destination overlaps input")
    destination.mkdir(parents=True, exist_ok=False)
    for relative in _files(case.fixture_path):
        source = case.fixture_path / relative
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(source.read_bytes())
        output.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
    _git(destination, "init", "-q")
    for relative in _files(case.fixture_path):
        path = destination / relative
        blob = (
            _git(destination, "hash-object", "-w", "--stdin", data=path.read_bytes())
            .decode()
            .strip()
        )
        mode = "100755" if path.stat().st_mode & 0o111 else "100644"
        _git(
            destination,
            "update-index",
            "--add",
            "--cacheinfo",
            mode,
            blob,
            relative.as_posix(),
        )
    _git(
        destination,
        "-c",
        "user.name=Benchmark",
        "-c",
        "user.email=benchmark@localhost",
        "commit",
        "--allow-empty",
        "-qm",
        "Solver base",
    )
    return SolverCase(
        case.case_id,
        case.base_commit,
        destination,
        case.prompt_path.read_text(encoding="utf-8"),
        case.image,
        case.platform,
    )
