"""Snapshot regular source files using controller-owned Git metadata."""

import hashlib
import os
import re
import tempfile
import zlib
from pathlib import Path

from crucible.benchmarks.contracts import SolutionPatch
from crucible.benchmarks.manifests import _git


def collect_solution_patch(
    workspace: Path,
    expected_base_tree: str,
    destination: Path,
    *,
    base_commit: str,
    max_bytes: int = 8_388_608,
    base_repository: Path | None = None,
) -> SolutionPatch:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_base_tree) or max_bytes <= 0:
        raise ValueError("invalid base tree or patch limit")
    source_repository = base_repository or workspace
    if workspace.is_symlink() or not (source_repository / ".git").is_dir():
        raise ValueError("invalid workspace")
    if destination.resolve().is_relative_to(workspace.resolve()):
        raise ValueError("patch destination must be outside workspace")
    if any(p.is_symlink() for p in [destination, *destination.parents]):
        raise ValueError("symlink patch destination")
    metadata = source_repository / ".git"
    if metadata.is_symlink():
        raise ValueError("symlink Git metadata")
    head = _local_read(metadata, "HEAD").decode().strip()
    if head.startswith("ref: "):
        ref = head[5:]
        if not ref.startswith("refs/heads/"):
            raise ValueError("unsupported HEAD ref")
        _validate_path(ref)
        head = _local_read(metadata, ref).decode().strip()
    commit_data = _object(metadata, head, "commit")
    if commit_data.splitlines()[0] != b"tree " + expected_base_tree.encode():
        raise ValueError("changed base tree")
    with tempfile.TemporaryDirectory(prefix="crucible-patch-") as temporary:
        root = Path(temporary)
        _git(root, "init", "-q")
        entries: dict[str, tuple[str, bytes]] = {}
        for name, mode, blob, content in _base_files(metadata, expected_base_tree):
            actual = (
                _git(root, "hash-object", "-w", "--stdin", data=content)
                .decode()
                .strip()
            )
            if actual != blob:
                raise ValueError("base object integrity failure")
            _git(root, "update-index", "--add", "--cacheinfo", mode, blob, name)
            entries[name] = (mode, content)
        if _git(root, "write-tree").decode().strip() != expected_base_tree:
            raise ValueError("base tree integrity failure")
        # Copy only regular supported files; the source index and config are never used.
        for directory, directories, files in os.walk(workspace, followlinks=False):
            if Path(directory) == workspace:
                directories[:] = [name for name in directories if name != ".git"]
            for name in directories:
                if (Path(directory) / name).is_symlink():
                    raise ValueError("unsupported symlink")
            for name in files:
                if Path(directory) == workspace and name == ".git":
                    continue
                source = Path(directory) / name
                relative = source.relative_to(workspace)
                _validate_path(relative.as_posix())
                if source.is_symlink() or not source.is_file():
                    raise ValueError("unsupported symlink or special file")
                output = root / relative
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(source.read_bytes())
                output.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
        untracked = _git(root, "ls-files", "--others", "--exclude-standard", "-z")
        names = set(entries) | {
            raw.decode("utf-8") for raw in untracked.split(b"\0") if raw
        }
        for name in entries:
            if not (root / name).is_file():
                _text(entries[name][1])
                _git(root, "update-index", "--force-remove", "--", name)
        for name in sorted(names):
            path = root / name
            if not path.is_file():
                _text(entries[name][1])
                _git(root, "update-index", "--force-remove", "--", name)
                continue
            content = path.read_bytes()
            mode = "100755" if path.stat().st_mode & 0o111 else "100644"
            if entries.get(name) == (mode, content):
                continue
            _text(content)
            if name in entries:
                _text(entries[name][1])
            blob = (
                _git(root, "hash-object", "-w", "--stdin", data=content)
                .decode()
                .strip()
            )
            mode = "100755" if path.stat().st_mode & 0o111 else "100644"
            _git(root, "update-index", "--add", "--cacheinfo", mode, blob, name)
        patch = _git(
            root,
            "diff",
            "--cached",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--binary",
            expected_base_tree,
            "--",
        )
        if len(patch) > max_bytes:
            raise ValueError("patch exceeds byte limit")
        patch.decode("utf-8")
        # Verify patch application reproduces the captured tree exactly.
        final_tree = _git(root, "write-tree")
        _git(root, "read-tree", expected_base_tree)
        if patch:
            _git(root, "apply", "--cached", "--whitespace=nowarn", "-", data=patch)
        if _git(root, "write-tree") != final_tree:
            raise ValueError("patch application integrity failure")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as stream:
        stream.write(patch)
    return SolutionPatch(
        destination, hashlib.sha256(patch).hexdigest(), base_commit, len(patch)
    )


def _validate_path(name: str) -> None:
    path = Path(name)
    if (
        path.is_absolute()
        or ".." in path.parts
        or any(p.lower() == ".git" for p in path.parts)
    ):
        raise ValueError("unsafe patch path")


def _text(content: bytes) -> None:
    if b"\0" in content:
        raise ValueError("unsupported binary content")
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("unsupported non-UTF8 content") from error


def _local_read(root: Path, relative: str) -> bytes:
    _validate_path(relative)
    path = root / relative
    if any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("symlink Git metadata")
    if not path.is_file():
        raise ValueError("missing loose Git metadata; packed repositories unsupported")
    return path.read_bytes()


def _object(metadata: Path, sha: str, kind: str) -> bytes:
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("invalid Git object identity")
    raw = zlib.decompress(_local_read(metadata, f"objects/{sha[:2]}/{sha[2:]}"))
    if hashlib.sha1(raw).hexdigest() != sha:
        raise ValueError("Git object integrity failure")
    header, content = raw.split(b"\0", 1)
    if header != f"{kind} {len(content)}".encode():
        raise ValueError("unexpected Git object type")
    return content


def _base_files(
    metadata: Path, tree: str, prefix: str = ""
) -> list[tuple[str, str, str, bytes]]:
    data = _object(metadata, tree, "tree")
    files = []
    while data:
        header, rest = data.split(b"\0", 1)
        mode, raw_name = header.split(b" ", 1)
        sha, data = rest[:20].hex(), rest[20:]
        name = prefix + raw_name.decode("utf-8")
        _validate_path(name)
        if mode == b"40000":
            files.extend(_base_files(metadata, sha, name + "/"))
        elif mode in {b"100644", b"100755"}:
            files.append((name, mode.decode(), sha, _object(metadata, sha, "blob")))
        else:
            raise ValueError("unsupported base symlink or submodule")
    return files
