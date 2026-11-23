"""Draw a balanced screening pool from pinned SWE-bench Verified metadata."""

import argparse
import hashlib
import json
import os
import random
import re
import subprocess
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path

import yaml  # type: ignore[import-untyped]

SEED = "targeted-dev-v1-screen"
PIN = "3d07b464b7b311a0cbfb5ed5b2d8a3b96f84a33d"
SOURCE = "https://github.com/SWE-bench/swe-bench-tasks.git"
PROJECTS = {
    "astropy__astropy": "astropy",
    "pytest-dev__pytest": "pytest",
    "scikit-learn__scikit-learn": "scikit-learn",
    "sphinx-doc__sphinx": "sphinx",
    "sympy__sympy": "sympy",
}
VERIFIED = "SWE-bench/SWE-bench_Verified"
ROOT = Path(__file__).resolve().parent
CASE_ID = re.compile(r"(?:" + "|".join(PROJECTS) + r")-\d+")


@dataclass(frozen=True)
class PoolCase:
    case_id: str
    project: str
    patch_changed_lines: int


def draw_pool(
    candidates: Iterable[PoolCase], excluded: set[str], *, seed: str = SEED
) -> tuple[list[PoolCase], list[PoolCase]]:
    eligible = [
        case
        for case in candidates
        if case.case_id not in excluded and case.patch_changed_lines <= 40
    ]
    pool, remainder = [], []
    for project in PROJECTS.values():
        rows = sorted(
            (case for case in eligible if case.project == project),
            key=lambda case: case.case_id,
        )
        random.Random(int(hashlib.sha256(seed.encode()).hexdigest(), 16)).shuffle(rows)
        if len(rows) < 5:
            raise ValueError(f"Fewer than five eligible cases for {project}")
        pool.extend(rows[:5])
        remainder.extend(rows[5:])
    return pool, remainder


def case_ids(value: object) -> Iterator[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield from case_ids(key)
            yield from case_ids(child)
    elif isinstance(value, list):
        for child in value:
            yield from case_ids(child)
    elif isinstance(value, str) and CASE_ID.fullmatch(value):
        yield value


def previous_ids(history: Path, run: Path) -> set[str]:
    excluded: set[str] = set()
    records = {
        "registry.json",
        "replacements.json",
        "experiment-manifest.json",
        "case.json",
        "calibration-case.json",
    }
    # Walk operator metadata and skip fixture/source trees and protected data.
    for directory, children, files in os.walk(history, followlinks=False):
        path = Path(directory)
        if path.name == "cases":
            excluded.update(
                name for name in children + files if CASE_ID.fullmatch(name)
            )
            children.clear()
            continue
        if path.name == "registry":
            excluded.update(
                identity
                for name in children + files
                if CASE_ID.fullmatch(identity := Path(name).stem)
            )
        children[:] = [
            name
            for name in children
            if path / name != run
            and name
            not in {
                "fixture",
                "protected",
                "task-source",
                "baseline-repo",
                "runtime",
                "node_modules",
                "evaluator",
                "driver-env",
                "workspace",
                "solver",
                "snapshot",
                "harness",
                "grading",
                "private",
            }
            and not name.startswith(".")
            and not name.endswith("-worktree")
        ]
        for name in files:
            if (path.name == "registry" and name.endswith(".json")) or name in records:
                record = path / name
                if not record.is_symlink():
                    excluded.update(case_ids(json.loads(record.read_text())))
    return excluded


def git(source: Path, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.check_output(
        ["git", "-C", str(source), *args],
        input=data,
        timeout=180,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1"},
    )


def read_blobs(source: Path, objects: Iterable[str]) -> dict[str, bytes]:
    oids = list(dict.fromkeys(objects))
    if not oids:
        return {}
    queries = ("\n".join(oids) + "\n").encode()
    availability = git(source, "cat-file", "--batch-check", data=queries)
    missing = [
        row.split()[0] for row in availability.splitlines() if row.endswith(b" missing")
    ]
    if missing:
        git(
            source,
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "--no-recurse-submodules",
            "--filter=blob:none",
            "origin",
            "--stdin",
            data=b"\n".join(missing) + b"\n",
        )
    blobs = BytesIO(git(source, "cat-file", "--batch", data=queries))
    result = {}
    for oid in oids:
        identity, kind, size = blobs.readline().split()
        if identity.decode() != oid or kind != b"blob":
            raise ValueError("Dataset object differs from the pinned blob")
        result[oid] = blobs.read(int(size))
        if blobs.read(1) != b"\n":
            raise ValueError("Incomplete dataset blob")
    return result


def changed_lines(source: Path, patch: bytes) -> int:
    stats = git(source, "apply", "--allow-empty", "--numstat", "-z", data=patch)
    total = 0
    for row in stats.rstrip(b"\0").split(b"\0") if stats else ():
        added, deleted, _ = row.split(b"\t", maxsplit=2)
        if added == b"-" or deleted == b"-":
            return 41  # Binary changes do not qualify for the textual line limit.
        total += int(added) + int(deleted)
    return total


def dataset_candidates(source: Path, excluded: set[str]) -> Iterator[PoolCase]:
    files: dict[str, dict[str, str]] = {}
    for entry in git(source, "ls-tree", "-rz", PIN, "tasks/").split(b"\0"):
        if not entry:
            continue
        blob, name = entry.decode().split("\t", maxsplit=1)
        parts = name.split("/")
        if len(parts) != 3:
            continue
        _, identity, filename = parts
        if (
            CASE_ID.fullmatch(identity)
            and identity not in excluded
            and filename in {"task.yaml", "gold.patch"}
        ):
            files.setdefault(identity, {})[filename] = blob.split()[2]
    metadata = read_blobs(source, (row["task.yaml"] for row in files.values()))
    verified = {
        identity: row["gold.patch"]
        for identity, row in files.items()
        if VERIFIED in yaml.safe_load(metadata[row["task.yaml"]]).get("datasets", [])
    }
    patches = read_blobs(source, verified.values())
    for identity, oid in verified.items():
        project = PROJECTS[identity.rsplit("-", 1)[0]]
        yield PoolCase(identity, project, changed_lines(source, patches[oid]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "task-source")
    source = parser.parse_args().source.resolve()
    if not source.exists():
        git(
            source.parent,
            "clone",
            "--sparse",
            "--no-checkout",
            "--filter=blob:none",
            SOURCE,
            str(source),
        )
    if git(source, "rev-parse", f"{PIN}^{{commit}}").decode().strip() != PIN:
        raise ValueError("Pinned dataset commit missing")
    excluded = previous_ids(ROOT.parent, ROOT)
    pool, remainder = draw_pool(dataset_candidates(source, excluded), excluded)
    for filename, cases in (("pool.json", pool), ("pool-remainder.json", remainder)):
        result = {
            "seed": SEED,
            "dataset_commit": PIN,
            "cases": [asdict(c) for c in cases],
        }
        (ROOT / filename).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
