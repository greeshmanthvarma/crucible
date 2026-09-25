"""Strict, partitioned definitions for explicitly invoked evaluations."""

import hashlib
import json
import re
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

from crucible.domain.repository import RepositorySettings

DIGEST_VERSION = 1
_ID = re.compile(r"^[a-z][a-z0-9_-]*$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_KINDS = {
    "required_file",
    "forbidden_file",
    "allowed_files",
    "required_api",
    "diff_constraints",
    "hidden_command",
}


class EvalPartition(StrEnum):
    DEVELOPMENT = "development"
    HELD_OUT = "held-out"


@dataclass(frozen=True)
class EvaluatorDefinition:
    kind: str
    options: dict[str, Any]
    content_digest: str | None = None


@dataclass(frozen=True)
class EvalCaseDefinition:
    case_id: str
    fixture_path: Path
    fixture_revision: str
    prompt: str
    repository_settings: RepositorySettings
    model: str
    evaluators: tuple[EvaluatorDefinition, ...]
    case_digest: str
    partition: EvalPartition
    fixture_ref: str


@dataclass(frozen=True)
class EvalSuiteDefinition:
    suite_id: str
    cases: tuple[EvalCaseDefinition, ...]
    suite_digest: str
    partition: EvalPartition


def _root(partition: EvalPartition, root: Path | None) -> Path:
    return (
        root
        if root is not None
        else Path(__file__).parents[4] / "evals" / partition.value
    )


def _safe_path(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts or "\\" in relative:
        raise ValueError("path must be relative to the selected partition")
    candidate = root / relative
    if any(
        part.is_symlink()
        for part in (candidate, *candidate.parents)
        if part != root and root in part.parents
    ):
        raise ValueError("symlink path is forbidden")
    if not candidate.resolve().is_relative_to(root.resolve()):
        raise ValueError("path escapes selected partition")
    return candidate


def _name(name: str) -> str:
    if not _ID.fullmatch(name):
        raise ValueError("invalid eval identifier")
    return name


def _digest(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read(path: Path, fields: set[str]) -> dict[str, Any]:
    value = tomllib.loads(path.read_text(encoding="utf-8"))
    if value.get("version") != 1 or set(value) - fields:
        raise ValueError("unsupported eval manifest schema")
    return value


def _fixture_tree(path: Path, revision: str) -> str:
    if not _REVISION.fullmatch(revision):
        raise ValueError("fixture revision must be a full commit ID")
    if path.suffix == ".bundle":
        with tempfile.TemporaryDirectory(prefix="crucible-manifest-") as directory:
            try:
                subprocess.run(
                    ["git", "clone", "--quiet", "--no-checkout", str(path), directory],
                    check=True,
                    capture_output=True,
                )
            except (OSError, subprocess.CalledProcessError) as error:
                raise ValueError("fixture bundle is invalid") from error
            return _fixture_tree(Path(directory), revision)
    try:
        actual = subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "--verify", f"{revision}^{{commit}}"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        tree = subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", f"{revision}^{{tree}}"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError("fixture revision does not exist") from error
    if actual != revision:
        raise ValueError("fixture revision does not resolve exactly")
    return tree


def load_case(
    partition: EvalPartition, case_id: str, *, root: Path | None = None
) -> EvalCaseDefinition:
    selected = _root(partition, root)
    path = _safe_path(selected, f"cases/{_name(case_id)}.toml")
    value = _read(
        path,
        {
            "version",
            "id",
            "fixture",
            "revision",
            "prompt",
            "model",
            "settings",
            "evaluators",
        },
    )
    if value.get("id") != case_id:
        raise ValueError("case ID does not match filename")
    fixture_rel = str(value["fixture"])
    if PurePosixPath(fixture_rel).parts[:1] != ("fixtures",):
        raise ValueError("fixture must live inside the partition fixtures directory")
    fixture = _safe_path(selected, fixture_rel)
    if not fixture.is_dir() and not (fixture.is_file() and fixture.suffix == ".bundle"):
        raise ValueError("fixture repository or bundle is missing")
    revision = str(value["revision"])
    tree = _fixture_tree(fixture, revision)
    settings_raw = value.get("settings", {})
    if not isinstance(settings_raw, dict) or set(settings_raw) - {
        "sandbox_image",
        "sandbox_network",
        "validation_repair_limit",
        "default_cwd",
        "model_input_limit",
        "model_output_reserve",
    }:
        raise ValueError("invalid repository settings")
    settings = RepositorySettings(**settings_raw)
    raw_evaluators = value.get("evaluators")
    if not isinstance(raw_evaluators, list) or not raw_evaluators:
        raise ValueError("case needs evaluators")
    evaluators: list[EvaluatorDefinition] = []
    digest_evaluators: list[dict[str, object]] = []
    for raw in raw_evaluators:
        if not isinstance(raw, dict) or raw.get("kind") not in _KINDS:
            raise ValueError("unknown evaluator kind")
        kind = str(raw["kind"])
        options = {str(key): item for key, item in raw.items() if key != "kind"}
        content_digest = None
        if kind == "hidden_command":
            hidden_rel = str(options.get("test_path", ""))
            if PurePosixPath(hidden_rel).parts[:1] != ("tests",):
                raise ValueError("hidden evaluator must live in partition tests")
            hidden = _safe_path(selected, hidden_rel)
            if not hidden.is_file():
                raise ValueError("hidden evaluator file is missing")
            content_digest = hashlib.sha256(hidden.read_bytes()).hexdigest()
        evaluators.append(EvaluatorDefinition(kind, options, content_digest))
        digest_evaluators.append(
            {"kind": kind, "options": options, "content_digest": content_digest}
        )
    snapshot = {
        "digest_version": DIGEST_VERSION,
        "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "fixture": fixture_rel,
        "fixture_tree": tree,
        "revision": revision,
        "evaluators": digest_evaluators,
    }
    return EvalCaseDefinition(
        case_id,
        fixture,
        revision,
        str(value["prompt"]),
        settings,
        str(value["model"]),
        tuple(evaluators),
        _digest(snapshot),
        partition,
        fixture_rel,
    )


def load_suite(
    partition: EvalPartition, name: str, *, root: Path | None = None
) -> EvalSuiteDefinition:
    selected = _root(partition, root)
    path = _safe_path(selected, f"suites/{_name(name)}.toml")
    value = _read(path, {"version", "id", "cases"})
    if value.get("id") != name:
        raise ValueError("suite ID does not match filename")
    case_ids = value.get("cases")
    if (
        not isinstance(case_ids, list)
        or not case_ids
        or len(set(case_ids)) != len(case_ids)
    ):
        raise ValueError("suite has missing or duplicate cases")
    cases = tuple(load_case(partition, str(item), root=selected) for item in case_ids)
    digest = _digest(
        {
            "digest_version": DIGEST_VERSION,
            "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "cases": [(item.case_id, item.case_digest) for item in cases],
        }
    )
    return EvalSuiteDefinition(name, cases, digest, partition)
