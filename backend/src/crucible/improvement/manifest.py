"""Offline freeze of an explicitly complete private admitted-case registry."""

import hashlib
import json
from pathlib import Path

from pydantic import Field

from crucible.benchmarks.admission import validate_admission
from crucible.benchmarks.manifests import case_digest, load_case
from crucible.improvement.contracts import Contract, ExecutionPolicy, ExperimentManifest


class ManifestDrift(ValueError):
    pass


class RegistryEntry(Contract):
    case_manifest: Path
    admission_record: Path
    retired: bool


class CaseRegistry(Contract):
    schema_version: int
    pending_slots: int = Field(ge=0)
    cases: dict[str, RegistryEntry]


def read_bounded(path: Path, maximum: int = 1_048_576) -> bytes:
    if (
        not path.is_absolute()
        or ".." in path.parts
        or any(p.is_symlink() for p in (path, *path.parents))
    ):
        raise ValueError("absolute nonsymlink input required")
    with path.open("rb") as source:
        data = source.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("input exceeds byte limit")
    return data


def manifest_digest(manifest: ExecutionPolicy) -> str:
    data = manifest.model_dump(mode="json")
    # Operator path spelling is not semantic identity; evaluator content is pinned.
    for name in ("driver_python", "driver_source"):
        del data["grader"][name]
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def freeze_manifest(
    config: Path, registry_root: Path, *, authorize_disposable_commands: bool = False
) -> ExperimentManifest:
    payload = json.loads(read_bounded(config))
    if authorize_disposable_commands:
        payload["disposable_commands_authorized"] = True
    manifest = ExperimentManifest.model_validate_json(json.dumps(payload))
    registry = CaseRegistry.model_validate_json(
        read_bounded(registry_root / "registry.json")
    )
    if registry.schema_version != 1 or registry.pending_slots:
        raise ManifestDrift("unknown registry version or pending cases")
    if set(registry.cases) != {c.case_id for c in manifest.cases}:
        raise ManifestDrift("missing or unexpected case")
    for ref in manifest.cases:
        entry = registry.cases[ref.case_id]
        if entry.retired:
            raise ManifestDrift("retired case")
        case = load_case(entry.case_manifest)
        if (
            case.case_id != ref.case_id
            or case.split != ref.split
            or case_digest(case) != ref.case_digest
        ):
            raise ManifestDrift("case identity drift")
        if (
            hashlib.sha256(read_bounded(entry.admission_record)).hexdigest()
            != ref.admission_digest
        ):
            raise ManifestDrift("admission digest drift")
        validate_admission(entry.admission_record, case, manifest.grader)
    return manifest
