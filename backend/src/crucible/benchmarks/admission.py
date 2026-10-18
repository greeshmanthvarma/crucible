"""Trusted empty/reference checks; never solver or optimizer inputs."""

import hashlib
import json
import platform
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from crucible.benchmarks.contracts import (
    BenchmarkCase,
    GraderConfig,
    GradeResult,
    SolutionPatch,
)
from crucible.benchmarks.manifests import case_digest
from crucible.benchmarks.swebench import (
    SwebenchGrader,
    validate_driver_source,
    validate_official_report,
)


@dataclass(frozen=True)
class AdmissionResult:
    case_digest: str
    driver_digest: str
    image_digest: str
    empty_grade: GradeResult
    reference_grade: GradeResult
    status: Literal["admitted", "rejected", "error"]
    code: str
    evidence_path: Path
    elapsed_seconds: float


def admission_decision(
    empty: str, reference: str
) -> tuple[Literal["admitted", "rejected", "error"], str]:
    if "error" in (empty, reference):
        return "error", "grading_infrastructure_error"
    if empty == "fail" and reference == "pass":
        return "admitted", "reference_pass_empty_fail"
    return "rejected", "uninformative_or_invalid_reference"


def grader_policy_digest(config: GraderConfig) -> str:
    semantic = config.model_dump(
        mode="json", exclude={"driver_python", "driver_source"}
    )
    return hashlib.sha256(json.dumps(semantic, sort_keys=True).encode()).hexdigest()


async def admit_case(
    case: BenchmarkCase,
    config: GraderConfig,
    reference_patch: SolutionPatch,
    output_dir: Path,
) -> AdmissionResult:
    started = time.monotonic()
    fingerprint = case_digest(case)
    validate_driver_source(config)
    if (
        not reference_patch.path.is_absolute()
        or ".." in reference_patch.path.parts
        or any(
            p.is_symlink()
            for p in [reference_patch.path, *reference_patch.path.parents]
        )
    ):
        raise ValueError("invalid protected reference path")
    if reference_patch.path.resolve().is_relative_to(case.fixture_path.resolve()):
        raise ValueError("protected reference inside fixture")
    for source in case.fixture_path.rglob("*"):
        if (
            source.is_file()
            and ".git" not in source.relative_to(case.fixture_path).parts
        ):
            if (
                source.name == reference_patch.path.name
                or hashlib.sha256(source.read_bytes()).hexdigest()
                == reference_patch.sha256
            ):
                raise ValueError("known protected reference in fixture")
    if not output_dir.is_absolute() or any(
        p.is_symlink() for p in [output_dir, *output_dir.parents]
    ):
        raise ValueError("invalid admission output")
    for path in (
        case.fixture_path,
        case.prompt_path,
        case.grader_input_path,
        reference_patch.path,
    ):
        if path.resolve().is_relative_to(
            output_dir.resolve()
        ) or output_dir.resolve().is_relative_to(path.resolve()):
            raise ValueError("admission output overlaps inputs")
    output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    empty_path = output_dir / "empty.diff"
    empty_path.write_bytes(b"")
    empty = SolutionPatch(
        empty_path, hashlib.sha256(b"").hexdigest(), case.base_commit, 0
    )
    grader = SwebenchGrader(config)
    empty_grade = await grader.grade(case, empty, uuid4(), output_dir / "empty")
    if empty_grade.verdict == "error":
        skipped_log = output_dir / "reference-not-run.log"
        skipped_log.write_text("Reference grading not run after empty grading error.\n")
        reference_grade = GradeResult(
            "error",
            "not_run_after_empty_error",
            "not-run",
            None,
            reference_patch.sha256,
            None,
            skipped_log,
            config.driver_commit,
            0,
        )
    else:
        reference_grade = await grader.grade(
            case, reference_patch, uuid4(), output_dir / "reference"
        )
    status, code = admission_decision(empty_grade.verdict, reference_grade.verdict)
    evidence = output_dir / "admission.json"
    result = AdmissionResult(
        fingerprint,
        config.driver_digest,
        case.image.split("@")[1],
        empty_grade,
        reference_grade,
        status,
        code,
        evidence,
        time.monotonic() - started,
    )
    reports = {}
    for label, grade in [("empty", empty_grade), ("reference", reference_grade)]:
        reports[label] = (
            hashlib.sha256(grade.report_path.read_bytes()).hexdigest()
            if grade.report_path
            else None
        )
    record = {
        "schema_version": 1,
        "result": asdict(result),
        "report_digests": reports,
        "grader_policy_digest": grader_policy_digest(config),
        "platform": case.platform,
        "host_architecture": platform.machine(),
        "limits": config.model_dump(mode="json"),
        "metrics": {
            "peak_memory_bytes": None,
            "cpu_seconds": None,
            "unavailable_reason": "resource telemetry not collected",
        },
    }
    evidence.write_text(json.dumps(record, default=str, sort_keys=True))
    evidence.chmod(0o600)
    return result


def validate_admission(path: Path, case: BenchmarkCase, config: GraderConfig) -> None:
    if any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("symlink admission evidence")
    validate_driver_source(config)
    record = json.loads(path.read_text())
    result = record["result"]
    if (
        record["schema_version"] != 1
        or result["status"] != "admitted"
        or result["case_digest"] != case_digest(case)
        or result["driver_digest"] != config.driver_digest
        or result["image_digest"] != case.image.split("@")[1]
        or record["platform"] != case.platform
        or record["grader_policy_digest"] != grader_policy_digest(config)
    ):
        raise ValueError("admission identity mismatch or not admitted")
    for label, expected in [("empty", False), ("reference", True)]:
        report = Path(result[label + "_grade"]["report_path"])
        if report.is_symlink() or not report.resolve().is_relative_to(
            path.parent.resolve()
        ):
            raise ValueError("admission report outside protected root")
        resolved, _ = validate_official_report(report, case.case_id)
        if (
            resolved is not expected
            or hashlib.sha256(report.read_bytes()).hexdigest()
            != record["report_digests"][label]
        ):
            raise ValueError("admission report integrity failure")


async def run_admission_config(path: Path, output_dir: Path) -> AdmissionResult:
    """Explicit operator opt-in for pre-staged local protected inputs only."""
    from crucible.benchmarks.manifests import load_case

    config = AdmissionConfig.model_validate_json(path.read_bytes())
    case = load_case(config.case_manifest)
    reference = config.reference_patch
    if not reference.is_absolute() or any(
        p.is_symlink() for p in [reference, *reference.parents]
    ):
        raise ValueError("invalid protected reference path")
    digest = hashlib.sha256(reference.read_bytes()).hexdigest()
    if digest != config.reference_sha256:
        raise ValueError("reference digest mismatch")
    patch = SolutionPatch(reference, digest, case.base_commit, reference.stat().st_size)
    return await admit_case(case, config.grader, patch, output_dir)


class AdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal[1]
    authorization: Literal["empty-and-reference-grading"]
    case_manifest: Path
    grader: GraderConfig
    reference_patch: Path
    reference_sha256: str
