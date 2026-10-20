import hashlib
import json
import shutil
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.benchmarks.admission import (
    admission_decision,
    admit_case,
    validate_admission,
)
from crucible.benchmarks.contracts import GraderConfig, GradeResult, SolutionPatch

pytest_plugins = ["tests.unit.test_benchmark_manifests"]


def test_reference_success_and_empty_failure_required() -> None:
    assert admission_decision("fail", "pass") == (
        "admitted",
        "reference_pass_empty_fail",
    )
    assert admission_decision("pass", "pass")[0] == "rejected"
    assert admission_decision("fail", "fail")[0] == "rejected"
    assert admission_decision("error", "pass")[0] == "error"
    assert admission_decision("fail", "error")[0] == "error"


@pytest.mark.parametrize(
    "empty,reference,status",
    [
        ("fail", "pass", "admitted"),
        ("pass", "pass", "rejected"),
        ("error", "pass", "error"),
    ],
)
async def test_admission_records_independent_checks(
    case_fixture, tmp_path, monkeypatch, empty, reference, status
):
    source = tmp_path / "evaluator"
    shutil.copytree(case_fixture.fixture_path, source)
    monkeypatch.setattr(
        "crucible.benchmarks.swebench.PINNED_COMMIT", case_fixture.base_commit
    )
    config = GraderConfig(
        driver_python=Path(sys.executable).resolve(),
        driver_source=source,
        driver_commit=case_fixture.base_commit,
        driver_digest=case_fixture.fixture_digest,
        timeout_seconds=10,
        memory_bytes=1024,
        cpus=1,
        output_limit_bytes=65536,
        network="none",
    )
    reference_path = tmp_path / "reference.diff"
    reference_path.write_text("protected reference")
    patch = SolutionPatch(
        reference_path,
        hashlib.sha256(reference_path.read_bytes()).hexdigest(),
        case_fixture.base_commit,
        reference_path.stat().st_size,
    )
    calls = []

    async def grade(self, case, patch, trial_id, output_dir):
        assert output_dir not in calls
        calls.append(output_dir)
        output_dir.mkdir()
        verdict = empty if len(calls) == 1 else reference
        report = output_dir / "report.json"
        report.write_text(
            json.dumps(
                {
                    case.case_id: {
                        "resolved": verdict == "pass",
                        "patch_exists": True,
                        "patch_is_None": False,
                        "patch_successfully_applied": True,
                        "infra_failure": False,
                        "tests_status": {
                            key: {
                                "success": ["test"]
                                if key == "FAIL_TO_PASS" and verdict == "pass"
                                else [],
                                "failure": ["test"]
                                if key == "FAIL_TO_PASS" and verdict == "fail"
                                else [],
                            }
                            for key in [
                                "FAIL_TO_PASS",
                                "PASS_TO_PASS",
                                "FAIL_TO_FAIL",
                                "PASS_TO_FAIL",
                            ]
                        },
                    }
                }
            )
        )
        return GradeResult(
            verdict,
            "test",
            str(uuid4()),
            verdict == "pass" if verdict != "error" else None,
            patch.sha256,
            report if verdict != "error" else None,
            output_dir / "log",
            "a" * 40,
            0,
        )

    monkeypatch.setattr("crucible.benchmarks.admission.SwebenchGrader.grade", grade)
    result = await admit_case(case_fixture, config, patch, tmp_path / "admission")
    assert result.status == status
    assert len(calls) == (1 if empty == "error" else 2)
    assert "protected reference" not in result.evidence_path.read_text()
    if status == "admitted":
        validate_admission(result.evidence_path, case_fixture, config)
        changed = config.model_copy(update={"driver_digest": "c" * 64})
        with pytest.raises(ValueError, match="integrity"):
            validate_admission(result.evidence_path, case_fixture, changed)
        changed_policy = config.model_copy(update={"memory_bytes": 2048})
        with pytest.raises(ValueError, match="identity"):
            validate_admission(result.evidence_path, case_fixture, changed_policy)
        (source / "app.py").write_text("changed evaluator")
        with pytest.raises(ValueError, match="integrity"):
            validate_admission(result.evidence_path, case_fixture, config)
        (source / "app.py").write_text("answer = 1\n")
        result.empty_grade.report_path.write_text("{}")
        with pytest.raises(ValueError, match="integrity"):
            validate_admission(result.evidence_path, case_fixture, config)
