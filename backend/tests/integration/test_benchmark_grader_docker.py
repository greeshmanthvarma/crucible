"""Explicitly authorized infrastructure admission; never an ordinary unit test."""

import os
from pathlib import Path

import pytest

from crucible.benchmarks.admission import run_admission_config


@pytest.mark.docker
@pytest.mark.benchmark_external
async def test_authorized_protected_case_admission(tmp_path: Path) -> None:
    config = os.environ.get("CRUCIBLE_BENCHMARK_ADMISSION_CONFIG")
    if not config:
        pytest.skip("requires separately authorized protected admission config")
    result = await run_admission_config(Path(config), tmp_path / "admission")
    assert result.status == "admitted", result.code
    assert result.empty_grade.verdict == "fail"
    assert result.reference_grade.verdict == "pass"
