"""Separate operator-authorized proof; no automatic real-model execution."""

import os
from pathlib import Path

import pytest

from crucible.benchmarks.cli import run_trial_config


@pytest.mark.docker
@pytest.mark.real_model
@pytest.mark.benchmark_external
async def test_authorized_non_suite_calibration(tmp_path: Path) -> None:
    config = os.environ.get("CRUCIBLE_BENCHMARK_REAL_CONFIG")
    if not config:
        pytest.skip(
            "requires separately authorized model, limits, admission "
            "and calibration config"
        )
    result = await run_trial_config(Path(config), tmp_path / "calibration")
    assert result.status == "complete", result.error_code
    assert result.solver is not None and result.solver.trace_path.is_file()
    assert result.patch is not None and result.patch.path.is_file()
    assert result.grade is not None and result.grade.verdict in {"pass", "fail"}
    assert result.grade.report_path is not None and result.grade.report_path.is_file()
