"""Diagnostic controls never enter the measured experiment path."""

import pytest

from crucible.improvement.cli import run_cli
from tests.integration import test_mining_diagnostic as fixtures


async def test_cli_requires_paid_authorization_to_continue(tmp_path, monkeypatch):
    from crucible.improvement.store import ExperimentStore

    def forbidden(*args, **kwargs):
        raise AssertionError("unauthorized command opened an experiment store")

    monkeypatch.setattr(ExperimentStore, "open", forbidden)
    assert (
        await run_cli(["mining-diagnostic", "continue", "--root", str(tmp_path)]) == 2
    )
    assert not list(tmp_path.iterdir())


harness = fixtures.harness
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial
diagnostic_config = fixtures.diagnostic_config


async def test_cli_prepare_status_report_and_close_are_execution_free(
    diagnostic_config, tmp_path, monkeypatch, capsys
):
    import json

    from crucible.improvement.lifecycle import ExperimentService
    from crucible.improvement.worker_supervisor import DockerRuntime, WorkerSupervisor
    from crucible.repair.gateway import ScopedModelGateway

    def forbidden(*args, **kwargs):
        raise AssertionError("read-only/offline command constructed execution")

    monkeypatch.setattr(WorkerSupervisor, "__init__", forbidden)
    monkeypatch.setattr(DockerRuntime, "__init__", forbidden)
    monkeypatch.setattr(ScopedModelGateway, "__init__", forbidden)
    monkeypatch.setattr(ExperimentService, "__init__", forbidden)
    config_path = tmp_path / "config.json"
    config_path.write_text(diagnostic_config.model_dump_json())
    root = tmp_path / "diagnostic"
    assert (
        await run_cli(
            [
                "mining-diagnostic",
                "prepare",
                "--config",
                str(config_path),
                "--output",
                str(root),
            ]
        )
        == 0
    )
    capsys.readouterr()
    ledger = root / "accounting.sqlite3"
    before = ledger.read_bytes()
    assert await run_cli(["mining-diagnostic", "status", "--root", str(root)]) == 0
    state = json.loads(capsys.readouterr().out)
    assert state["status"] == "ready"
    assert ledger.read_bytes() == before
    output = tmp_path / "report"
    assert (
        await run_cli(
            [
                "mining-diagnostic",
                "report",
                "--root",
                str(root),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert set(json.loads(capsys.readouterr().out)) == {
        "metrics.json",
        "report.md",
        "report-manifest.json",
    }
    assert ledger.read_bytes() == before
    assert await run_cli(["mining-diagnostic", "close", "--root", str(root)]) == 0
    closed = json.loads(capsys.readouterr().out)
    assert (closed["status"], closed["next_action"]) == ("closed", "none")


@pytest.mark.parametrize("control", ["status", "recover", "stop", "report", "close"])
async def test_missing_diagnostic_is_never_created(tmp_path, control):
    root = tmp_path / "absent"
    args = ["mining-diagnostic", control, "--root", str(root)]
    if control == "report":
        args += ["--output", str(tmp_path / "report")]
    assert await run_cli(args) == 2
    assert not root.exists()


async def test_cli_stop_records_stop_even_when_frozen_input_drifted(
    diagnostic_config, tmp_path, capsys
):
    import json

    config = tmp_path / "config.json"
    config.write_text(diagnostic_config.model_dump_json())
    root = tmp_path / "diagnostic"
    assert (
        await run_cli(
            [
                "mining-diagnostic",
                "prepare",
                "--config",
                str(config),
                "--output",
                str(root),
            ]
        )
        == 0
    )
    capsys.readouterr()
    (
        root / "frozen/package/source/backend/src/crucible/tools/filesystem.py"
    ).write_bytes(b"drift")
    assert await run_cli(["mining-diagnostic", "stop", "--root", str(root)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "interrupted"
