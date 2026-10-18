import pytest

from crucible.benchmarks.cli import TrialConfig


def test_cli_refuses_missing_model_limits_and_admission() -> None:
    with pytest.raises(ValueError):
        TrialConfig.model_validate_json('{"schema_version":1}')


async def test_show_does_not_execute_or_resume(tmp_path, monkeypatch, capsys):
    from crucible.benchmarks.cli import run_cli
    from crucible.benchmarks.evidence import TrialEvidence

    evidence = TrialEvidence(tmp_path / "trial", {"trial_id": "trial"})
    evidence.stage("solving")

    async def forbidden(*args):
        raise AssertionError("must not run a trial")

    monkeypatch.setattr("crucible.benchmarks.cli.run_trial_config", forbidden)
    assert await run_cli(["show", str(evidence.root)]) == 0
    assert "interrupted" in capsys.readouterr().out


async def test_existing_eval_help_is_unchanged(capsys):
    from crucible.evals.cli import run_cli

    with pytest.raises(SystemExit) as exit_info:
        await run_cli(["eval", "--help"])
    assert exit_info.value.code == 0
    assert "{run,show}" in capsys.readouterr().out


@pytest.mark.parametrize("model", ["fake", "deterministic"])
def test_production_deterministic_models_rejected(model):
    from crucible.benchmarks.cli import ModelSettings

    with pytest.raises(ValueError):
        ModelSettings(
            model_id=model,
            input_limit=1000,
            output_reserve=100,
            provider_defaults="unspecified",
        )


async def test_root_command_dispatches_benchmark_show(tmp_path, capsys):
    from crucible.benchmarks.evidence import TrialEvidence
    from crucible.evals.cli import run_cli

    evidence = TrialEvidence(tmp_path / "trial", {"trial_id": "trial"})
    assert await run_cli(["benchmark", "show", str(evidence.root)]) == 0
    assert "interrupted" in capsys.readouterr().out


pytest_plugins = ["tests.unit.test_benchmark_manifests"]


async def test_overlapping_output_rejected_without_mutating_fixture(
    case_fixture, tmp_path, monkeypatch
):
    import json

    from crucible.benchmarks.cli import run_cli

    manifest = tmp_path / "case.json"
    manifest.write_text(case_fixture.model_dump_json())
    reference = tmp_path / "reference.diff"
    reference.write_text("protected")
    config = tmp_path / "admit.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "authorization": "empty-and-reference-grading",
                "case_manifest": str(manifest),
                "reference_patch": str(reference),
                "reference_sha256": "a" * 64,
                "grader": {
                    "driver_python": "/usr/bin/python3",
                    "driver_source": str(tmp_path / "evaluator"),
                    "driver_commit": "a" * 40,
                    "driver_digest": "b" * 64,
                    "timeout_seconds": 10,
                    "memory_bytes": 1024,
                    "cpus": 1,
                    "output_limit_bytes": 10000,
                    "network": "none",
                },
            }
        )
    )

    async def forbidden(*args):
        raise AssertionError("must not grade overlapping output")

    monkeypatch.setattr("crucible.benchmarks.cli.run_admission_config", forbidden)
    output = case_fixture.fixture_path / "new-output"
    assert (
        await run_cli(["admit", "--config", str(config), "--output", str(output)]) == 2
    )
    assert not output.exists()


@pytest.fixture
def mocked_trial_config(case_fixture, tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from uuid import uuid4

    from crucible.benchmarks.manifests import case_digest
    from crucible.repair.identity import digest

    manifest = tmp_path / "case.json"
    manifest.write_text(case_fixture.model_dump_json())
    model = {
        "model_id": "test-provider/model",
        "input_limit": 10000,
        "output_reserve": 1000,
        "provider_defaults": "unspecified",
    }
    limits = {
        "max_steps": 2,
        "max_tool_calls": 5,
        "max_model_tokens": 10000,
        "max_active_seconds": 10,
        "wall_timeout_seconds": 20,
    }
    trial = str(uuid4())
    value = {
        "schema_version": 1,
        "authorization": "one-baseline-calibration",
        "approval_mode": "interactive",
        "case_manifest": str(manifest),
        "admission_record": str(tmp_path / "admission.json"),
        "data_dir": str(tmp_path / "data"),
        "model": model,
        "limits": limits,
        "identity": {
            "trial_id": trial,
            "case_digest": case_digest(case_fixture),
            "harness_commit": "1" * 40,
            "harness_tree_digest": "d" * 64,
            "model_config_digest": digest(model),
            "limits_digest": digest(limits),
            "repeat_index": 0,
        },
        "grader": {
            "driver_python": "/usr/bin/python3",
            "driver_source": str(tmp_path / "evaluator"),
            "driver_commit": "a" * 40,
            "driver_digest": "b" * 64,
            "timeout_seconds": 10,
            "memory_bytes": 1024,
            "cpus": 1,
            "output_limit_bytes": 10000,
            "network": "none",
        },
    }
    config = tmp_path / "trial.json"
    config.write_text(json.dumps(value))

    async def current(*args):
        return SimpleNamespace(commit="1" * 40, source_digest="d" * 64)

    monkeypatch.setattr("crucible.benchmarks.cli.capture_runtime_identity", current)
    monkeypatch.setattr("crucible.benchmarks.cli._git", lambda *args: b"")
    monkeypatch.setattr(
        "crucible.benchmarks.cli.validate_admission", lambda *args: None
    )
    monkeypatch.setattr("crucible.benchmarks.cli.command.upgrade", lambda *args: None)
    created = []

    async def close():
        pass

    async def create(database_url, *args, **kwargs):
        created.append(database_url)
        return SimpleNamespace(close=close)

    monkeypatch.setattr("crucible.benchmarks.cli.ApplicationContainer.create", create)
    return config, trial, created


@pytest.mark.parametrize("termination", ["error", "cancel"])
async def test_duplicate_trial_identity_never_reexecutes(
    mocked_trial_config, tmp_path, monkeypatch, termination
):
    import asyncio

    from crucible.benchmarks.cli import run_cli
    from crucible.benchmarks.evidence import TrialEvidence

    config, trial, created = mocked_trial_config

    async def run(*args):
        if termination == "cancel":
            raise asyncio.CancelledError
        raise RuntimeError("mock setup failure")

    monkeypatch.setattr("crucible.benchmarks.cli.BenchmarkTrialRunner.run", run)
    output = tmp_path / "first"
    assert await run_cli(
        ["trial", "--config", str(config), "--output", str(output)]
    ) == (130 if termination == "cancel" else 3)
    before = (output / "result.json").read_bytes()
    assert TrialEvidence.show(output)["status"] == (
        "interrupted" if termination == "cancel" else "error"
    )
    assert (
        await run_cli(
            ["trial", "--config", str(config), "--output", str(tmp_path / "second")]
        )
        == 2
    )
    assert len(created) == 1
    assert f"/benchmark-runtime/{trial}/crucible.db" in created[0]
    assert (output / "result.json").read_bytes() == before


async def test_mutable_source_rejects_before_runtime_creation(
    mocked_trial_config, tmp_path, monkeypatch
):
    from crucible.benchmarks.cli import run_cli

    config, _, created = mocked_trial_config
    monkeypatch.setattr("crucible.benchmarks.cli._git", lambda *args: b" M source.py")
    output = tmp_path / "trial-output"
    assert (
        await run_cli(["trial", "--config", str(config), "--output", str(output)]) == 2
    )
    assert not created
    assert not output.exists()


async def test_sigterm_records_interruption(mocked_trial_config, tmp_path, monkeypatch):
    import asyncio
    import signal

    from crucible.benchmarks.cli import run_cli
    from crucible.benchmarks.evidence import TrialEvidence

    config, _, _ = mocked_trial_config
    started = asyncio.Event()

    async def run(*args):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("crucible.benchmarks.cli.BenchmarkTrialRunner.run", run)
    output = tmp_path / "signalled"
    task = asyncio.create_task(
        run_cli(["trial", "--config", str(config), "--output", str(output)])
    )
    await asyncio.wait_for(started.wait(), 5)
    signal.raise_signal(signal.SIGTERM)
    assert await asyncio.wait_for(task, 5) == 130
    assert TrialEvidence.show(output)["status"] == "interrupted"
