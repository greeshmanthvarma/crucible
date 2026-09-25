import json
from pathlib import Path

import pytest

from crucible.evals.cli import run_cli
from tests.integration.evals.test_runner import CommandRequestGateway, FakeDockerClient


async def test_cli_requires_explicit_case_and_protected_root(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        await run_cli(
            [
                "eval",
                "run",
                "smoke",
                "--partition",
                "held-out",
                "--data-dir",
                str(tmp_path),
            ]
        )
    with pytest.raises(SystemExit):
        await run_cli(["eval", "run", "smoke", "--auto-approve"])
    with pytest.raises(SystemExit):
        await run_cli(["eval", "run", "smoke", "--trials", "0"])


async def test_cli_repeat_and_show_retain_independent_trials(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = await run_cli(
        ["eval", "run", "smoke", "--trials", "2", "--data-dir", str(tmp_path)],
        docker_client=FakeDockerClient(),
    )
    assert status == 1  # The stock fake model does not create answer.txt.
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    invocation = lines[-1]["invocation_id"]
    assert len(lines[-1]["trials"]) == 2
    assert lines[-1]["trials"][0]["trial_id"] != lines[-1]["trials"][1]["trial_id"]
    shown = await run_cli(
        ["eval", "show", invocation, "--data-dir", str(tmp_path)],
        docker_client=FakeDockerClient(),
    )
    assert shown == 0
    report = json.loads(capsys.readouterr().out)
    assert report["invocation_id"] == invocation
    assert len(report["trials"]) == 2


async def test_prompt_eof_cancels_run_and_never_passes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_input(prompt: str) -> str:
        raise EOFError("input closed")

    monkeypatch.setattr("builtins.input", no_input)
    status = await run_cli(
        ["eval", "run", "smoke", "--data-dir", str(tmp_path)],
        gateway_factory=CommandRequestGateway,
        docker_client=FakeDockerClient(),
    )
    assert status == 1
    output = capsys.readouterr().out.splitlines()
    assert any("spec_digest" in line for line in output)
    summary = json.loads(output[-1])
    assert summary["trials"][0]["verdict"] == "error"
    assert summary["trials"][0]["status"] == "failed"
