import asyncio
import hashlib
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.benchmarks.contracts import GraderConfig, SolutionPatch
from crucible.benchmarks.manifests import fixture_digest
from crucible.benchmarks.swebench import (
    SwebenchGrader,
    evaluation_identity,
    validate_official_report,
)

pytest_plugins = ["tests.unit.test_benchmark_manifests"]


def test_changed_patch_gets_distinct_evaluation_identity() -> None:
    trial = uuid4()
    assert evaluation_identity(
        trial, "a" * 64, "b" * 64, "c" * 64
    ) != evaluation_identity(trial, "a" * 64, "d" * 64, "c" * 64)


def test_missing_report_is_error(tmp_path: Path) -> None:
    assert validate_official_report(tmp_path / "missing", "case") == (
        None,
        "missing_official_report",
    )


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("pass", "pass"),
        ("fail", "fail"),
        ("missing", "error"),
        ("wrong_case", "error"),
        ("partial", "error"),
        ("wrong_label", "error"),
        ("malformed", "error"),
        ("timeout", "error"),
        ("output", "error"),
        ("tampered_patch", "error"),
        ("changed_driver", "error"),
        ("untracked_driver", "error"),
        ("launch_failure", "error"),
        ("missing_patch", "error"),
        ("cancel", "interrupted"),
    ],
)
async def test_driver_process_reports(
    case_fixture, tmp_path, monkeypatch, mode, expected
):
    import json

    task_data = json.dumps({"FAIL_TO_PASS": ["test"], "PASS_TO_PASS": []}).encode()
    case_fixture.grader_input_path.write_bytes(task_data)
    case_fixture = case_fixture.model_copy(
        update={
            "grader_input_digest": hashlib.sha256(task_data).hexdigest(),
        }
    )
    source = tmp_path / "driver-source"
    source.mkdir()
    subprocess.run(["git", "-C", str(source), "init", "-q"], check=True)
    (source / "driver.py").write_text("trusted = True\n")
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "pin",
        ],
        check=True,
    )
    commit = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    monkeypatch.setattr("crucible.benchmarks.swebench.PINNED_COMMIT", commit)
    patch_path = tmp_path / "solution.diff"
    patch_path.write_text("")
    patch = SolutionPatch(
        patch_path, hashlib.sha256(b"").hexdigest(), case_fixture.base_commit, 0
    )
    config = GraderConfig(
        driver_python=Path(sys.executable).resolve(),
        driver_source=source,
        driver_commit=commit,
        driver_digest=fixture_digest(source),
        timeout_seconds=1,
        memory_bytes=1024,
        cpus=1,
        output_limit_bytes=65536,
        network="none",
    )
    if mode == "tampered_patch":
        patch_path.write_text("tampered")
    if mode == "missing_patch":
        patch_path.unlink()
    if mode == "changed_driver":
        (source / "driver.py").write_text("changed")
    if mode == "untracked_driver":
        (source / "docker.py").write_text("injected")
    fake = tmp_path / "fake.py"
    fake.write_text("""import json, sys, hashlib, time
from pathlib import Path
root = Path(sys.argv[1]); mode = sys.argv[2]
r = json.loads((root / 'request.json').read_text())
if mode in ('timeout', 'cancel'): time.sleep(5)
if mode == 'output': print('x' * 100000)
if mode == 'missing': sys.exit(0)
case = r['case_id'] if mode != 'wrong_case' else 'other'
report = {case: dict(resolved=mode != 'fail', patch_exists=True, patch_is_None=False,
    patch_successfully_applied=True, infra_failure=False,
    tests_status={'FAIL_TO_PASS': {'success':['test'] if mode != 'fail' else [], 
        'failure':[] if mode != 'fail' else ['test']},
                  'PASS_TO_PASS': {'success':[], 'failure':[]},
                  'FAIL_TO_FAIL': {'success':[], 'failure':[]},
                  'PASS_TO_FAIL': {'success':[], 'failure':[]}})}
if mode == 'partial': del report[case]['tests_status']
p = root / 'official-report.json'
p.write_text(json.dumps(report) if mode != 'malformed' else '{')
envelope = dict(schema_version=1, evaluation_id=r['evaluation_id'],
    case_id=r['case_id'],
    patch_digest=r['patch_digest'],
    model_name_or_path=r['evaluation_id'] if mode != 'wrong_label' else 'other',
    classification='official_report',
    report_digest=hashlib.sha256(p.read_bytes()).hexdigest())
(root / 'result.json').write_text(json.dumps(envelope))
""")

    cleaned = []

    async def cleanup(*args):
        cleaned.append(True)

    monkeypatch.setattr(SwebenchGrader, "_cleanup_owned", cleanup)
    spawned = asyncio.Event()
    real_spawn = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        if mode == "launch_failure":
            raise OSError("interpreter unavailable")
        assert not any("KEY" in name or "TOKEN" in name for name in kwargs["env"])
        spawned.set()
        return await real_spawn(
            sys.executable, str(fake), str(tmp_path / "grade"), mode, **kwargs
        )

    monkeypatch.setattr(
        "crucible.benchmarks.swebench.asyncio.create_subprocess_exec", spawn
    )
    if mode == "cancel":
        task = asyncio.create_task(
            SwebenchGrader(config).grade(
                case_fixture, patch, uuid4(), tmp_path / "grade"
            )
        )
        await spawned.wait()
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleaned
        return
    result = await SwebenchGrader(config).grade(
        case_fixture, patch, uuid4(), tmp_path / "grade"
    )
    assert result.verdict == expected
    if mode == "timeout":
        assert result.code == "grading_timeout"
    if mode == "output":
        assert result.code == "grading_output_limit"
