import importlib.util
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


def script(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_budget_is_one_trial():
    prepare = script("prepare_solver_calibration")
    budget = prepare.trial_budget(
        {
            "max_model_tokens": 200000,
            "max_steps": 20,
            "max_active_seconds": 600,
            "wall_timeout_seconds": 1800,
        },
        {"max_calls_per_trial": 100, "max_output_bytes_per_call": 1024},
    )
    assert budget == {
        "max_reported_and_reserved_tokens": 200000,
        "max_model_requests": 20,
        "max_trial_starts": 1,
        "max_active_seconds": 600,
        "max_worker_wall_seconds": 1800,
        "max_total_output_bytes": 102400,
    }


def test_metrics_and_missing_evidence(tmp_path):
    summary = script("summarize_solver_calibration")
    (tmp_path / "result.json").write_text(
        json.dumps(
            {
                "result": {
                    "grade": {"verdict": "pass"},
                    "solver": {"termination": "completed"},
                }
            }
        )
    )
    (tmp_path / "solution.diff").write_text("")
    (tmp_path / "usage.json").write_text(
        json.dumps({"usage": {"requests": 3, "input_tokens": 42}})
    )
    (tmp_path / "trace.json").write_text(
        json.dumps(
            {
                "data": {
                    "tool_calls": [
                        {"id": "a", "name": "apply_patch"},
                        {"id": "b", "name": "edit_file"},
                        {"id": "c", "name": "list_files"},
                    ],
                    "tool_results": [
                        {
                            "tool_call_id": "a",
                            "status": "failed",
                            "error_code": "bad_patch",
                        },
                        {
                            "tool_call_id": "b",
                            "status": "succeeded",
                            "error_code": None,
                        },
                    ],
                }
            }
        )
    )
    row = summary.trial_metrics(tmp_path)
    assert (
        row["passes"],
        row["empty_patches"],
        row["model_requests"],
        row["input_tokens"],
        row["list_files_calls"],
    ) == (1, 1, 3, 42, 1)
    assert row["tool_outcomes"] == {
        "apply_patch:failed:bad_patch": 1,
        "edit_file:succeeded:none": 1,
    }
    merged = summary.combine([row, row])
    assert merged["termination"] == {"completed": 2}
    assert merged["passes"] == 2
    (tmp_path / "solution.diff").unlink()
    assert summary.trial_metrics(tmp_path)["empty_patches"] == 0
    assert summary.trial_metrics(tmp_path)["missing_patches"] == 1


def test_baseline_query_excludes_candidates_and_other_splits(tmp_path):
    summary = script("summarize_solver_calibration")
    original = tmp_path / "data/self-improvement/full-run-v1"
    (original / "runtime").mkdir(parents=True)
    (original / "experiment-manifest.json").write_text(
        json.dumps(
            {
                "experiment_id": "e",
                "initial_commit": "old",
                "cases": [
                    {"case_id": "dev", "split": "development"},
                    {"case_id": "held", "split": "final-test"},
                ],
            }
        )
    )
    with sqlite3.connect(original / "runtime/experiments.sqlite3") as db:
        db.executescript("""
        CREATE TABLE experiment_trials(id,case_id,experiment,snapshot);
        CREATE TABLE experiment_snapshots(id,kind,parent_commit);
        INSERT INTO experiment_snapshots VALUES ('b','baseline','old');
        INSERT INTO experiment_snapshots VALUES ('c','candidate','old');
        INSERT INTO experiment_trials VALUES ('one','dev','e','b');
        INSERT INTO experiment_trials VALUES ('two','dev','e','c');
        INSERT INTO experiment_trials VALUES ('three','held','e','b');
        """)
    report = summary.summarize(tmp_path)
    assert report["original-baseline"]["total"]["trials"] == 1
    assert set(report["original-baseline"]["cases"]) == {"dev"}


def test_runner_preserves_terminal_and_stops_on_infrastructure(tmp_path):
    prepare = script("prepare_solver_calibration")
    target = tmp_path / "run.sh"
    prepare.write_runner(target)
    content = target.read_text()
    assert "[ -t 0 ]" in content
    assert "rev-parse HEAD" in content
    assert "status --porcelain" in content
    assert '[ -e "$output" ]' in content
    assert "exit 3" in content
    assert '--config "$config" --output "$output"' in content
    assert "| uv" not in content


def test_runner_skips_outputs_and_stops_on_exit_three(tmp_path):
    import os
    import subprocess

    prepare = script("prepare_solver_calibration")
    root = tmp_path / "plans with spaces"
    root.mkdir()
    repository = tmp_path / "checkout"
    (repository / "backend").mkdir(parents=True)
    batch = root / "new-v2-harness"
    batch.mkdir()
    (batch / "batch.json").write_text(
        json.dumps({"repository": str(repository), "commit": "frozen"})
    )
    for name in ("a", "b", "c"):
        (batch / name).mkdir()
        (batch / name / "config.json").write_text("{}")
    (batch / "a/output").mkdir()
    binaries = tmp_path / "bin"
    binaries.mkdir()
    (binaries / "git").write_text(
        '#!/bin/bash\ncase "$*" in *rev-parse*) echo frozen;; esac\n'
    )
    (binaries / "uv").write_text('#!/bin/bash\necho "MOCK $*"\nexit 3\n')
    for executable in binaries.iterdir():
        executable.chmod(0o755)
    runner = root / "run.sh"
    prepare.write_runner(runner)
    master, slave = os.openpty()
    try:
        result = subprocess.run(
            [str(runner), "new-v2"],
            stdin=slave,
            capture_output=True,
            text=True,
            env={**os.environ, "PATH": f"{binaries}:{os.environ['PATH']}"},
            timeout=10,
        )
    finally:
        os.close(master)
        os.close(slave)
    assert result.returncode == 3
    assert "Skipping existing" in result.stdout
    assert result.stdout.count("MOCK") == 1
    assert str(batch / "b/config.json") in result.stdout
    assert str(batch / "c/config.json") not in result.stdout


def test_prepare_rejects_invalid_batch_name(tmp_path):
    prepare = script("prepare_solver_calibration")
    with pytest.raises(ValueError, match="batch"):
        prepare.prepare(tmp_path, tmp_path, "../x")
    assert list(tmp_path.iterdir()) == []


def test_runner_rejects_invalid_batch_name(tmp_path):
    target = tmp_path / "run.sh"
    script("prepare_solver_calibration").write_runner(target)
    result = subprocess.run([str(target), "../x"], capture_output=True, text=True)
    assert result.returncode == 2
    assert "Invalid batch" in result.stderr


def test_summary_reads_isolated_calibration_output(tmp_path):
    test_baseline_query_excludes_candidates_and_other_splits(tmp_path)
    directory = tmp_path / "data/self-improvement/calibration-v1/x-harness/trial"
    output = directory / "output"
    output.mkdir(parents=True)
    case = tmp_path / "case.json"
    case.write_text(json.dumps({"case_id": "dev"}))
    (directory / "config.json").write_text(
        json.dumps({"manifest": {"case_manifest": str(case)}})
    )
    (output / "trace.json").write_text(
        json.dumps({"messages": [], "tool_calls": [], "tool_results": []})
    )
    (output / "usage.json").write_text(json.dumps({"authority": "host-broker"}))
    (output / "result.json").write_text(json.dumps({"grade": {"verdict": "pass"}}))
    (output / "solution.diff").write_text("patch")
    ledger = directory / "private/id/calibration.sqlite3"
    ledger.parent.mkdir(parents=True)
    with sqlite3.connect(ledger) as db:
        db.execute("CREATE TABLE experiment_usage(kind, settled_json)")
        db.executemany(
            "INSERT INTO experiment_usage VALUES (?, ?)",
            [
                ("model", json.dumps({"tokens": 100, "requests": 1})),
                ("model", json.dumps({"tokens": 250, "requests": 1})),
                ("worker", json.dumps({"tokens": 900, "requests": 7})),
            ],
        )
    row = script("summarize_solver_calibration").summarize(tmp_path)["x-harness"][
        "total"
    ]
    assert row["model_requests"] == 2
    assert row["model_tokens"] == 350


def test_summary_counts_repeat_reads_and_first_edit(tmp_path):
    summary = script("summarize_solver_calibration")
    messages = [
        {"id": str(i), "role": "assistant", "conversation_sequence": i}
        for i in range(1, 4)
    ]
    calls = [
        {
            "id": "a",
            "assistant_message_id": "1",
            "name": "read_file",
            "arguments": {"path": "a.py"},
        },
        {
            "id": "b",
            "assistant_message_id": "2",
            "name": "read_file",
            "arguments": {"path": "a.py"},
        },
        {
            "id": "c",
            "assistant_message_id": "2",
            "name": "read_file",
            "arguments": {"path": "b.py"},
        },
        {
            "id": "d",
            "assistant_message_id": "3",
            "name": "edit_file",
            "arguments": {"path": "a.py"},
        },
    ]
    trace = {"messages": messages, "tool_calls": calls}
    path = tmp_path / "trace.json"
    path.write_text(json.dumps(trace))
    row = summary.trial_metrics(tmp_path)
    assert row["repeat_reads"] == 1
    assert row["read_calls"] == 3
    assert row["first_edit_request"] == [3]
    trace["tool_calls"] = calls[:-1]
    path.write_text(json.dumps(trace))
    no_edit = summary.trial_metrics(tmp_path)
    assert no_edit["first_edit_request"] == [None]
    assert summary.combine([row, no_edit])["first_edit_request"] == [3, None]
