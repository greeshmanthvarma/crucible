"""Read calibration and frozen baseline evidence; no execution or model access."""

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from crucible.engine.coding_behavior import MUTATION_TOOLS


def read(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def trial_metrics(directory: Path, ledger: Path | None = None) -> dict:
    envelope = read(directory / "result.json")
    result = envelope.get("result", envelope)
    if not result:
        result = read(directory / "authoritative-result.json")
    usage = read(directory / "usage.json").get("usage", {})
    raw = read(directory / "trace.json")
    trace = raw.get("data", raw)
    model_requests = usage.get("requests", 0)
    model_tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
    if ledger is not None and ledger.exists():
        with sqlite3.connect(f"{ledger.as_uri()}?mode=ro", uri=True) as connection:
            settled = [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT settled_json FROM experiment_usage WHERE kind='model'"
                )
            ]
        model_requests = sum(row.get("requests", 0) for row in settled)
        model_tokens = sum(row.get("tokens", 0) for row in settled)
    assistants = {
        message["id"]: index
        for index, message in enumerate(
            sorted(
                (m for m in trace.get("messages", []) if m["role"] == "assistant"),
                key=lambda m: m["conversation_sequence"],
            ),
            1,
        )
    }
    seen = set()
    read_calls = repeat_reads = 0
    first_edit = None
    for call in sorted(
        trace.get("tool_calls", []), key=lambda c: c.get("created_at", "")
    ):
        if call["name"] == "read_file":
            path = call.get("arguments", {}).get("path")
            read_calls += 1
            repeat_reads += int(path in seen)
            seen.add(path)
        if call["name"] in MUTATION_TOOLS:
            request = assistants.get(call.get("assistant_message_id"))
            if request is not None:
                first_edit = request if first_edit is None else min(first_edit, request)
    calls = {c["id"]: c["name"] for c in trace.get("tool_calls", [])}
    outcomes: Counter[str] = Counter()
    for item in trace.get("tool_results", []):
        name = calls.get(item.get("tool_call_id"))
        if name in ("edit_file", "apply_patch"):
            outcomes[f"{name}:{item['status']}:{item.get('error_code') or 'none'}"] += 1
    patch = directory / "solution.diff"
    return {
        "trials": 1,
        "passes": int((result.get("grade") or {}).get("verdict") == "pass"),
        "empty_patches": int(patch.exists() and not patch.read_bytes().strip()),
        "missing_patches": int(not patch.exists()),
        "model_requests": model_requests,
        "model_tokens": model_tokens,
        "read_calls": read_calls,
        "repeat_reads": repeat_reads,
        "first_edit_request": [first_edit],
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "termination": {
            (result.get("solver") or {}).get("termination")
            or result.get("error_code")
            or "unknown": 1
        },
        "edit_file_calls": list(calls.values()).count("edit_file"),
        "apply_patch_calls": list(calls.values()).count("apply_patch"),
        "list_files_calls": list(calls.values()).count("list_files"),
        "tool_outcomes": dict(outcomes),
    }


def combine(rows: list[dict]) -> dict:
    result: dict = {}
    for row in rows:
        for key, value in row.items():
            if isinstance(value, dict):
                result.setdefault(key, Counter()).update(value)
            elif isinstance(value, list):
                result.setdefault(key, []).extend(value)
            else:
                result[key] = result.get(key, 0) + value
    return result


def summarize(root: Path) -> dict:
    original = root / "data/self-improvement/full-run-v1"
    manifest = read(original / "experiment-manifest.json")
    development = {
        c["case_id"] for c in manifest["cases"] if c["split"] == "development"
    }
    rows: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    database = original / "runtime/experiments.sqlite3"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        baseline = connection.execute(
            "SELECT t.id, t.case_id FROM experiment_trials t "
            "JOIN experiment_snapshots s ON s.id=t.snapshot "
            "WHERE t.experiment=? AND s.kind='baseline' AND s.parent_commit=?",
            (manifest["experiment_id"], manifest["initial_commit"]),
        ).fetchall()
    for trial_id, case in baseline:
        if case in development:
            rows["original-baseline"][case].append(
                trial_metrics(original / "runtime/trials" / trial_id)
            )
    calibration_root = root / "data/self-improvement/calibration-v1"
    for harness in sorted(calibration_root.glob("*-harness")):
        for config in sorted(harness.glob("*/config.json")):
            output = config.parent / "output"
            if not (output / "result.json").exists():
                continue
            frozen = read(config)["manifest"]
            case = read(Path(frozen["case_manifest"]))["case_id"]
            ledger = next(
                (config.parent / "private").glob("*/calibration.sqlite3"), None
            )
            rows[harness.name][case].append(trial_metrics(output, ledger))
    return {
        harness: {
            "cases": {case: combine(values) for case, values in sorted(cases.items())},
            "total": combine([v for values in cases.values() for v in values]),
        }
        for harness, cases in rows.items()
    }


def table(report: dict) -> str:
    lines = [
        "harness / case | passes/trials | empty | requests | "
        "model tokens | repeat reads/reads"
    ]
    for harness, data in report.items():
        for case, row in [*data["cases"].items(), ("TOTAL", data["total"])]:
            lines.append(
                f"{harness} / {case} | {row['passes']}/{row['trials']} | "
                f"{row['empty_patches']} | {row['model_requests']} | "
                f"{row['model_tokens']} | {row['repeat_reads']}/{row['read_calls']}"
            )
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    args = parser.parse_args()
    report = summarize(args.root.resolve())
    print(json.dumps(report, indent=2))
    print(table(report))
