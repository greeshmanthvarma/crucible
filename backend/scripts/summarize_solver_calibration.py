"""Read calibration and frozen baseline evidence; no execution or model access."""

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


def read(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def trial_metrics(directory: Path) -> dict:
    envelope = read(directory / "result.json")
    result = envelope.get("result", envelope)
    if not result:
        result = read(directory / "authoritative-result.json")
    usage = read(directory / "usage.json").get("usage", {})
    trace = read(directory / "trace.json").get("data", {})
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
        "model_requests": usage.get("requests", 0),
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
    for harness in ("new", "old"):
        for config in sorted(
            (root / "data/self-improvement/calibration-v1" / f"{harness}-harness").glob(
                "*/config.json"
            )
        ):
            output = config.parent / "output"
            if not (output / "result.json").exists():
                continue
            frozen = read(config)["manifest"]
            case = read(Path(frozen["case_manifest"]))["case_id"]
            rows[f"{harness}-harness"][case].append(trial_metrics(output))
    return {
        harness: {
            "cases": {case: combine(values) for case, values in sorted(cases.items())},
            "total": combine([v for values in cases.values() for v in values]),
        }
        for harness, cases in rows.items()
    }


def table(report: dict) -> str:
    lines = ["harness / case | passes/trials | empty | requests | input tokens"]
    for harness, data in report.items():
        for case, row in [*data["cases"].items(), ("TOTAL", data["total"])]:
            lines.append(
                f"{harness} / {case} | {row['passes']}/{row['trials']} | "
                f"{row['empty_patches']} | {row['model_requests']} | "
                f"{row['input_tokens']}"
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
