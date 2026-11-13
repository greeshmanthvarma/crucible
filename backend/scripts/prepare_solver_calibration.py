"""Prepare local, interactive calibration plans; never execute a trial.

Run only after committing these scripts. For an old checkout, invoke this script
with that checkout's uv environment and --repository pointing at that checkout.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from crucible.improvement.calibration import (
    IsolatedCalibrationConfig,
    validate_calibration_inputs,
)
from crucible.improvement.contracts import EDITABLE_PATHS, CalibrationManifest
from crucible.improvement.source import _digest, inspect_source


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def trial_budget(limits: dict, policy: dict) -> dict:
    return {
        "max_reported_and_reserved_tokens": limits["max_model_tokens"],
        "max_model_requests": limits["max_steps"],
        "max_trial_starts": 1,
        "max_active_seconds": limits["max_active_seconds"],
        "max_worker_wall_seconds": limits["wall_timeout_seconds"],
        "max_total_output_bytes": policy["max_calls_per_trial"]
        * policy["max_output_bytes_per_call"],
    }


def write_runner(destination: Path) -> None:
    destination.write_text("""#!/bin/bash
set -eu
batch=${1:?usage: run.sh new|old}
case "$batch" in new|old) ;; *) echo 'Choose new or old' >&2; exit 2;; esac
root=$(cd -- "$(dirname -- "$0")" && pwd)
plan="$root/$batch-harness/batch.json"
[ -t 0 ] || { echo 'Terminal stdin required for manual approval' >&2; exit 2; }
# Python validates the frozen plan and emits shell-quoted arguments only.
eval "$(python3 - "$plan" <<'PYTHON'
import json, shlex, sys
p=json.load(open(sys.argv[1]))
print('repository='+shlex.quote(p['repository']))
print('expected='+shlex.quote(p['commit']))
PYTHON
)"
[ -z "$(git -C "$repository" status --porcelain)" ] || {
  echo 'Checkout must be clean' >&2; exit 2;
}
[ "$(git -C "$repository" rev-parse HEAD)" = "$expected" ] || {
  echo "Checkout must be at $expected" >&2; exit 2;
}
cd "$repository/backend"
for config in "$root/$batch-harness"/*/config.json; do
  output="$(dirname "$config")/output"
  if [ -e "$output" ]; then echo "Skipping existing $output"; continue; fi
  echo "Running $config"
  code=0
  uv run crucible benchmark isolated-trial \
    --config "$config" --output "$output" || code=$?
  echo "Finished $config (exit $code)"
  [ "$code" -ne 3 ] || { echo 'Infrastructure failure: stopping' >&2; exit 3; }
  [ "$code" -eq 0 ] || exit "$code"
done
""")
    destination.chmod(0o755)


def prepare(repository: Path, source_root: Path, batch: str) -> dict:
    if git(repository, "status", "--porcelain"):
        raise ValueError("commit scripts first: repository must be clean")
    original = source_root / "data/self-improvement/full-run-v1"
    frozen = json.loads((original / "experiment-manifest.json").read_text())
    registry = json.loads((original / "registry/registry.json").read_text())["cases"]
    commit = git(repository, "rev-parse", "HEAD")
    source = inspect_source(repository, commit)
    root = source_root / "data/self-improvement/calibration-v1"
    target = root / f"{batch}-harness"
    if target.exists():
        raise ValueError(f"refusing to overwrite batch: {target}")
    shared = {
        k: frozen[k]
        for k in (
            "worker_image",
            "worker_platform",
            "relay_image",
            "grader",
            "model",
            "trial_limits",
            "command_policy",
            "network_policy",
        )
    }
    shared.update(
        kind="isolated-calibration",
        schema_version=1,
        worker_protocol_version=2,
        authorization="one-isolated-calibration",
        approval_mode="interactive",
        initial_commit=commit,
        initial_source_digest=source.source_digest,
        controller_digest=_digest(
            {k: v for k, v in source.files.items() if k not in EDITABLE_PATHS}
        ),
        dependency_lock_digest=hashlib.sha256(
            (repository / "backend/uv.lock").read_bytes()
        ).hexdigest(),
        budget=trial_budget(frozen["trial_limits"], frozen["command_policy"]),
    )
    trials = []
    # Validate every plan before writing anything. This function only inspects
    # local files, git, admission/layout and hashes; it never starts execution.
    for case in frozen["cases"]:
        if case["split"] != "development":
            continue
        for repeat in range(3):
            trial_id = str(uuid4())
            directory = target / f"{case['case_id']}-r{repeat + 1}-{trial_id}"
            manifest = CalibrationManifest.model_validate_json(
                json.dumps(
                    {
                        **shared,
                        "experiment_id": str(uuid4()),
                        "trial_id": trial_id,
                        **{
                            k: registry[case["case_id"]][k]
                            for k in ("case_manifest", "admission_record")
                        },
                        "case_digest": case["case_digest"],
                        "admission_digest": case["admission_digest"],
                    }
                )
            )
            settings = IsolatedCalibrationConfig.model_validate_json(
                json.dumps(
                    {
                        "manifest": json.loads(manifest.model_dump_json()),
                        "repository": str(repository),
                        "data_dir": str(directory / "private"),
                    }
                )
            )
            output = directory / "output"
            validate_calibration_inputs(manifest, repository, output, settings.data_dir)
            trials.append((directory, settings))
    target.mkdir(parents=True)
    for directory, settings in trials:
        directory.mkdir(mode=0o700)
        settings.data_dir.mkdir(mode=0o700)
        (directory / "config.json").write_text(settings.model_dump_json(indent=2))
        (directory / "manifest.json").write_text(
            settings.manifest.model_dump_json(indent=2)
        )
    plan = {
        "repository": str(repository),
        "commit": commit,
        "trials": len(trials),
        "max_tokens": len(trials)
        * shared["budget"]["max_reported_and_reserved_tokens"],
    }
    (target / "batch.json").write_text(json.dumps(plan, indent=2) + "\n")
    write_runner(root / "run.sh")
    return plan


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--source-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument("--batch", choices=("new", "old"), default="new")
    args = parser.parse_args()
    print(
        json.dumps(
            prepare(args.repository.resolve(), args.source_root.resolve(), args.batch),
            indent=2,
        )
    )
