"""The public improve preview operation is read-only and entirely offline."""

import hashlib
import json

from crucible.evals.cli import run_cli
from crucible.improvement.store import ExperimentStore
from crucible.improvement.worker_supervisor import DockerRuntime
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from tests.unit import test_case_evidence as fixtures

nonpatch_package = fixtures.nonpatch_package
parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


def _hashes(root):
    return {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


async def test_preview_never_opens_execution_ports(
    nonpatch_package, monkeypatch, capsys
):
    def unexpected(*args, **kwargs):
        raise AssertionError("preview opened an execution port")

    monkeypatch.setattr(ExperimentStore, "open", unexpected)
    monkeypatch.setattr(DockerRuntime, "__init__", unexpected)
    monkeypatch.setattr(LiteLLMResponsesGateway, "__init__", unexpected)
    root, package, trial_id = nonpatch_package
    baseline = _hashes(root)
    args = [
        "improve",
        "evidence-preview",
        "--package",
        str(root),
        "--bundle-digest",
        package.bundle_digest,
        "--trial",
        str(trial_id),
    ]
    assert await run_cli(args) == 0
    initial = json.loads(capsys.readouterr().out)
    assert initial["followup"] is None
    assert initial["total_reserved_tokens"] == initial["initial"]["input_tokens"] + 2000

    assert await run_cli([*args, "--detail", "E6"]) == 0
    stdout, stderr = capsys.readouterr()
    expanded = json.loads(stdout)
    assert stderr == ""
    assert expanded["initial"] == initial["initial"]
    assert expanded["packet"] == initial["packet"]
    assert expanded["followup"]["input_tokens"] <= 12000
    assert expanded["total_reserved_tokens"] == (
        expanded["initial"]["input_tokens"]
        + expanded["followup"]["input_tokens"]
        + 4000
    )
    assert "RAW_COMMAND_OUTPUT_SENTINEL" not in stdout
    assert "_details" not in expanded
    assert package.bundle_digest not in stdout
    assert str(root) not in stdout

    assert await run_cli([*args, "--detail", "../source/private"]) == 2
    rejected = capsys.readouterr()
    assert rejected.out == ""
    assert rejected.err == "Evidence preview rejected (invalid_refs).\n"
    bad_trial = args[:-1] + ["not-a-uuid"]
    assert await run_cli(bad_trial) == 2
    rejected = capsys.readouterr()
    assert rejected.out == ""
    assert rejected.err == "Evidence preview rejected (invalid_target).\n"
    assert _hashes(root) == baseline
