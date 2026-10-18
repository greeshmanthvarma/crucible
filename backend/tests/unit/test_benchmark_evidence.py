from pathlib import Path

import pytest

from crucible.benchmarks.evidence import TrialEvidence


def test_interrupted_trial_is_not_reexecuted_implicitly(tmp_path: Path) -> None:
    output = tmp_path / "trial"
    evidence = TrialEvidence(output, {"trial_id": "trial"})
    evidence.stage("solving")
    assert TrialEvidence.show(output)["status"] == "interrupted"
    before = (output / "request.json").read_bytes()
    with pytest.raises(FileExistsError):
        TrialEvidence(output, {"trial_id": "trial"})
    assert (output / "request.json").read_bytes() == before


def test_atomic_write_interruption_retains_prior_result(tmp_path, monkeypatch):
    import json

    from crucible.benchmarks.evidence import atomic_json

    path = tmp_path / "record.json"
    atomic_json(path, {"status": "prior"})

    def interrupted(*args):
        raise OSError("interrupted replace")

    monkeypatch.setattr("crucible.benchmarks.evidence.os.replace", interrupted)
    with pytest.raises(OSError):
        atomic_json(path, {"status": "new"})
    assert json.loads(path.read_text()) == {"status": "prior"}
    assert not list(tmp_path.glob(".record-*"))


def test_secret_sanitization_and_artifact_escape(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_API_KEY", "private-value")
    evidence = TrialEvidence(
        tmp_path / "evidence", {"trial_id": "trial", "note": "private-value"}
    )
    assert "private-value" not in (evidence.root / "request.json").read_text()
    evidence.stage("solving")
    evidence.stage("snapshotting")
    evidence.stage("grading")
    with pytest.raises(ValueError, match="escapes"):
        evidence.finish(
            {"status": "complete", "grade": {"log_path": str(tmp_path / "outside")}},
            "complete",
        )
    assert not (evidence.root / "result.json").exists()


def test_complete_evidence_has_bounded_status_and_relative_artifacts(tmp_path):
    evidence = TrialEvidence(tmp_path / "evidence", {"identity": {"trial_id": "trial"}})
    for stage in ["solving", "snapshotting", "grading"]:
        evidence.stage(stage)
    log = evidence.root / "log.txt"
    log.write_text("private detailed report")
    evidence.finish(
        {"status": "complete", "grade": {"verdict": "fail", "log_path": str(log)}},
        "complete",
    )
    view = TrialEvidence.show(evidence.root)
    assert view["status"] == "complete"
    assert view["verdict"] == "fail"
    assert "private detailed report" not in str(view)
    assert '"log_path":"log.txt"' in (evidence.root / "result.json").read_text()


def test_crash_tail_is_read_as_interrupted_without_rewriting(tmp_path):
    evidence = TrialEvidence(tmp_path / "trial", {"trial_id": "trial"})
    path = evidence.root / "events.jsonl"
    with path.open("ab") as stream:
        stream.write(b'{"stage":"solv')
    before = path.read_bytes()
    assert TrialEvidence.show(evidence.root)["status"] == "interrupted"
    assert path.read_bytes() == before
