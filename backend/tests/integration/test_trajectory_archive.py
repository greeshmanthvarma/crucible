"""Archive preparation never migrates or changes historical evidence."""

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest

from crucible.improvement.trajectory_contracts import ExportLimits, TrialSelection
from crucible.improvement.trajectory_export import (
    export_trajectory_package,
    verify_trajectory_package,
)
from crucible.improvement.verified_evidence import DevelopmentArchiveReader
from crucible.repair.identity import canonical_json
from tests.unit.test_improvement_evidence_views import evidence_setup  # noqa: F401


@dataclass
class Archive:
    root: Path
    experiment_id: UUID
    snapshot_id: UUID
    selection: TrialSelection

    def hash_all_original_files(self):
        return {
            p.relative_to(self.root).as_posix(): hashlib.sha256(
                p.read_bytes()
            ).hexdigest()
            for p in self.root.rglob("*")
            if p.is_file()
        }


@pytest.fixture
async def archive_fixture(evidence_setup):  # noqa: F811
    store, root, run, snapshot, ids = evidence_setup
    await store.close()
    (root / "state.sqlite3").rename(root / "experiments.sqlite3")
    with sqlite3.connect(root / "experiments.sqlite3") as conn:
        for trial_id in ids:
            path = root / "trials" / str(trial_id) / "result.json"
            envelope = json.loads(path.read_bytes())
            solver = envelope["result"]["solver"]
            raw = canonical_json(
                {
                    "schema_version": 2,
                    "messages": [
                        {
                            "id": str(UUID(int=500)),
                            "task_id": solver["task_id"],
                            "run_id": solver["run_id"],
                            "step_id": None,
                            "conversation_sequence": 1,
                            "role": "user",
                            "status": "completed",
                            "parts": [
                                {
                                    "id": str(UUID(int=501)),
                                    "part_sequence": 1,
                                    "kind": "text",
                                    "text_content": "Task token-canary " + str(root),
                                    "reasoning_content": None,
                                    "tool_call_id": None,
                                    "tool_result_id": None,
                                }
                            ],
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "completed_at": "2026-01-01T00:00:00+00:00",
                        }
                    ],
                    "tool_calls": [],
                    "tool_results": [],
                }
            )
            (path.parent / "trace.json").write_bytes(raw)
            envelope["artifacts"]["trace.json"] = hashlib.sha256(raw).hexdigest()
            path.write_bytes(canonical_json(envelope))
            conn.execute(
                "UPDATE experiment_trials SET artifact_ref=? WHERE id=?",
                (
                    json.dumps(
                        {
                            "path": str(path.relative_to(root)),
                            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        }
                    ),
                    str(trial_id),
                ),
            )
        conn.execute("UPDATE experiment_runs SET status='error', stage='mining'")
    conn.close()
    return Archive(
        root,
        run.id,
        snapshot.snapshot_id,
        TrialSelection(mode="explicit-three-repeat", trial_ids=tuple(ids)),
    )


def load(archive):
    return DevelopmentArchiveReader(archive.root).load(
        archive.experiment_id, archive.snapshot_id, archive.selection
    )


async def test_historical_export_is_read_only(archive_fixture, tmp_path):
    before = archive_fixture.hash_all_original_files()
    inputs = load(archive_fixture)
    package = export_trajectory_package(
        inputs, tmp_path / "public-view", ExportLimits()
    )
    assert package.selected_trial_ids == inputs.selected_trial_ids
    assert verify_trajectory_package(tmp_path / "public-view") == package
    assert archive_fixture.hash_all_original_files() == before


async def test_package_contains_only_selected_development_evidence(
    archive_fixture, tmp_path, monkeypatch
):
    import crucible.improvement.verified_evidence as evidence

    original = evidence.read_regular_artifact
    opened = []

    def read(path, maximum):
        opened.append(path)
        return original(path, maximum)

    monkeypatch.setattr(evidence, "read_regular_artifact", read)
    package = export_trajectory_package(
        load(archive_fixture), tmp_path / "view", ExportLimits()
    )
    contents = b"".join(
        p.read_bytes() for p in (tmp_path / "view").rglob("*") if p.is_file()
    )
    for canary in (
        b"HIDDEN_TRACE_CANARY",
        b"REFERENCE_CANARY",
        b"PRIVATE_TEST_CANARY",
        b"PRIVATE_CANARY",
        b"selection-validation",
        b"final-test",
        str(archive_fixture.root).encode(),
    ):
        assert canary not in contents
    trial_root = archive_fixture.root / "trials"
    assert all(
        p.parts[len(trial_root.parts)] in set(map(str, package.selected_trial_ids))
        for p in opened
        if p.is_relative_to(trial_root)
    )


async def test_secrets_redacted_in_every_derived_file(
    archive_fixture, tmp_path, monkeypatch
):
    from dataclasses import replace

    inputs = load(archive_fixture)
    monkeypatch.setenv("EXPORT_API_KEY", "token-canary")
    trial = inputs.trials[0]
    data = json.loads(trial.trace_bytes)
    data["messages"][0]["parts"][0]["text_content"] += (
        " Bearer abcdefghijk /Users/person/private.txt file:///sensitive.txt"
    )
    raw = canonical_json(data)
    trial = replace(
        trial,
        trace_bytes=raw,
        trace_ref=trial.trace_ref.model_copy(
            update={"sha256": hashlib.sha256(raw).hexdigest()}
        ),
    )
    inputs = replace(inputs, trials=(trial, *inputs.trials[1:]))
    export_trajectory_package(inputs, tmp_path / "view", ExportLimits())
    for path in (tmp_path / "view").rglob("*"):
        if path.is_file() and "source" not in path.relative_to(tmp_path / "view").parts:
            raw = path.read_bytes()
            for secret in (
                b"token-canary",
                b"abcdefghijk",
                b"/Users/person",
                b"file://",
                str(archive_fixture.root).encode(),
            ):
                assert secret not in raw


@pytest.mark.parametrize("target", ["snapshot", "trace", "manifest", "solution"])
async def test_snapshot_and_original_artifact_digest_drift_rejected(
    archive_fixture, target
):
    from crucible.improvement.trajectory import TrajectoryError

    if target == "manifest":
        with sqlite3.connect(archive_fixture.root / "experiments.sqlite3") as conn:
            conn.execute("UPDATE experiment_runs SET manifest_hash=?", ("0" * 64,))
        conn.close()
    else:
        path = (
            archive_fixture.root / "snapshot/snapshot.json"
            if target == "snapshot"
            else archive_fixture.root
            / "trials"
            / str(archive_fixture.selection.trial_ids[0])
            / ("trace.json" if target == "trace" else "solution.diff")
        )
        path.write_text("{}")
    with pytest.raises(TrajectoryError):
        load(archive_fixture)


async def test_malformed_trace_rejected_before_any_export(archive_fixture, tmp_path):
    from dataclasses import replace

    from crucible.improvement.trajectory import TrajectoryError

    inputs = load(archive_fixture)
    trial = inputs.trials[-1]
    raw = b'{"schema_version":2,"messages":['
    trial = replace(
        trial,
        trace_bytes=raw,
        trace_ref=trial.trace_ref.model_copy(
            update={"sha256": hashlib.sha256(raw).hexdigest()}
        ),
    )
    inputs = replace(inputs, trials=(*inputs.trials[:-1], trial))
    with pytest.raises(TrajectoryError, match="invalid_trace"):
        export_trajectory_package(inputs, tmp_path / "view", ExportLimits())
    assert not (tmp_path / "view").exists()
    assert not list(tmp_path.glob(".incomplete-trajectory-*"))


async def test_failed_family_selection_is_deterministic(archive_fixture):
    automatic = TrialSelection(mode="failed-family-v1", trial_ids=())
    reader = DevelopmentArchiveReader(archive_fixture.root)
    inputs = reader.load(
        archive_fixture.experiment_id, archive_fixture.snapshot_id, automatic
    )
    assert inputs.selected_trial_ids == archive_fixture.selection.trial_ids
    assert (
        reader.load(
            archive_fixture.experiment_id, archive_fixture.snapshot_id, inputs.selection
        ).selection
        == inputs.selection
    )
    wrong = TrialSelection(
        mode="failed-family-v1", trial_ids=tuple(reversed(inputs.selected_trial_ids))
    )
    with pytest.raises(ValueError, match="frozen automatic selection"):
        reader.load(archive_fixture.experiment_id, archive_fixture.snapshot_id, wrong)


async def test_no_failures_is_explicit_no_selection(archive_fixture):
    with sqlite3.connect(archive_fixture.root / "experiments.sqlite3") as conn:
        conn.execute("UPDATE experiment_trials SET grade_verdict='pass'")
    conn.close()
    with pytest.raises(ValueError, match="no_failed_development_trial"):
        DevelopmentArchiveReader(archive_fixture.root).load(
            archive_fixture.experiment_id,
            archive_fixture.snapshot_id,
            TrialSelection(mode="failed-family-v1", trial_ids=()),
        )


@pytest.mark.parametrize("change", ["limits", "generation"])
async def test_selected_generation_and_limits_must_match(archive_fixture, change):
    with sqlite3.connect(archive_fixture.root / "experiments.sqlite3") as conn:
        if change == "generation":
            conn.execute(
                "INSERT INTO experiment_trials SELECT "
                "'00000000-0000-0000-0000-000000000099', experiment, snapshot, "
                "case_id, repeat_index, key, status, generation+1, worker_job_id, "
                "artifact_ref, grade_verdict, solver_termination, error_code "
                "FROM experiment_trials WHERE id=?",
                (str(archive_fixture.selection.trial_ids[0]),),
            )
        else:
            row = conn.execute("SELECT manifest_json FROM experiment_runs").fetchone()
            from crucible.improvement.contracts import ExperimentManifest
            from crucible.improvement.manifest import manifest_digest

            payload = json.loads(row[0])
            payload["trial_limits"]["max_steps"] += 1
            manifest = ExperimentManifest.model_validate_json(canonical_json(payload))
            conn.execute(
                "UPDATE experiment_runs SET manifest_json=?, manifest_hash=?",
                (manifest.model_dump_json(), manifest_digest(manifest)),
            )
    conn.close()
    with pytest.raises(ValueError, match="identity_mismatch"):
        load(archive_fixture)


async def test_package_limit_is_not_event_selection(archive_fixture, tmp_path):
    inputs = load(archive_fixture)
    with pytest.raises(ValueError, match="export_limit"):
        export_trajectory_package(
            inputs, tmp_path / "view", ExportLimits(evidence_bytes=1000)
        )
    assert not (tmp_path / "view").exists()


async def test_live_wal_snapshot_is_read_consistently(archive_fixture):
    writer = sqlite3.connect(archive_fixture.root / "experiments.sqlite3")
    try:
        writer.execute("UPDATE experiment_runs SET error_code='committed_wal'")
        writer.commit()
        writer.execute("UPDATE experiment_trials SET status='error'")  # uncommitted
        before = archive_fixture.hash_all_original_files()
        assert len(load(archive_fixture).trials) == 3
        assert archive_fixture.hash_all_original_files() == before
    finally:
        writer.rollback()
        writer.close()


async def test_archive_changed_during_capture_fails_closed(
    archive_fixture, monkeypatch
):
    import crucible.improvement.verified_evidence as evidence

    original = evidence.read_regular_artifact
    changed = False

    def read(path, maximum):
        nonlocal changed
        raw = original(path, maximum)
        if path.name == "experiments.sqlite3" and not changed:
            changed = True
            with sqlite3.connect(path) as writer:
                writer.execute("UPDATE experiment_runs SET error_code='changed'")
            writer.close()
        return raw

    monkeypatch.setattr(evidence, "read_regular_artifact", read)
    with pytest.raises(ValueError, match="archive changed during snapshot"):
        load(archive_fixture)


async def test_source_and_state_overlap_or_symlink_rejected(archive_fixture, tmp_path):
    inputs = load(archive_fixture)
    for output in (
        archive_fixture.root / "view",
        archive_fixture.root.parent,
        archive_fixture.root / "snapshot/harness/view",
    ):
        with pytest.raises(ValueError):
            export_trajectory_package(inputs, output, ExportLimits())
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        export_trajectory_package(inputs, link / "view", ExportLimits())


async def test_duplicate_matching_baselines_are_rejected(archive_fixture):
    with sqlite3.connect(archive_fixture.root / "experiments.sqlite3") as conn:
        conn.execute(
            "INSERT INTO experiment_snapshots SELECT ?, experiment, "
            "parent_commit, kind, source_digest, ?, patch_digest, "
            "artifact_ref, status FROM experiment_snapshots",
            (str(UUID(int=999)), "f" * 64),
        )
    conn.close()
    with pytest.raises(ValueError, match="identity_mismatch"):
        load(archive_fixture)


async def test_snapshot_final_fence_catches_change_to_already_read_db(
    archive_fixture, monkeypatch
):
    import crucible.improvement.verified_evidence as evidence

    writer = sqlite3.connect(archive_fixture.root / "experiments.sqlite3")
    writer.execute("UPDATE experiment_runs SET error_code='wal'")
    writer.commit()
    original = evidence.read_regular_artifact
    wal_reads = 0

    def read(path, maximum):
        nonlocal wal_reads
        raw = original(path, maximum)
        if path.name == "experiments.sqlite3-wal":
            wal_reads += 1
            if wal_reads == 2:
                db = archive_fixture.root / "experiments.sqlite3"
                # Simulate a checkpoint changing the DB after its second read.
                import os

                info = db.stat()
                os.utime(db, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
        return raw

    monkeypatch.setattr(evidence, "read_regular_artifact", read)
    try:
        with pytest.raises(ValueError, match="archive changed during snapshot"):
            load(archive_fixture)
    finally:
        writer.close()


def reseal(output):
    from crucible.repair.identity import digest

    manifest_path = output / "package-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    for name in manifest["files"]:
        manifest["files"][name] = hashlib.sha256(
            (output / name).read_bytes()
        ).hexdigest()
    manifest["bundle_digest"] = digest(
        {k: v for k, v in manifest.items() if k != "bundle_digest"}
    )
    manifest_path.write_bytes(canonical_json(manifest))


async def test_empty_detail_file_registry_has_bounded_failure(
    archive_fixture, tmp_path
):
    from crucible.improvement.trajectory import TrajectoryError

    output = tmp_path / "view"
    export_trajectory_package(load(archive_fixture), output, ExportLimits())
    path = next(output.glob("trials/*/details-manifest.json"))
    entries = json.loads(path.read_bytes())
    entries[0]["files"] = []
    path.write_bytes(canonical_json(entries))
    reseal(output)
    with pytest.raises(TrajectoryError):
        verify_trajectory_package(output)


async def test_concurrent_output_is_never_overwritten(
    archive_fixture, tmp_path, monkeypatch
):
    import os

    output = tmp_path / "view"
    original_mkdir, original_rename = os.mkdir, os.rename
    competitor_inode = None

    def mkdir(path, mode=0o777, *, dir_fd=None):
        nonlocal competitor_inode
        if Path(path) == output:
            original_mkdir(path, mode)
            competitor_inode = output.stat().st_ino
        return original_mkdir(path, mode, dir_fd=dir_fd)

    def rename(source, destination, *args, **kwargs):
        nonlocal competitor_inode
        if Path(destination) == output and not output.exists():
            original_mkdir(output, 0o700)
            competitor_inode = output.stat().st_ino
        return original_rename(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "mkdir", mkdir)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(ValueError):
        export_trajectory_package(load(archive_fixture), output, ExportLimits())
    assert output.stat().st_ino == competitor_inode
    assert not list(output.iterdir())
    for staging in tmp_path.glob(".incomplete-trajectory-*"):
        assert not (staging / "package-manifest.json").exists()


async def test_usage_requires_hash_verified_controller_artifact(archive_fixture):
    trial_id = archive_fixture.selection.trial_ids[0]
    directory = archive_fixture.root / "trials" / str(trial_id)
    usage = directory / "usage.json"
    usage.write_bytes(
        canonical_json(
            {
                "trust": "protected-host",
                "usage": {
                    "requests": 2,
                    "input_tokens": 123,
                    "output_tokens": 45,
                    "basis": "reported",
                },
                "commands": {},
            }
        )
    )
    # An unregistered usage file / solver host path is never a fallback.
    assert load(archive_fixture).trials[0].usage is None
    result_path = directory / "result.json"
    envelope = json.loads(result_path.read_bytes())
    envelope["artifacts"]["usage.json"] = hashlib.sha256(usage.read_bytes()).hexdigest()
    result_path.write_bytes(canonical_json(envelope))
    with sqlite3.connect(archive_fixture.root / "experiments.sqlite3") as conn:
        conn.execute(
            "UPDATE experiment_trials SET artifact_ref=? WHERE id=?",
            (
                json.dumps(
                    {
                        "path": str(result_path.relative_to(archive_fixture.root)),
                        "sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                    }
                ),
                str(trial_id),
            ),
        )
    conn.close()
    trusted = load(archive_fixture).trials[0].usage
    assert trusted.input_tokens == 123 and trusted.output_tokens == 45
    usage.write_text("{}")
    with pytest.raises(ValueError, match="artifact_drift"):
        load(archive_fixture)


async def test_evidence_export_never_opens_write_store_or_gateway(
    archive_fixture, tmp_path, monkeypatch
):
    from crucible.improvement.cli import run_cli
    from crucible.improvement.store import ExperimentStore
    from crucible.improvement.worker_supervisor import DockerRuntime, WorkerSupervisor
    from crucible.repair.gateway import ScopedModelGateway
    from crucible.sandbox.docker import DockerSandboxBackend

    def forbidden(*args, **kwargs):
        raise AssertionError("offline exporter crossed execution boundary")

    monkeypatch.setattr(ExperimentStore, "open", forbidden)
    monkeypatch.setattr(WorkerSupervisor, "__init__", forbidden)
    monkeypatch.setattr(ScopedModelGateway, "__init__", forbidden)
    monkeypatch.setattr(DockerSandboxBackend, "__init__", forbidden)
    monkeypatch.setattr(DockerRuntime, "__init__", forbidden)
    selection_file = tmp_path / "selection.json"
    selection_file.write_text(archive_fixture.selection.model_dump_json())
    output = tmp_path / "view"
    assert (
        await run_cli(
            [
                "evidence-export",
                "--source-state-root",
                str(archive_fixture.root),
                "--experiment",
                str(archive_fixture.experiment_id),
                "--snapshot",
                str(archive_fixture.snapshot_id),
                "--selection-file",
                str(selection_file),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert (
        verify_trajectory_package(output).selected_trial_ids
        == archive_fixture.selection.trial_ids
    )
    assert await run_cli(["evidence-verify", "--package", str(output)]) == 0


@pytest.mark.parametrize("mutation", ["missing", "tampered", "extra", "source-mode"])
async def test_missing_or_tampered_detail_fails_verification(
    archive_fixture, tmp_path, mutation
):
    output = tmp_path / "view"
    export_trajectory_package(load(archive_fixture), output, ExportLimits())
    detail = next(output.glob("trials/*/details/*.message.json"))
    if mutation == "missing":
        detail.unlink()
    elif mutation == "tampered":
        detail.write_text("{}")
    elif mutation == "extra":
        (output / "extra.json").write_text("{}")
    else:
        next(p for p in (output / "source").rglob("*") if p.is_file()).chmod(0o755)
    with pytest.raises(ValueError, match="artifact_drift"):
        verify_trajectory_package(output)
    from crucible.improvement.cli import run_cli

    assert await run_cli(["evidence-verify", "--package", str(output)]) == 2


async def test_all_index_records_resolve_without_full_trace_read(
    archive_fixture, tmp_path
):
    output = tmp_path / "view"
    package = export_trajectory_package(load(archive_fixture), output, ExportLimits())
    locator = json.loads((output / package.record_locator_ref).read_bytes())
    for trial_id in package.selected_trial_ids:
        prefix = f"trials/{trial_id}/"
        overview = json.loads(
            (output / package.trial_overview_refs[trial_id]).read_bytes()
        )
        for line in (output / prefix / overview["index_ref"]).read_bytes().splitlines():
            record = json.loads(line)
            target = prefix + record["detail_ref"]
            matches = [
                entry
                for entry in locator
                if entry["trial_id"] == str(trial_id)
                and entry["original_record_id"] == record["record_id"]
                and entry["detail_ref"] == target
            ]
            assert len(matches) == 1
            assert (
                hashlib.sha256((output / target).read_bytes()).hexdigest()
                == matches[0]["detail_sha256"]
            )
    assert not list(output.rglob("trace.json"))


async def test_export_does_not_require_provider_credentials(
    archive_fixture, tmp_path, monkeypatch
):
    import os

    from crucible.improvement.cli import run_cli

    for key in list(os.environ):
        if key.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD")):
            monkeypatch.delenv(key)
    selection = tmp_path / "selection.json"
    selection.write_text(archive_fixture.selection.model_dump_json())
    assert (
        await run_cli(
            [
                "evidence-export",
                "--source-state-root",
                str(archive_fixture.root),
                "--experiment",
                str(archive_fixture.experiment_id),
                "--snapshot",
                str(archive_fixture.snapshot_id),
                "--selection-file",
                str(selection),
                "--output",
                str(tmp_path / "view"),
            ]
        )
        == 0
    )
