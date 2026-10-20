"""Role schemas refuse references that could name arbitrary private files."""

import pytest

from crucible.improvement.roles import EvidenceRef


def test_arbitrary_path_ref_rejected():
    with pytest.raises(ValueError):
        EvidenceRef.model_validate_json(
            '{"trial_id":"00000000-0000-0000-0000-000000000001",'
            '"artifact":"../../reference.patch","sha256":"' + "a" * 64 + '"}'
        )


@pytest.fixture
async def evidence_setup(tmp_path, request):
    import hashlib
    import json
    from uuid import uuid4

    from crucible.benchmarks.contracts import (
        GradeResult,
        SolutionPatch,
        SolverResult,
        TrialIdentity,
        TrialResult,
    )
    from crucible.improvement.source import _digest
    from crucible.improvement.store import ExperimentStore
    from crucible.repair.identity import canonical_json, digest
    from tests.unit.test_improvement_search_store import baseline, search_manifest

    root = tmp_path / "state"
    root.mkdir()
    store = await ExperimentStore.open(root / "state.sqlite3")
    manifest = search_manifest()
    run = await store.create(manifest, "repo", "key")
    harness = root / "snapshot/harness"
    harness.mkdir(parents=True)
    source = harness / "backend/src/crucible/tools/filesystem.py"
    source.parent.mkdir(parents=True)
    source.write_text("# ordinary source\n")
    snapshot = baseline(manifest).model_copy(
        update={
            "export_digest": _digest(
                {
                    "backend/src/crucible/tools/filesystem.py": (
                        0o100644,
                        source.read_bytes(),
                    )
                }
            )
        }
    )
    (harness.parent / "snapshot.json").write_text(snapshot.model_dump_json())
    await store.register_snapshot(run.id, snapshot, "snapshot/snapshot.json")
    ids = []
    for case in ("development", "selection-validation"):
        for repeat in range(3):
            trial = await store.create_trial(
                run.id, snapshot.snapshot_id, case, repeat, "key"
            )
            directory = root / "trials" / str(trial)
            directory.mkdir(parents=True)
            trace = directory / "trace.json"
            trace.write_text(
                json.dumps(
                    getattr(request, "param", {"data": "ordinary trace"})
                    if case == "development"
                    else {"data": "HIDDEN_TRACE_CANARY"}
                )
            )
            patch = directory / "solution.diff"
            patch.write_text("ordinary patch")
            sha = hashlib.sha256(patch.read_bytes()).hexdigest()
            result = TrialResult(
                TrialIdentity(
                    trial_id=trial,
                    case_digest="f" * 64,
                    harness_commit=manifest.initial_commit,
                    harness_tree_digest=snapshot.source_digest,
                    model_config_digest=digest(manifest.model),
                    limits_digest=digest(manifest.trial_limits),
                    repeat_index=repeat,
                ),
                SolverResult(
                    "budget_exhausted",
                    uuid4(),
                    uuid4(),
                    directory / "workspace",
                    trace,
                    directory / "usage.json",
                ),
                SolutionPatch(patch, sha, "a" * 40, patch.stat().st_size),
                GradeResult(
                    "pass" if repeat else "fail",
                    "official_report",
                    "id",
                    bool(repeat),
                    sha,
                    directory / "REFERENCE_CANARY.json",
                    directory / "PRIVATE_TEST_CANARY.log",
                    "a" * 40,
                    1.0,
                ),
                "complete",
                None,
            )
            envelope = {
                "result": result,
                "artifacts": {
                    "trace.json": hashlib.sha256(trace.read_bytes()).hexdigest(),
                    "solution.diff": sha,
                },
            }
            path = directory / "result.json"
            path.write_bytes(canonical_json(envelope))
            await store.trial_state(
                trial,
                "complete",
                artifact=json.dumps(
                    {
                        "path": str(path.relative_to(root)),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    }
                ),
                verdict=result.grade.verdict,
                termination="budget_exhausted",
            )
            if case == "development":
                ids.append(trial)
    for name in (
        ".env",
        "registry.json",
        "earlier-private.json",
        ".git/history",
        "tests/protected.py",
    ):
        path = root / name
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text("PRIVATE_CANARY")
    yield store, root, run, snapshot, ids
    await store.close()


async def test_role_view_excludes_hidden_cases_and_reference(evidence_setup, tmp_path):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder

    store, root, run, snapshot, ids = evidence_setup
    builder = DevelopmentViewBuilder(store, root)
    output = tmp_path / "view"
    view = await builder.build_development_view(run.id, snapshot.snapshot_id, output)
    assert {ref.trial_id for ref in view.evidence_refs} == set(ids)
    contents = b"".join(p.read_bytes() for p in output.rglob("*") if p.is_file())
    for secret in (
        b"HIDDEN_TRACE_CANARY",
        b"REFERENCE_CANARY",
        b"PRIVATE_TEST_CANARY",
        b"PRIVATE_CANARY",
        b"selection-validation",
        b"final-test",
        str(root).encode(),
    ):
        assert secret not in contents
    assert b"budget_exhausted" in contents
    assert b'"pass"' in contents and b'"fail"' in contents
    ref = next(r for r in view.evidence_refs if r.artifact == "trace")
    path = builder.resolve_evidence(view, ref)
    path.write_text("tampered")
    with pytest.raises(ValueError, match="digest"):
        builder.resolve_evidence(view, ref)


async def test_altered_original_artifact_refused(evidence_setup, tmp_path):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder

    store, root, run, snapshot, ids = evidence_setup
    (root / "trials" / str(ids[0]) / "trace.json").write_text("changed")
    with pytest.raises(ValueError, match="digest"):
        await DevelopmentViewBuilder(store, root).build_development_view(
            run.id, snapshot.snapshot_id, tmp_path / "view"
        )


async def test_unregistered_evidence_refused(evidence_setup, tmp_path):
    from uuid import uuid4

    from crucible.improvement.evidence_views import DevelopmentViewBuilder

    store, root, run, snapshot, ids = evidence_setup
    builder = DevelopmentViewBuilder(store, root)
    view = await builder.build_development_view(
        run.id, snapshot.snapshot_id, tmp_path / "view"
    )
    with pytest.raises(ValueError, match="outside"):
        builder.resolve_evidence(
            view, EvidenceRef(trial_id=uuid4(), artifact="trace", sha256="a" * 64)
        )


async def test_missing_development_repeat_stops_view(evidence_setup, tmp_path):
    from crucible.improvement.evidence_views import DevelopmentViewBuilder

    store, root, run, snapshot, ids = evidence_setup
    await store.create_trial(run.id, snapshot.snapshot_id, "development", 0, "new-key")
    with pytest.raises(ValueError, match="ambiguous|complete"):
        await DevelopmentViewBuilder(store, root).build_development_view(
            run.id, snapshot.snapshot_id, tmp_path / "view"
        )


def test_protocol_version_invalidates_trial_reuse():
    from crucible.improvement.fingerprints import trial_key
    from tests.unit.test_improvement_manifest import make_manifest
    from tests.unit.test_improvement_search_store import baseline

    old = make_manifest()
    new = old.model_copy(update={"schema_version": 2, "worker_protocol_version": 2})
    snapshot = baseline(old)
    assert trial_key(old, snapshot, old.cases[0], 0) != trial_key(
        new, snapshot, new.cases[0], 0
    )


def test_prior_feedback_excludes_hidden_case_counts_and_ids():
    from crucible.improvement.roles import CaseCount, role_feedback
    from tests.unit.test_improvement_search_store import (
        baseline,
        reject,
        search_manifest,
    )

    snapshot = baseline(search_manifest())
    decision = reject(snapshot).model_copy(
        update={
            "reason": "case_regression",
            "counts": (
                CaseCount(
                    case_id="development", split="development", baseline=0, candidate=1
                ),
                CaseCount(
                    case_id="HIDDEN_ID_CANARY",
                    split="selection-validation",
                    baseline=3,
                    candidate=2,
                ),
            ),
        }
    )
    feedback = role_feedback(decision)
    assert feedback.hidden_gate_rejected
    assert "HIDDEN_ID_CANARY" not in feedback.model_dump_json()
    assert feedback.development[0].baseline == 0
    assert feedback.development[0].candidate == 1


@pytest.mark.parametrize(
    "evidence_setup",
    [
        {
            "schema_version": 2,
            "messages": [{"id": "message-1", "text": "display summary"}],
            "tool_calls": [
                {
                    "id": "call-1",
                    "assistant_message_id": "message-1",
                    "name": "read_file",
                    "arguments": {"path": "example.py"},
                }
            ],
            "tool_results": [
                {
                    "tool_call_id": "call-1",
                    "result": {
                        "ok": True,
                        "secret": "tool-secret-canary",
                        "text": "x" * 1_100_000,
                    },
                }
            ],
        }
    ],
    indirect=True,
)
async def test_structured_trace_retrieval_is_sanitized_and_bounded(
    evidence_setup, tmp_path, monkeypatch
):
    from crucible.improvement.evidence_views import MAX_ARTIFACT, DevelopmentViewBuilder

    monkeypatch.setenv("TEST_API_KEY", "tool-secret-canary")
    store, root, run, snapshot, _ = evidence_setup
    builder = DevelopmentViewBuilder(store, root)
    view = await builder.build_development_view(
        run.id, snapshot.snapshot_id, tmp_path / "view"
    )
    ref = next(ref for ref in view.evidence_refs if ref.artifact == "trace")
    raw = builder.resolve_evidence(view, ref).read_bytes()
    assert len(raw) <= MAX_ARTIFACT
    assert b'"name": "read_file"' in raw
    assert b'"arguments": {"path": "example.py"}' in raw
    assert b'"tool_call_id": "call-1"' in raw
    assert b'"ok": true' in raw
    assert b"tool-secret-canary" not in raw
    assert b"[TRUNCATED:" in raw
