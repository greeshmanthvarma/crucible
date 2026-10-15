"""Equivalence identity covers controlled inputs, independently of provenance."""

import pytest

from crucible.repair.fingerprints import EvaluationIdentity, SourceIdentity, fingerprint


@pytest.fixture
def identity():
    return EvaluationIdentity(
        source=SourceIdentity(commit="abc", tree_digest="tree"),
        model="deterministic-test-model",
        parameters={"temperature": 0, "seed": 7},
        fixture_digest="fixture",
        eval_digest="eval",
        instruction_digest="instructions",
        tool_schema_digest="tools",
        prompt_digest="prompt",
        lock_digest="lock",
        image_digest="sha256:image",
        verifier_version="1",
        estimator_version="characters-divided-by-four-v1",
        adapter_version="1",
        resource_policy={"cpus": 1, "network": "none"},
        budgets={"tokens": 100, "seconds": 60},
        repeats=3,
    )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("source", SourceIdentity(commit="def", tree_digest="tree")),
        ("source", SourceIdentity(commit="abc", tree_digest="new-tree")),
        ("model", "other"),
        ("parameters", {"temperature": 1}),
        ("fixture_digest", "other"),
        ("eval_digest", "other"),
        ("instruction_digest", "other"),
        ("tool_schema_digest", "other"),
        ("prompt_digest", "other"),
        ("lock_digest", "other"),
        ("image_digest", "sha256:other"),
        ("verifier_version", "2"),
        ("estimator_version", "2"),
        ("adapter_version", "2"),
        ("resource_policy", {"network": "gateway"}),
        ("budgets", {"tokens": 101, "seconds": 60}),
        ("repeats", 1),
    ],
)
def test_fingerprint_invalidates_changed_controlled_inputs(
    identity, field, replacement
):
    old = fingerprint(identity)
    assert old != fingerprint(identity.model_copy(update={field: replacement}))
    assert old == fingerprint(identity.model_copy())


def test_fingerprint_uses_canonical_versioned_json(identity):
    reordered = identity.model_copy(
        update={"parameters": {"seed": 7, "temperature": 0}}
    )
    assert fingerprint(identity) == fingerprint(reordered)
    assert len(fingerprint(identity)) == 64
    assert identity.model_dump(mode="json")["schema_version"] == 1
    assert not {"task_id", "run_id", "job_id"} & type(identity).model_fields.keys()


async def test_source_snapshot_includes_additions_and_deletions(tmp_path):
    import subprocess

    from crucible.repair.fingerprints import capture_source_identity

    def git(*args):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    source = tmp_path / "source.py"
    source.write_text("original")
    (tmp_path / ".gitignore").write_text("private/\n.env\n")
    git("add", "source.py", ".gitignore")
    git("commit", "-qm", "fixture")
    original = await capture_source_identity(tmp_path)
    new = tmp_path / "new.py"
    new.write_text("new")
    assert (await capture_source_identity(tmp_path)).tree_digest != original.tree_digest
    new.unlink()
    assert await capture_source_identity(tmp_path) == original
    source.unlink()
    assert (await capture_source_identity(tmp_path)).tree_digest != original.tree_digest
    source.write_text("original")
    (tmp_path / ".env").write_text("synthetic private value")
    (tmp_path / "private").mkdir()
    (tmp_path / "private" / "case.json").write_text("private fixture")
    assert await capture_source_identity(tmp_path) == original
