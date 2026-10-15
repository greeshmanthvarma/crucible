"""Private regression registration is immutable, local, and controller-owned."""

import json
import stat
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crucible.repair.cases import ApprovedCase
from crucible.repair.incident_suite import IncidentSuite
from tests.integration.repair.test_trials import approved_case, environment

COMMIT = "a" * 40


def suite(env, root):
    return IncidentSuite(
        root,
        env["factory"],
        env["artifacts"],
        Clock(),
        fixtures={env["fixture_digest"]: env["fixture"]},
    )


class Clock:
    def now(self):
        from tests.integration.repair.test_trials import NOW

        return NOW


async def test_accepted_incident_survives_restart_with_private_fixture_snapshot(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    root = tmp_path / "data" / "repair" / "incident-suite"
    registered = await suite(env, root).register(case, COMMIT)
    assert registered.case_digest == case.case_digest
    assert registered.accepted_commit == COMMIT
    assert registered.definition_artifact_id is not None
    assert registered.fixture_manifest_artifact_id is not None
    accepted = await suite(env, root).list_accepted()
    assert len(accepted) == 1
    assert accepted[0].case_digest == case.case_digest
    definition, raw = await env["artifacts"].read(registered.definition_artifact_id)
    assert definition.sensitivity == "private"
    restored = ApprovedCase.model_validate_json(raw)
    assert restored.case_digest == case.case_digest
    fixture, raw_fixture = await env["artifacts"].read(
        registered.fixture_manifest_artifact_id
    )
    assert fixture.sensitivity == "private"
    assert "before\\n" in json.dumps(json.loads(raw_fixture))
    # Mutating a caller's old fixture must not rewrite already registered evidence.
    (env["fixture"] / "note.txt").write_text("later mutation")
    _, again = await env["artifacts"].read(registered.fixture_manifest_artifact_id)
    assert again == raw_fixture
    assert (await suite(env, root).list_accepted())[0] == registered
    assert any(path.is_file() for path in root.rglob("*"))
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    for path in root.rglob("*"):
        assert not path.is_symlink()
        assert stat.S_IMODE(path.stat().st_mode) & 0o077 == 0


async def test_registration_is_idempotent_and_does_not_overwrite_acceptance(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    root = tmp_path / "data" / "repair" / "incident-suite"
    first = await suite(env, root).register(case, COMMIT)
    second = await suite(env, root).register(case, COMMIT)
    assert first == second
    with pytest.raises(ValueError):
        await suite(env, root).register(case, "b" * 40)
    assert await suite(env, root).list_accepted() == (first,)


async def test_unapproved_or_tampered_cases_never_enter_private_suite(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    registry = suite(env, tmp_path / "incident-suite")
    # model_construct deliberately bypasses Pydantic; the service must revalidate.
    forged = ApprovedCase.model_construct(
        case_digest=case.case_digest,
        case=case.case.model_copy(update={"expected_behavior": "forged expectation"}),
        review_id=uuid4(),
    )
    with pytest.raises((ValueError, ValidationError, TypeError)):
        await registry.register(forged, COMMIT)
    with pytest.raises((ValueError, ValidationError, TypeError)):
        await registry.register(case.case, COMMIT)
    assert await registry.list_accepted() == ()


@pytest.mark.parametrize("defect", ["symlink", "missing", "changed"])
async def test_registration_rejects_missing_unsafe_or_changed_fixture(
    database, tmp_path, defect
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    note = env["fixture"] / "note.txt"
    note.unlink()
    if defect == "symlink":
        outside = tmp_path / "private-host-file"
        outside.write_text("not a fixture")
        note.symlink_to(outside)
    elif defect == "changed":
        note.write_text("different input")
    registry = suite(env, tmp_path / "incident-suite")
    with pytest.raises((ValueError, OSError)):
        await registry.register(case, COMMIT)
    assert await registry.list_accepted() == ()


async def test_private_suite_is_discovered_only_in_development_partition(
    database, tmp_path
):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    registry = suite(env, tmp_path / "data" / "repair" / "incident-suite")
    await registry.register(case, COMMIT)
    development = await registry.discover(partition="development")
    held_out = await registry.discover(partition="held-out")
    assert [item.case_digest for item in development] == [case.case_digest]
    assert held_out == ()
    assert not (env["source_root"] / "evals").exists()
    assert not (env["source_root"] / "data").exists()


async def test_suite_rejects_tampered_definition_artifact_on_read(database, tmp_path):
    env = await environment(database, tmp_path)
    case = approved_case(env)
    root = tmp_path / "incident-suite"
    registered = await suite(env, root).register(case, COMMIT)
    metadata, _ = await env["artifacts"].read(registered.definition_artifact_id)
    blob = tmp_path / "artifacts" / metadata.storage_identity
    assert blob.is_file()
    blob.write_text('{"case_digest":"forged"}')
    with pytest.raises((ValueError, ValidationError)):
        await suite(env, root).list_accepted()


async def test_suite_does_not_follow_a_symlinked_private_root(database, tmp_path):
    env = await environment(database, tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "incident-suite"
    root.symlink_to(outside, target_is_directory=True)
    with pytest.raises((ValueError, OSError)):
        await suite(env, root).register(approved_case(env), COMMIT)
    assert list(outside.iterdir()) == []
