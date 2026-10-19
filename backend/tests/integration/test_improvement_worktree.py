from uuid import uuid4

from crucible.improvement.worktree import ExperimentWorktree
from tests.unit.test_improvement_source import git

pytest_plugins = ["tests.unit.test_improvement_source"]


async def test_main_checkout_unchanged(harness, tmp_path):
    root, commit = harness
    branch = git(root, "symbolic-ref", "HEAD")
    manager = ExperimentWorktree(tmp_path / "state")
    owned = await manager.create(root, uuid4(), commit)
    assert git(root, "symbolic-ref", "HEAD") == branch
    assert git(owned.path, "rev-parse", "HEAD") == commit
    assert (await owned.inspect()).source_digest


async def test_user_edits_on_resume_refused(harness, tmp_path):
    import pytest

    from crucible.improvement.contracts import EDITABLE_PATHS
    from crucible.improvement.source import ScopeRejected

    root, commit = harness
    owned = await ExperimentWorktree(tmp_path / "state").create(root, uuid4(), commit)
    baseline = (await owned.inspect()).source_digest
    (owned.path / EDITABLE_PATHS[0]).write_text("user edit")
    with pytest.raises(ScopeRejected, match="unexpected user edits"):
        await owned.restore_last_accepted(baseline)
    assert (owned.path / EDITABLE_PATHS[0]).read_text() == "user edit"


async def test_owned_interrupted_patch_archived_before_restore(harness, tmp_path):
    from crucible.improvement.contracts import EDITABLE_PATHS

    root, commit = harness
    owned = await ExperimentWorktree(tmp_path / "state").create(root, uuid4(), commit)
    baseline = (await owned.inspect()).source_digest
    (owned.path / EDITABLE_PATHS[0]).write_text("owned change")
    await owned.checkpoint_owned_edits()
    archive = await owned.archive_interrupted(uuid4())
    assert (archive / "candidate.diff").exists()
    await owned.restore_last_accepted(baseline)
    assert (await owned.inspect()).source_digest == baseline
