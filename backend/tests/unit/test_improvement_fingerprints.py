from uuid import uuid4

from crucible.improvement.contracts import HarnessSnapshot
from crucible.improvement.fingerprints import trial_key
from tests.unit.test_improvement_manifest import make_manifest


def test_model_limits_image_policy_and_evaluator_invalidate_key():
    manifest = make_manifest()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit="a" * 40,
        source_digest="a" * 64,
        export_digest="b" * 64,
        patch_digest=None,
        kind="baseline",
    )
    original = trial_key(manifest, snapshot, manifest.cases[0], 0)
    changes = [
        dict(model=manifest.model.model_copy(update={"model_id": "another"})),
        dict(trial_limits=manifest.trial_limits.model_copy(update={"max_steps": 10})),
        dict(worker_image="other@sha256:" + "f" * 64),
        dict(grader=manifest.grader.model_copy(update={"driver_digest": "f" * 64})),
        dict(
            command_policy=manifest.command_policy.model_copy(
                update={"max_calls_per_trial": 2}
            )
        ),
    ]
    for update in changes:
        assert (
            trial_key(
                manifest.model_copy(update=update), snapshot, manifest.cases[0], 0
            )
            != original
        )
    assert trial_key(manifest, snapshot, manifest.cases[0], 1) != original
