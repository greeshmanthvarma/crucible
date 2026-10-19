"""Trusted assertions never execute in the candidate Python process."""

import pytest

from crucible.improvement.checks import validate_probe


def test_candidate_cannot_replace_protected_suite():
    with pytest.raises(ValueError):
        validate_probe("guidance", {"passed": True, "collected": ["forged"]})


def test_invalid_guidance_rejected_by_external_assertion():
    with pytest.raises(ValueError):
        validate_probe("guidance", {"coding": "é" * 4097, "step": ""})


async def test_infrastructure_failure_is_error_not_rejection(tmp_path):
    import json
    from uuid import uuid4

    from crucible.improvement.checks import CHECK_IDS, check_candidate, suite_digest
    from crucible.improvement.contracts import HarnessSnapshot
    from crucible.improvement.source import _digest
    from tests.unit.test_improvement_manifest import make_manifest

    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "suite.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                test_ids=CHECK_IDS,
                timeout_seconds=10,
                output_bytes=1024,
            )
        )
    )
    manifest = make_manifest().model_copy(
        update={"boundary_suite_digest": suite_digest(suite)}
    )
    harness = tmp_path / "harness"
    harness.mkdir()
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit=manifest.initial_commit,
        source_digest="f" * 64,
        export_digest=_digest({}),
        patch_digest="b" * 64,
        kind="candidate",
    )

    class BrokenProbe:
        async def probe(self, *args):
            raise ValueError("Docker failed")

    result = await check_candidate(
        snapshot,
        manifest,
        suite,
        tmp_path / "output",
        harness_export=harness,
        probes=BrokenProbe(),
    )
    assert result.verdict == "error"
    assert not result.cleanup_confirmed
