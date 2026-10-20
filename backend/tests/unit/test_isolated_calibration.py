"""Calibration must not masquerade as an admitted measured experiment."""

import json
from uuid import uuid4

import pytest

from crucible.improvement.contracts import CalibrationManifest, ExperimentManifest
from tests.unit.test_improvement_manifest import manifest_payload


def calibration_payload():
    experiment = manifest_payload()
    fields = (
        "experiment_id",
        "initial_commit",
        "initial_source_digest",
        "controller_digest",
        "dependency_lock_digest",
        "worker_image",
        "worker_platform",
        "relay_image",
        "grader",
        "model",
        "trial_limits",
        "command_policy",
        "budget",
        "network_policy",
    )
    return {
        **{key: experiment[key] for key in fields},
        "schema_version": 1,
        "worker_protocol_version": 2,
        "kind": "isolated-calibration",
        "authorization": "one-isolated-calibration",
        "approval_mode": "interactive",
        "case_manifest": "/private/case.json",
        "admission_record": "/private/admission.json",
        "case_digest": "a" * 64,
        "admission_digest": "b" * 64,
        "trial_id": str(uuid4()),
    }


def test_calibration_not_experiment():
    raw = json.dumps(calibration_payload())
    assert CalibrationManifest.model_validate_json(raw).kind == "isolated-calibration"
    with pytest.raises(ValueError):
        ExperimentManifest.model_validate_json(raw)


def test_calibration_requires_interactive_commands():
    payload = calibration_payload()
    payload["command_policy"]["mode"] = "preauthorized-disposable"
    with pytest.raises(ValueError, match="interactive"):
        CalibrationManifest.model_validate_json(json.dumps(payload))


async def test_execution_manifest_roundtrip_and_quotas(tmp_path):
    from crucible.improvement.contracts import UsageAmounts
    from crucible.improvement.store import CalibrationStore, QuotaExhausted

    manifest = CalibrationManifest.model_validate_json(
        json.dumps(calibration_payload())
    )
    store = await CalibrationStore.open(tmp_path / "calibration.sqlite3")
    try:
        record = await store.create(manifest, "calibration-only", "one-shot")
        assert (await store.get(record.id)).manifest == manifest
        await store.reserve_usage(
            record.id, None, UsageAmounts(tokens=100), kind="model"
        )
        with pytest.raises(QuotaExhausted):
            await store.reserve_usage(
                record.id, None, UsageAmounts(tokens=1), kind="model"
            )
        with pytest.raises(ValueError, match="experiment"):
            await store.begin_round(record.id, uuid4(), record.revision)
    finally:
        await store.close()
