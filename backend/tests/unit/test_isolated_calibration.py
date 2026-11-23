"""Calibration must not masquerade as an admitted measured experiment."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from crucible.domain.clock import SystemClock
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.improvement import calibration
from crucible.improvement.broker import ImprovementBroker, WorkspaceBinding
from crucible.improvement.contracts import (
    CalibrationManifest,
    ExperimentManifest,
    HarnessSnapshot,
)
from crucible.improvement.store import CalibrationStore
from crucible.repair.gateway import ScopedModelGateway
from crucible.sandbox.protocol import SandboxOutcome, SandboxTermination
from tests.contract.test_improvement_worker import make_job
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


def test_calibration_requires_matching_command_modes():
    payload = calibration_payload()
    payload["command_policy"]["mode"] = "preauthorized-disposable"
    with pytest.raises(ValueError, match="mode"):
        CalibrationManifest.model_validate_json(json.dumps(payload))


def disposable_payload():
    payload = calibration_payload()
    payload["approval_mode"] = "preauthorized-disposable"
    payload["command_policy"]["mode"] = "preauthorized-disposable"
    return payload


def test_disposable_calibration_requires_explicit_authorization():
    with pytest.raises(ValueError, match="require operator authorization"):
        CalibrationManifest.model_validate_json(json.dumps(disposable_payload()))


@pytest.mark.parametrize(
    ("mode", "blocked"),
    [
        ("preauthorized-disposable", None),
        ("preauthorized-disposable", "network"),
        ("preauthorized-disposable", "image"),
        ("preauthorized-disposable", "environment"),
        ("preauthorized-disposable", "limits"),
        ("interactive", None),
        ("interactive", "operator"),
    ],
)
async def test_calibration_command_approval_at_broker(
    tmp_path, monkeypatch, mode, blocked
):
    payload = calibration_payload()
    if mode == "preauthorized-disposable":
        payload = disposable_payload()
        payload["disposable_commands_authorized"] = True
    manifest = CalibrationManifest.model_validate_json(json.dumps(payload))
    job = make_job().model_copy(
        update={
            "schema_version": 2,
            "protocol_version": 2,
            "experiment_id": manifest.experiment_id,
            "trial_id": manifest.trial_id,
        }
    )
    prompts = []
    pauses = []
    commands = []

    async def operator_line():
        prompts.append(True)
        return "deny" if blocked == "operator" else "approve"

    async def pause(job_id, paused):
        pauses.append(paused)

    class Executor:
        async def execute(self, job, binding, command_id, spec):
            commands.append(spec)
            now = datetime.now(UTC)
            return (), SandboxOutcome(
                0,
                SandboxTermination.COMPLETED,
                now,
                now,
                binding.image,
                str(command_id),
                0,
                0,
                False,
            )

    monkeypatch.setattr(calibration, "_operator_line", operator_line)
    store = await CalibrationStore.open(tmp_path / "calibration.sqlite3")
    try:
        await store.create(manifest, "calibration-only", str(manifest.trial_id))
        snapshot = HarnessSnapshot(
            snapshot_id=uuid4(),
            parent_commit=manifest.initial_commit,
            source_digest=job.harness_digest,
            export_digest=job.export_digest,
            patch_digest=None,
            kind="baseline",
        )
        await store.register_snapshot(manifest.experiment_id, snapshot, "snapshot.json")
        await store.admit_trial(
            manifest.experiment_id, snapshot.snapshot_id, "development"
        )
        gateway = ScopedModelGateway(
            None,
            clock=SystemClock(),
            endpoint="http://broker/internal/improvement/model",
        )
        broker = ImprovementBroker(
            store, gateway, Executor(), calibration.calibration_approval(manifest)
        )
        broker.pause_worker = pause
        binding = WorkspaceBinding(
            tmp_path.resolve(), job.solver_case.image, job.solver_case.platform
        )
        credential = broker.issue(job, manifest, binding)
        spec = CommandSpec(
            "true",
            (),
            ".",
            5,
            CommandNetwork.NONE,
            {},
            binding.image,
            "check",
            CommandLimits(1, 1024, 64, 100),
        )
        changes = {
            "network": {"network": CommandNetwork.OUTBOUND},
            "image": {"image": "other@sha256:" + "f" * 64},
            "environment": {"environment": {"EXAMPLE": "override"}},
            "limits": {"limits": replace(spec.limits, cpus=2)},
        }
        spec = replace(spec, **changes.get(blocked, {}))
        command_id = uuid4()
        result = await broker.command(credential.token, job.job_id, command_id, spec)
        approved = blocked is None
        decision = json.loads(result.splitlines()[-1])
        assert decision["type"] == ("outcome" if approved else "denied")
        assert commands == ([spec] if approved else [])
        events = await store.events(manifest.experiment_id, "command_approval")
        if approved or blocked == "operator":
            assert events == (
                {
                    "job_id": str(job.job_id),
                    "command_id": str(command_id),
                    "spec_digest": spec.digest,
                    "approved": approved,
                },
            )
        else:
            assert events == ()
        assert prompts == ([True] if mode == "interactive" else [])
        assert pauses == ([True, False] if mode == "interactive" else [])
    finally:
        await store.close()


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
