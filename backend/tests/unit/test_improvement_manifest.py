from uuid import uuid4

import pytest

from crucible.improvement.contracts import EDITABLE_PATHS, ExperimentManifest
from crucible.improvement.manifest import manifest_digest


def manifest_payload():
    return dict(
        schema_version=1,
        experiment_id=str(uuid4()),
        initial_commit="a" * 40,
        initial_source_digest="a" * 64,
        controller_digest="b" * 64,
        dependency_lock_digest="c" * 64,
        worker_image="worker@sha256:" + "d" * 64,
        worker_platform="linux/amd64",
        relay_image="relay@sha256:" + "e" * 64,
        cases=[
            dict(case_id=s, split=s, case_digest="f" * 64, admission_digest="a" * 64)
            for s in ["development", "selection-validation", "final-test"]
        ],
        grader=dict(
            driver_python="/usr/bin/python3",
            driver_source="/protected/evaluator",
            driver_commit="a" * 40,
            driver_digest="b" * 64,
            timeout_seconds=60,
            memory_bytes=1024,
            cpus=1,
            output_limit_bytes=1024,
            network="none",
        ),
        model=dict(
            model_id="provider/model",
            input_limit=1000,
            output_reserve=100,
            provider_defaults="unspecified",
        ),
        trial_limits=dict(
            max_steps=5,
            max_tool_calls=10,
            max_model_tokens=100,
            max_active_seconds=60,
            wall_timeout_seconds=90,
        ),
        role_limits={
            r: dict(
                max_steps=5,
                max_tool_calls=10,
                max_model_tokens=100,
                max_active_seconds=60,
                wall_timeout_seconds=90,
            )
            for r in ["mining", "proposal"]
        },
        repeat_count=3,
        max_rounds=3,
        editable_paths=list(EDITABLE_PATHS),
        boundary_suite_digest="b" * 64,
        command_policy=dict(
            mode="interactive",
            max_calls_per_trial=10,
            max_seconds_per_call=30,
            max_output_bytes_per_call=1024,
            allow_network=False,
        ),
        budget=dict(
            max_reported_and_reserved_tokens=100,
            max_model_requests=10,
            max_trial_starts=100,
            max_active_seconds=100,
            max_worker_wall_seconds=100,
            max_total_output_bytes=1024,
        ),
        network_policy="broker-only",
        scoring_version="per-case-count-v1",
    )


def make_manifest():
    import json

    return ExperimentManifest.model_validate_json(json.dumps(manifest_payload()))


def test_missing_case_blocks_freeze():
    payload = manifest_payload()
    payload["cases"].pop()
    import json

    with pytest.raises(ValueError, match="split|at least 3"):
        ExperimentManifest.model_validate_json(json.dumps(payload))


def test_fingerprint_changes_with_model():
    original = make_manifest()
    changed = original.model_copy(
        update={
            "model": original.model.model_copy(update={"model_id": "provider/other"})
        }
    )
    assert manifest_digest(original) != manifest_digest(changed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("worker_image", "worker:latest"),
        ("repeat_count", 4),
        ("schema_version", 2),
        ("editable_paths", []),
        ("disposable_commands_authorized", True),
    ],
)
def test_frozen_policy_rejects_invalid_inputs(field, value):
    import json

    payload = manifest_payload()
    payload[field] = value
    if field == "disposable_commands_authorized":
        payload["command_policy"]["mode"] = "preauthorized-disposable"
        payload[field] = False
    with pytest.raises(ValueError):
        ExperimentManifest.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("change", ["image", "evaluator", "model"])
def test_semantic_identity_includes_runtime_and_evaluator(change):
    original = make_manifest()
    updates = {
        "image": dict(worker_image="other@sha256:" + "c" * 64),
        "evaluator": dict(
            grader=original.grader.model_copy(update={"driver_digest": "f" * 64})
        ),
        "model": dict(model=original.model.model_copy(update={"output_reserve": 200})),
    }
    assert manifest_digest(original) != manifest_digest(
        original.model_copy(update=updates[change])
    )


def test_missing_registry_case_and_pending_slots_rejected(tmp_path):
    import json

    from crucible.improvement.manifest import ManifestDrift, freeze_manifest

    config = tmp_path / "config.json"
    config.write_text(make_manifest().model_dump_json())
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps(dict(schema_version=1, pending_slots=1, cases={})))
    with pytest.raises(ManifestDrift, match="pending"):
        freeze_manifest(config, tmp_path)
    registry.write_text(json.dumps(dict(schema_version=1, pending_slots=0, cases={})))
    with pytest.raises(ManifestDrift, match="missing"):
        freeze_manifest(config, tmp_path)


def test_schedule_excludes_final_and_retains_upper_bound():
    manifest = make_manifest()
    assert manifest.baseline_trial_count == 6
    assert manifest.planned_trial_upper_bound == 30


def test_retired_case_rejected_before_admission(tmp_path):
    import json

    from crucible.improvement.manifest import ManifestDrift, freeze_manifest

    manifest = make_manifest()
    config = tmp_path / "config.json"
    config.write_text(manifest.model_dump_json())
    registry = dict(
        schema_version=1,
        pending_slots=0,
        cases={
            c.case_id: dict(
                case_manifest=str(tmp_path / "case.json"),
                admission_record=str(tmp_path / "admission.json"),
                retired=True,
            )
            for c in manifest.cases
        },
    )
    (tmp_path / "registry.json").write_text(json.dumps(registry))
    with pytest.raises(ManifestDrift, match="retired"):
        freeze_manifest(config, tmp_path)


def test_calibration_split_is_never_measured():
    import json

    payload = manifest_payload()
    payload["cases"][0]["split"] = "calibration"
    with pytest.raises(ValueError):
        ExperimentManifest.model_validate_json(json.dumps(payload))
