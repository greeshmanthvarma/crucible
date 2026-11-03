"""Package export/verification at the positive immutable input seam."""

import pytest

from crucible.improvement.trajectory_contracts import TrialSelection
from tests.unit import test_trajectory as fixtures


def test_selection_ids_cannot_repeat():
    from uuid import UUID

    with pytest.raises(ValueError):
        TrialSelection(mode="explicit-three-repeat", trial_ids=(UUID(int=1),) * 3)


parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


def test_credentials_in_structured_arguments_are_redacted(verified_trial, tmp_path):
    import hashlib
    import json
    from dataclasses import replace
    from uuid import UUID

    from crucible.improvement.source import _digest
    from crucible.improvement.trajectory_contracts import (
        ExportLimits,
        VerifiedDevelopmentInputs,
    )
    from crucible.improvement.trajectory_export import export_trajectory_package
    from crucible.repair.identity import canonical_json

    data = json.loads(verified_trial.trace_bytes)
    secrets = {
        "token": "TOKEN_FIELD_CANARY",
        "api-key": "API_FIELD_CANARY",
        "password": "PASSWORD_FIELD_CANARY",
        "authorization": "AUTH_FIELD_CANARY",
    }
    data["tool_calls"][0]["arguments"] = secrets
    raw = canonical_json(data)
    source_files = {
        "backend/src/crucible/tools/filesystem.py": (0o100644, b"# source\n")
    }
    source = verified_trial.source.model_copy(
        update={"export_digest": _digest(source_files)}
    )
    trial = replace(
        verified_trial,
        source=source,
        trace_bytes=raw,
        trace_ref=verified_trial.trace_ref.model_copy(
            update={"sha256": hashlib.sha256(raw).hexdigest()}
        ),
    )
    inputs = VerifiedDevelopmentInputs(
        (trial,),
        source,
        source_files,
        (trial.trial_id,),
        UUID(int=800),
        UUID(int=801),
        TrialSelection(mode="failed-family-v1", trial_ids=(trial.trial_id,)),
    )
    output = tmp_path / "view"
    export_trajectory_package(inputs, output, ExportLimits())
    contents = b"".join(p.read_bytes() for p in output.rglob("*") if p.is_file())
    assert all(secret.encode() not in contents for secret in secrets.values())
