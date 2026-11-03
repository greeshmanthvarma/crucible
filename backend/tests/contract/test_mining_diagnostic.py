"""Pure diagnostic contracts; no provider or container execution."""

import json
from uuid import UUID

import pytest

from crucible.improvement.contracts import MiningDiagnosticManifest
from tests.unit.test_improvement_manifest import manifest_payload


def diagnostic_payload():
    base = manifest_payload()
    fields = {
        "cases",
        "role_limits",
        "repeat_count",
        "max_rounds",
        "editable_paths",
        "boundary_suite_digest",
        "scoring_version",
    }
    base = {k: v for k, v in base.items() if k not in fields}
    analyst = dict(
        max_steps=10,
        max_tool_calls=20,
        max_model_tokens=50000,
        max_active_seconds=300,
        wall_timeout_seconds=600,
    )
    base.update(
        kind="mining-diagnostic",
        schema_version=2,
        worker_protocol_version=2,
        authorization="explicit-mining-diagnostic",
        package_digest="d" * 64,
        investigation_source=dict(
            commit="b" * 40, source_digest="e" * 64, export_digest="f" * 64
        ),
        selection_digest="a" * 64,
        selected_trial_ids=[str(UUID(int=i)) for i in (1, 2, 3)],
        analyst_limits=analyst,
        trial_limits=analyst,
        synthesis_limits=dict(
            max_steps=15,
            max_tool_calls=30,
            max_model_tokens=100000,
            max_active_seconds=600,
            wall_timeout_seconds=900,
        ),
        analyst_max_requests=10,
        synthesis_max_requests=15,
        report_bytes=16384,
        max_format_corrections_per_slot=1,
        prompt_digest="a" * 64,
        report_schema_digest="b" * 64,
        export_config_digest="c" * 64,
        budget=dict(
            max_reported_and_reserved_tokens=250000,
            max_model_requests=45,
            max_trial_starts=0,
            max_active_seconds=1500,
            max_worker_wall_seconds=2700,
            max_total_output_bytes=16777216,
        ),
    )
    base["command_policy"]["max_output_bytes_per_call"] = 1048576
    return base


@pytest.fixture
def diagnostic_manifest():
    return MiningDiagnosticManifest.model_validate_json(
        json.dumps(diagnostic_payload())
    )


def test_diagnostic_requires_exact_frozen_allocations(diagnostic_manifest):
    assert diagnostic_manifest.budget.max_trial_starts == 0
    assert diagnostic_manifest.selected_trial_ids == tuple(
        UUID(int=i) for i in (1, 2, 3)
    )


def role_payload(manifest, kind="trial_analysis"):
    return dict(
        schema_version=2,
        protocol_version=2,
        job_id=str(UUID(int=10)),
        role_id=str(UUID(int=11)),
        round_id=str(UUID(int=12)),
        experiment_id=str(manifest.experiment_id),
        job_kind=kind,
        harness_digest=manifest.initial_source_digest,
        export_digest="b" * 64,
        model=manifest.model.model_dump(mode="json"),
        limits=manifest.analyst_limits.model_dump(mode="json"),
        input_digest="c" * 64,
        workspace="/workspace",
        command_image=manifest.worker_image,
        command_platform=manifest.worker_platform,
        output_limit_bytes=1048576,
        wall_deadline_seconds=600,
        budget_slot_id=str(UUID(int=13)),
    )


@pytest.mark.parametrize(
    "kind", ["trial_analysis", "mining_synthesis", "format_correction"]
)
def test_diagnostic_role_protocol(diagnostic_manifest, kind):
    from crucible.improvement.worker_protocol import (
        RoleJob,
        read_worker_job,
        validate_job_manifest,
    )

    payload = role_payload(diagnostic_manifest, kind)
    if kind == "mining_synthesis":
        payload["limits"] = diagnostic_manifest.synthesis_limits.model_dump(mode="json")
        payload["wall_deadline_seconds"] = 900
    if kind == "format_correction":
        payload.update(
            correction_of_job_id=str(UUID(int=14)), correction_target="trial_analysis"
        )
    job = RoleJob.model_validate_json(json.dumps(payload))
    assert read_worker_job(job.model_dump_json().encode()) == job
    validate_job_manifest(job, diagnostic_manifest)


def test_diagnostic_forbids_trial_and_proposal_jobs(diagnostic_manifest):
    from crucible.improvement.worker_protocol import RoleJob, validate_job_manifest
    from tests.contract.test_improvement_worker import make_job

    with pytest.raises(ValueError, match="diagnostic"):
        validate_job_manifest(make_job(), diagnostic_manifest)
    payload = role_payload(diagnostic_manifest, "proposal")
    payload.pop("budget_slot_id")
    with pytest.raises(ValueError, match="diagnostic"):
        validate_job_manifest(
            RoleJob.model_validate_json(json.dumps(payload)), diagnostic_manifest
        )


def test_foreign_assignment_and_duplicate_trial_ids_rejected(diagnostic_manifest):
    from crucible.improvement.mining_contracts import AnalystAssignment

    assignment = AnalystAssignment(
        slot_id=UUID(int=13),
        trial_id=UUID(int=99),
        package_digest="d" * 64,
        investigation_source_digest="e" * 64,
    )
    with pytest.raises(ValueError, match="assignment"):
        assignment.validate_manifest(diagnostic_manifest)
    payload = diagnostic_payload()
    payload["selected_trial_ids"][1] = payload["selected_trial_ids"][0]
    with pytest.raises(ValueError, match="distinct"):
        MiningDiagnosticManifest.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize(
    "field,value",
    [
        ("budget_slot_id", None),
        ("correction_target", "trial_analysis"),
        ("output_limit_bytes", 1048577),
        ("wall_deadline_seconds", 601),
    ],
)
def test_diagnostic_rejects_malformed_primary(diagnostic_manifest, field, value):
    from crucible.improvement.worker_protocol import RoleJob, validate_job_manifest

    payload = role_payload(diagnostic_manifest)
    payload[field] = value
    with pytest.raises(ValueError):
        validate_job_manifest(
            RoleJob.model_validate_json(json.dumps(payload)), diagnostic_manifest
        )


def test_report_contracts_preserve_no_action_and_precise_support():
    from crucible.improvement.mining_contracts import (
        SynthesisReport,
        TrialAnalysisReport,
    )

    analysis = dict(
        schema_version=1,
        trial_id=str(UUID(int=1)),
        package_digest="d" * 64,
        investigation_source_digest="e" * 64,
        investigation_complete=False,
        summary="Evidence is incomplete.",
        observations=[],
        open_questions=[],
        limitations=[],
    )
    assert not TrialAnalysisReport.model_validate_json(
        json.dumps(analysis)
    ).investigation_complete
    result = dict(
        schema_version=1,
        disposition="no_actionable_weakness",
        weaknesses=[],
        summary="No finding.",
    )
    report = dict(
        schema_version=1,
        package_digest="d" * 64,
        result=result,
        support=[],
        limitations=[],
    )
    parsed = SynthesisReport.model_validate_json(json.dumps(report))
    assert parsed.result.model_dump(mode="json") == result
    citation = dict(
        original=dict(trial_id=str(UUID(int=1)), artifact="trace", sha256="a" * 64),
        record_kind="message",
        record_id=str(UUID(int=2)),
        record_key="message:2",
        derived_sha256="b" * 64,
        start_byte=0,
        end_byte=10,
    )
    weakness = dict(
        id="w1",
        observed_behavior="Observed",
        hypothesized_mechanism="Hypothesis",
        evidence=[dict(citation["original"])],
        editable_paths=["backend/src/crucible/tools/filesystem.py"],
        confidence="low",
        risks=[],
        limitations="Unverified",
    )
    report["result"].update(disposition="actionable", weaknesses=[weakness])
    with pytest.raises(ValueError, match="support"):
        SynthesisReport.model_validate_json(json.dumps(report))
    report["support"] = [
        dict(
            weakness_id="w1",
            evidence=[citation],
            source=[
                dict(
                    path="backend/src/crucible/tools/filesystem.py",
                    sha256="c" * 64,
                    start_line=1,
                    end_line=3,
                )
            ],
        )
    ]
    assert (
        SynthesisReport.model_validate_json(json.dumps(report))
        .support[0]
        .evidence[0]
        .original.sha256
        == "a" * 64
    )
    report["support"][0]["evidence"][0]["original"]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="refs"):
        SynthesisReport.model_validate_json(json.dumps(report))


@pytest.mark.parametrize(
    "kind,record_id,start,end",
    [
        ("message", None, 0, 1),
        ("solution_diff", str(UUID(int=2)), 0, 1),
        ("message", str(UUID(int=2)), 1, 1),
        ("message", str(UUID(int=2)), -1, 1),
    ],
)
def test_record_citation_rejects_invalid_identity_or_span(kind, record_id, start, end):
    from crucible.improvement.mining_contracts import RecordCitation

    payload = dict(
        original=dict(trial_id=str(UUID(int=1)), artifact="trace", sha256="a" * 64),
        record_kind=kind,
        record_id=record_id,
        record_key="message:2",
        derived_sha256="b" * 64,
        start_byte=start,
        end_byte=end,
    )
    with pytest.raises(ValueError):
        RecordCitation.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize(
    "path", ["/private/file", "../file", "a/../file", "a\\file", "a//file"]
)
def test_source_citation_rejects_controller_paths(path):
    from crucible.improvement.mining_contracts import SourceCitation

    with pytest.raises(ValueError):
        SourceCitation(path=path, sha256="a" * 64, start_line=1, end_line=1)


@pytest.mark.parametrize(
    "section,field",
    [
        (section, field)
        for section, fields in [
            (
                "analyst_limits",
                (
                    "max_steps",
                    "max_tool_calls",
                    "max_model_tokens",
                    "max_active_seconds",
                    "wall_timeout_seconds",
                ),
            ),
            (
                "synthesis_limits",
                (
                    "max_steps",
                    "max_tool_calls",
                    "max_model_tokens",
                    "max_active_seconds",
                    "wall_timeout_seconds",
                ),
            ),
            (
                "budget",
                (
                    "max_reported_and_reserved_tokens",
                    "max_model_requests",
                    "max_trial_starts",
                    "max_active_seconds",
                    "max_worker_wall_seconds",
                    "max_total_output_bytes",
                ),
            ),
        ]
        for field in fields
    ],
)
def test_each_frozen_cap_rejects_changes(section, field):
    payload = diagnostic_payload()
    payload[section][field] += 1
    with pytest.raises(ValueError):
        MiningDiagnosticManifest.model_validate_json(json.dumps(payload))


def test_diagnostic_job_cannot_enter_measured_experiment(diagnostic_manifest):
    from crucible.improvement.worker_protocol import RoleJob, validate_job_manifest
    from tests.unit.test_improvement_manifest import make_manifest

    job = RoleJob.model_validate_json(json.dumps(role_payload(diagnostic_manifest)))
    with pytest.raises(ValueError, match="diagnostic"):
        validate_job_manifest(job, make_manifest())


def test_diagnostic_engine_requires_immutable_mounted_job(
    diagnostic_manifest, tmp_path
):
    from crucible.application.worker_container import create_worker_engine
    from crucible.improvement.worker_protocol import RoleJob

    job = RoleJob.model_validate_json(json.dumps(role_payload(diagnostic_manifest)))
    with pytest.raises(FileNotFoundError):
        create_worker_engine(None, job, None, "unused", job_path=tmp_path / "absent")
