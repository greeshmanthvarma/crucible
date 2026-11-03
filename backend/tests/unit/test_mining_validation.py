"""Report acceptance requires original frozen bytes and successful read receipts."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID

import pytest

from crucible.improvement.mining_validation import (
    load_inspection_package,
    validate_synthesis_report,
)
from crucible.improvement.source import _digest
from crucible.improvement.trajectory_contracts import (
    ExportLimits,
    TrialSelection,
    VerifiedDevelopmentInputs,
)
from crucible.improvement.trajectory_export import export_trajectory_package
from tests.unit import test_trajectory as fixtures

parallel_records = fixtures.parallel_records
parallel_trace = fixtures.parallel_trace
verified_trial = fixtures.verified_trial


def exported_package(trial, root, limits=None):
    source_files = {
        "backend/src/crucible/tools/filesystem.py": (0o100644, b"# first\n# second\n")
    }
    source = trial.source.model_copy(update={"export_digest": _digest(source_files)})
    trial = replace(trial, source=source)
    inputs = VerifiedDevelopmentInputs(
        (trial,),
        source,
        source_files,
        (trial.trial_id,),
        UUID(int=800),
        UUID(int=801),
        TrialSelection(mode="failed-family-v1", trial_ids=(trial.trial_id,)),
    )
    export_trajectory_package(inputs, root, limits or ExportLimits())
    return load_inspection_package(root, job_id=UUID(int=900))


@pytest.fixture
def inspection_package(verified_trial, tmp_path):
    root = tmp_path / "package"
    package = exported_package(verified_trial, root)
    locator = next(
        r
        for r in json.loads((root / package.record_locator_ref).read_bytes())
        if r["kind"] == "message"
    )
    citation = dict(
        original=locator["original_artifact"],
        record_kind="message",
        record_id=locator["original_record_id"],
        record_key=locator["detail_ref"],
        derived_sha256=locator["detail_sha256"],
        start_byte=0,
        end_byte=4,
    )
    source_citation = dict(
        path="backend/src/crucible/tools/filesystem.py",
        sha256=package.files["source/backend/src/crucible/tools/filesystem.py"],
        start_line=1,
        end_line=2,
    )
    weakness = dict(
        id="w1",
        observed_behavior="Recorded behavior",
        hypothesized_mechanism="Hypothesis",
        evidence=[citation["original"]],
        editable_paths=[source_citation["path"]],
        confidence="low",
        risks=[],
        limitations="Not causal proof",
    )
    raw = dict(
        schema_version=1,
        package_digest=package.bundle_digest,
        result=dict(
            schema_version=1,
            disposition="actionable",
            weaknesses=[weakness],
            summary="A hypothesis",
        ),
        support=[dict(weakness_id="w1", evidence=[citation], source=[source_citation])],
        limitations=[],
    )
    return root, package, raw


async def test_synthesis_requires_original_record_inspection(inspection_package):
    root, package, report = inspection_package
    with pytest.raises(ValueError, match="uninspected_support"):
        validate_synthesis_report(json.dumps(report).encode(), package, ())

    receipts = await inspect_support(root, report)
    validated = validate_synthesis_report(
        json.dumps(report).encode(), package, receipts
    )
    assert (
        validated.support[0].evidence[0].original.sha256
        == report["support"][0]["evidence"][0]["original"]["sha256"]
    )
    assert validated.result.model_dump(mode="json") == report["result"]


async def inspect_support(root, report, evidence_path=None):
    from uuid import uuid4

    from crucible.improvement.mining_inspection import audited_role_registry
    from crucible.tools.definitions import ToolContext

    receipts = []
    registry = audited_role_registry("mining_synthesis", receipts, UUID(int=900))
    citation = report["support"][0]["evidence"][0]
    for offset in range(citation["start_byte"], citation["end_byte"], 8192):
        await registry.get("read_file_chunk").invoke(
            ToolContext(root, tool_call_id=uuid4()),
            dict(
                path=evidence_path or citation["record_key"],
                offset=offset,
                limit=min(8192, max(4, citation["end_byte"] - offset)),
            ),
        )
    source = report["support"][0]["source"][0]
    await registry.get("read_file").invoke(
        ToolContext(root, tool_call_id=uuid4()),
        dict(
            path="source/" + source["path"],
            start_line=source["start_line"],
            end_line=source["end_line"],
        ),
    )
    return tuple(receipts)


@pytest.mark.parametrize(
    "fault", ["trial", "original_hash", "derived_hash", "record_id", "record_key"]
)
def test_wrong_trial_or_hash_citation_rejected(inspection_package, fault):
    _, package, report = inspection_package
    citation = report["support"][0]["evidence"][0]
    if fault == "trial":
        citation["original"]["trial_id"] = str(UUID(int=999))
    elif fault == "original_hash":
        citation["original"]["sha256"] = "f" * 64
    elif fault == "derived_hash":
        citation["derived_sha256"] = "f" * 64
    elif fault == "record_id":
        citation["record_id"] = str(UUID(int=999))
    else:
        citation["record_key"] = "../../private"
    with pytest.raises(ValueError, match="invalid_citation"):
        validate_synthesis_report(json.dumps(report).encode(), package, ())


async def test_source_lines_must_exist(inspection_package):
    root, package, report = inspection_package
    receipts = await inspect_support(root, report)
    report["support"][0]["source"][0]["end_line"] = 99
    with pytest.raises(ValueError, match="invalid_citation"):
        validate_synthesis_report(json.dumps(report).encode(), package, receipts)


def test_no_action_result_needs_no_manufactured_weakness(inspection_package):
    _, package, _ = inspection_package
    report = dict(
        schema_version=1,
        package_digest=package.bundle_digest,
        result=dict(
            schema_version=1,
            disposition="no_actionable_weakness",
            weaknesses=[],
            summary="No supported weakness",
        ),
        support=[],
        limitations=[],
    )
    parsed = validate_synthesis_report(json.dumps(report).encode(), package, ())
    assert parsed.result.weaknesses == parsed.support == ()


async def test_receipts_require_primary_job_and_full_span_coverage(inspection_package):
    root, package, report = inspection_package
    receipts = await inspect_support(root, report)
    with pytest.raises(ValueError, match="identity_mismatch"):
        validate_synthesis_report(
            json.dumps(report).encode(),
            package,
            (receipts[0].model_copy(update={"job_id": UUID(int=901)}), *receipts[1:]),
        )
    report["support"][0]["evidence"][0]["end_byte"] = 8
    with pytest.raises(ValueError, match="uninspected_support"):
        validate_synthesis_report(json.dumps(report).encode(), package, receipts)
    second = receipts[0].model_copy(update={"start_byte": 4, "end_byte": 8})
    assert validate_synthesis_report(
        json.dumps(report).encode(), package, (*receipts, second)
    ).support


def analysis_payload(package):
    return dict(
        schema_version=1,
        trial_id=str(package.selected_trial_ids[0]),
        package_digest=package.bundle_digest,
        investigation_source_digest=package.source.source_digest,
        investigation_complete=True,
        summary="No observation",
        observations=[],
        open_questions=[],
        limitations=[],
    )


def test_analyst_report_size_and_observation_limit(inspection_package):
    from crucible.improvement.mining_contracts import AnalystAssignment
    from crucible.improvement.mining_validation import validate_analysis_report

    _, package, synthesis = inspection_package
    assignment = AnalystAssignment(
        slot_id=UUID(int=902),
        trial_id=package.selected_trial_ids[0],
        package_digest=package.bundle_digest,
        investigation_source_digest=package.source.source_digest,
    )
    report = analysis_payload(package)
    raw = json.dumps(report).encode()
    assert validate_analysis_report(
        raw + b" " * (16384 - len(raw)), assignment, package, ()
    ).investigation_complete
    with pytest.raises(ValueError, match="report_limit"):
        validate_analysis_report(
            raw + b" " * (16385 - len(raw)), assignment, package, ()
        )
    observation = dict(
        id="o",
        observed_behavior="Behavior",
        evidence=synthesis["support"][0]["evidence"],
        hypothesized_mechanism=None,
        alternatives=[],
        contradictory_evidence=[],
        source=[],
        limitations="",
    )
    report["observations"] = [observation] * 6
    with pytest.raises(ValueError, match="invalid_report"):
        validate_analysis_report(json.dumps(report).encode(), assignment, package, ())
    report.update(observations=[], investigation_complete=False)
    assert not validate_analysis_report(
        json.dumps(report).encode(), assignment, package, ()
    ).investigation_complete


async def test_truncated_unavailable_bytes_cannot_support_claim(
    inspection_package, verified_trial, tmp_path
):
    from crucible.repair.identity import canonical_json

    root, _, report = inspection_package
    trace = json.loads(verified_trial.trace_bytes)
    trace["messages"][0]["parts"][0]["text_content"] = "large " * 20000
    raw = canonical_json(trace)
    trial = replace(
        verified_trial,
        trace_bytes=raw,
        trace_ref=verified_trial.trace_ref.model_copy(
            update={"sha256": hashlib.sha256(raw).hexdigest()}
        ),
    )
    root = tmp_path / "truncated"
    package = exported_package(trial, root, ExportLimits(detail_bytes=16384))
    locator = next(
        r
        for r in json.loads((root / package.record_locator_ref).read_bytes())
        if r["kind"] == "message" and r["truncated"]
    )
    envelope = json.loads((root / locator["detail_ref"]).read_bytes())
    prefix = "trials/" + str(trial.trial_id) + "/"
    path = prefix + envelope["excerpt_ref"]
    excerpt = (root / path).read_bytes()
    citation = dict(
        original=locator["original_artifact"],
        record_kind="message",
        record_id=locator["original_record_id"],
        record_key=locator["detail_ref"],
        derived_sha256=package.files[path],
        start_byte=0,
        end_byte=len(excerpt) + 1,
    )
    report["package_digest"] = package.bundle_digest
    report["support"][0]["evidence"] = [citation]
    report["result"]["weaknesses"][0]["evidence"] = [citation["original"]]
    with pytest.raises(ValueError, match="invalid_citation"):
        validate_synthesis_report(json.dumps(report).encode(), package, ())
    citation["end_byte"] = len(excerpt)
    receipts = await inspect_support(root, report, evidence_path=path)
    assert validate_synthesis_report(
        json.dumps(report).encode(), package, receipts
    ).support


def test_synthesis_serialized_envelope_limit(inspection_package):
    _, package, _ = inspection_package
    report = dict(
        schema_version=1,
        package_digest=package.bundle_digest,
        result=dict(
            schema_version=1,
            disposition="insufficient_evidence",
            weaknesses=[],
            summary="No finding",
        ),
        support=[],
        limitations=[],
    )
    raw = json.dumps(report).encode()
    assert (
        validate_synthesis_report(
            raw + b" " * (1048576 - len(raw)), package, ()
        ).result.disposition
        == "insufficient_evidence"
    )
    with pytest.raises(ValueError, match="report_limit"):
        validate_synthesis_report(raw + b" " * (1048577 - len(raw)), package, ())


async def test_analyst_private_alias_receipts_are_scoped_to_assignment(
    inspection_package, tmp_path
):
    import shutil

    from crucible.improvement.mining_contracts import AnalystAssignment
    from crucible.improvement.mining_inspection import audited_role_registry
    from crucible.improvement.mining_validation import validate_analysis_report
    from crucible.tools.definitions import ToolContext

    root, package, synthesis = inspection_package
    trial = package.selected_trial_ids[0]
    view = tmp_path / "analyst"
    shutil.copytree(root / "trials" / str(trial), view / "trial")
    shutil.copytree(root / "source", view / "source")
    citation = synthesis["support"][0]["evidence"][0]
    receipts = []
    tool = audited_role_registry("trial_analysis", receipts, UUID(int=900)).get(
        "read_file_chunk"
    )
    alias = "trial/" + citation["record_key"].removeprefix(f"trials/{trial}/")
    await tool.invoke(
        ToolContext(view, tool_call_id=UUID(int=903)), dict(path=alias, limit=4)
    )
    report = analysis_payload(package)
    report["observations"] = [
        dict(
            id="o",
            observed_behavior="Observed",
            evidence=[citation],
            hypothesized_mechanism=None,
            alternatives=[],
            contradictory_evidence=[],
            source=[],
            limitations="Not causal proof",
        )
    ]
    assignment = AnalystAssignment(
        slot_id=UUID(int=902),
        trial_id=trial,
        package_digest=package.bundle_digest,
        investigation_source_digest=package.source.source_digest,
    )
    assert validate_analysis_report(
        json.dumps(report).encode(), assignment, package, tuple(receipts)
    ).observations
    invalid = receipts[0].model_copy(update={"start_byte": -1})
    with pytest.raises(ValueError, match="invalid_citation"):
        validate_analysis_report(
            json.dumps(report).encode(), assignment, package, (invalid,)
        )
    report["trial_id"] = str(UUID(int=999))
    with pytest.raises(ValueError, match="identity_mismatch"):
        validate_analysis_report(
            json.dumps(report).encode(), assignment, package, tuple(receipts)
        )
