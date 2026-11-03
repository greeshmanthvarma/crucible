"""Deterministic metrics; raw evidence and precise support stay private."""

import json
from pathlib import Path
from typing import Literal, TypedDict
from urllib.parse import unquote, urlparse
from uuid import UUID

from pydantic import TypeAdapter

from crucible.improvement.contracts import UsageAmounts
from crucible.improvement.mining_contracts import (
    MiningDiagnosticStatus,
    SynthesisReport,
    TrialAnalysisReport,
)
from crucible.improvement.mining_diagnostic import (
    MiningDiagnosticRunner,
    _json,
    _sha,
    _write,
)
from crucible.improvement.mining_inspection import read_inspection_receipts
from crucible.improvement.mining_store import MiningDiagnosticStore
from crucible.improvement.source import safe_path
from crucible.improvement.trajectory_contracts import TrialOverview
from crucible.improvement.trajectory_export import verify_trajectory_package
from crucible.improvement.trials import owned_artifact
from crucible.improvement.worker_protocol import (
    JobOutcome,
    RoleJob,
    read_role_observation,
)
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import canonical_json


class ReportedModel(TypedDict):
    requests: int
    input_tokens: int
    output_tokens: int
    basis: Literal["reported"]


def _root(store: MiningDiagnosticStore) -> Path:
    path = Path(unquote(urlparse(store.engine.url.database or "").path))
    safe_path(path)
    if path.name != "accounting.sqlite3":
        raise ValueError("private diagnostic ledger required")
    return path.parent


async def status(store: MiningDiagnosticStore, id: UUID) -> MiningDiagnosticStatus:
    return await store.diagnostic_status(id)


def _checks(state: str, code: str | None, normal: bool | None) -> dict[str, object]:
    if state in ("complete", "incomplete"):
        schema, citations = "valid", "valid"
    elif state == "invalid" and normal:
        schema = "invalid" if code in ("invalid_report", "report_limit") else "valid"
        citations = "not_checked" if schema == "invalid" else "invalid"
    else:
        schema = citations = "not_checked"
    return {
        "schema": schema,
        "citations": citations,
        "semantic_review": "not_performed",
    }


async def _attempt(
    store: MiningDiagnosticStore, root: Path, attempt_id: UUID
) -> dict[str, object]:
    row = await store.attempt_record(attempt_id)
    job = RoleJob.model_validate_json(row["job_json"])
    validation = json.loads(row["validation_json"] or "{}")
    code = validation.get("error_code")
    # Codes are controller-generated, but the report still uses a fixed vocabulary.
    public_codes = {
        "invalid_report",
        "identity_mismatch",
        "invalid_citation",
        "uninspected_support",
        "report_limit",
        "incomplete_analysis",
        "execution_validation_failed",
        "artifact_quota_denied",
    }
    public_code = code if code in public_codes else "other" if code else None
    directory = root / "attempts" / str(job.job_id)
    outcome = None
    hashes = validation.get("artifact_hashes") or {}
    if row["outcome_ref"] is not None:
        raw = read_regular_artifact(owned_artifact(root, row["outcome_ref"]), 1_048_576)
        if hashes.get("outcome.json") != _sha(raw):
            raise ValueError("outcome digest drift")
        outcome = TypeAdapter(JobOutcome).validate_json(raw, strict=True)
        if (
            outcome.job_id != job.job_id
            or outcome.experiment_id != job.experiment_id
            or outcome.attempt_id != job.role_id
            or outcome.harness_digest != job.harness_digest
        ):
            raise ValueError("outcome identity drift")
    reservations = []
    model_requests = 0
    model_usage_unknown = False
    for usage in sorted(
        await store.attempt_usage(attempt_id), key=lambda row: row["reservation_id"]
    ):
        requested = UsageAmounts.model_validate_json(usage["requested_json"])
        observed = (
            UsageAmounts.model_validate_json(usage["settled_json"])
            if usage["settled_json"]
            else None
        )
        reservations.append(
            {
                "reservation_id": usage["reservation_id"],
                "kind": usage["kind"],
                "status": usage["status"],
                "requested": requested.model_dump(),
                "ledger_amounts": observed.model_dump()
                if observed is not None
                else None,
                "ledger_basis": "observed"
                if usage["status"] == "settled"
                else "conservative_charge"
                if usage["status"] == "conservative"
                else "retained_or_in_flight_exposure",
                "observed": observed.model_dump()
                if observed is not None and usage["status"] == "settled"
                else None,
            }
        )
        if usage["kind"] == "model":
            if usage["status"] != "settled":
                model_usage_unknown = True
            model_requests += (observed or requested).requests
    reported = None
    if (
        outcome is not None
        and outcome.model_usage.basis == "reported"
        and not model_usage_unknown
        and outcome.model_usage.requests == model_requests
    ):
        reported = {
            "requests": outcome.model_usage.requests,
            "input_tokens": outcome.model_usage.input_tokens,
            "output_tokens": outcome.model_usage.output_tokens,
            "basis": "reported",
        }
    elif not model_requests and not model_usage_unknown and outcome is not None:
        # Confirmed non-dispatch has known zero usage, unlike a missing outcome.
        if (
            outcome.stage == "admission"
            and outcome.failure
            and outcome.failure.dispatch == "not_sent"
        ):
            reported = {
                "requests": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "basis": "reported",
            }
    inspection = None
    if hashes.get("inspection-receipts.json") and hashes.get("observation.json"):
        observation_raw = read_regular_artifact(
            directory / "observation.json", 1_048_576
        )
        receipt_raw = read_regular_artifact(
            directory / "inspection-receipts.json", 1_048_576
        )
        if (
            _sha(observation_raw) != hashes["observation.json"]
            or _sha(receipt_raw) != hashes["inspection-receipts.json"]
        ):
            raise ValueError("inspection artifact drift")
        observation = read_role_observation(observation_raw, job)
        assert observation.inspection_receipts_sha256 is not None
        receipts = read_inspection_receipts(
            receipt_raw,
            job_id=job.job_id,
            expected_sha256=observation.inspection_receipts_sha256,
            kind=job.job_kind,
        )
        inspection = {
            "receipt_count": len(receipts),
            "distinct_inspected_files": len({r.path for r in receipts}),
            "returned_read_bytes": sum(r.end_byte - r.start_byte for r in receipts),
        }
    counts: dict[str, int] | None = None
    if row["status"] == "complete" and row["output_ref"]:
        raw = read_regular_artifact(owned_artifact(root, row["output_ref"]), 1_048_576)
        if _sha(raw) != row["output_sha"]:
            raise ValueError("accepted output digest drift")
        target = (
            job.correction_target
            if job.job_kind == "format_correction"
            else job.job_kind
        )
        if target == "trial_analysis":
            report = TrialAnalysisReport.model_validate_json(raw)
            counts = {
                "observations": len(report.observations),
                "evidence_citations": sum(
                    len(o.evidence) + len(o.contradictory_evidence)
                    for o in report.observations
                ),
                "source_citations": sum(len(o.source) for o in report.observations),
            }
        else:
            synthesis = SynthesisReport.model_validate_json(raw)
            counts = {
                "weaknesses": len(synthesis.result.weaknesses),
                "evidence_citations": sum(len(s.evidence) for s in synthesis.support),
                "source_citations": sum(len(s.source) for s in synthesis.support),
            }
    return {
        "attempt_id": str(attempt_id),
        "job_id": str(job.job_id),
        "kind": row["kind"],
        "status": row["status"],
        "error_code": public_code,
        "termination": outcome.termination if outcome else None,
        "normal_completion": validation.get("normal_completion"),
        "cleanup_confirmed": validation.get("cleanup_confirmed"),
        "validation": _checks(row["status"], code, validation.get("normal_completion")),
        "reported_model": reported,
        "tool_calls": row["tool_call_count"],
        "active_seconds": outcome.resource_usage.active_seconds
        if outcome and outcome.accounting_basis == "host_measured"
        else None,
        "worker_wall_seconds": outcome.resource_usage.worker_wall_seconds
        if outcome and outcome.accounting_basis == "host_measured"
        else None,
        "reservations": reservations,
        "inspection": inspection,
        "citation_counts": counts,
        "reuses_primary_inspection": row["kind"] == "format_correction",
    }


async def write_mining_report(
    store: MiningDiagnosticStore, id: UUID, output: Path
) -> dict[str, str]:
    safe_path(output)
    root = _root(store)
    if output.exists() or output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("new nonoverlapping report output required")
    state = await MiningDiagnosticRunner(store, root, None).validate(id)
    manifest = (await store.get(id)).manifest
    package_root = root / "frozen/package"
    package = verify_trajectory_package(package_root)
    evidence_trials = []
    for trial_id in state.selected_trial_ids:
        overview = TrialOverview.model_validate_json(
            read_regular_artifact(
                package_root / package.trial_overview_refs[trial_id], 8192
            )
        )
        completeness = overview.completeness
        evidence_trials.append(
            {
                "trial_id": str(trial_id),
                "case_id": overview.case_id,
                "repeat_index": overview.repeat_index,
                "verdict": overview.verdict,
                "termination": overview.termination,
                "messages": overview.message_count,
                "tool_calls": overview.call_count,
                "tool_results": overview.result_count,
                "missing_results": len(completeness.missing_result_call_ids),
                "unresolved_part_refs": len(completeness.unresolved_part_refs),
                "truncated_details": len(completeness.truncated_detail_refs),
                "unavailable_fields": list(completeness.unavailable_fields),
                "trial_usage_available": overview.usage_available,
                "task_instruction_available": overview.task_instruction_available,
            }
        )
    slots = []
    attempts = []
    for slot in state.slots:
        values = [await _attempt(store, root, a.attempt_id) for a in slot.attempts]
        attempts.extend(values)
        slots.append(
            {**slot.model_dump(mode="json", exclude={"attempts"}), "attempts": values}
        )
    known = [
        TypeAdapter(ReportedModel).validate_python(a["reported_model"])
        for a in attempts
        if a["reported_model"] is not None
    ]
    missing = len(attempts) - len(known)
    reported_totals = {
        name: sum(v[name] for v in known)
        for name in ("requests", "input_tokens", "output_tokens")
    }
    completed = await store.final_checkpoint(id) != (None, None)
    weakness_count = None
    if completed:
        synthesis = SynthesisReport.model_validate_json(
            read_regular_artifact(root / "synthesis-report.json", 1_048_576)
        )
        weakness_count = len(synthesis.result.weaknesses)
    source_bytes = sum(
        (package_root / name).stat().st_size
        for name in package.files
        if name.startswith("source/")
    )
    evidence_bytes = sum(
        (package_root / name).stat().st_size
        for name in package.files
        if not name.startswith("source/")
    )
    evidence_bytes += (package_root / "package-manifest.json").stat().st_size
    metrics = {
        "schema_version": 1,
        "diagnostic": state.model_dump(
            mode="json", exclude={"slots", "final_disposition"}
        ),
        "policy": {
            "budget": manifest.budget.model_dump(),
            "model": manifest.model.model_dump(mode="json"),
            "prompt_digest": manifest.prompt_digest,
            "report_schema_digest": manifest.report_schema_digest,
            "export_config_digest": manifest.export_config_digest,
            "selection_digest": manifest.selection_digest,
            "worker_image": manifest.worker_image,
            "relay_image": manifest.relay_image,
        },
        "selection": {
            "mode": package.selection.mode,
            "selected_repeat_count": len(state.selected_trial_ids),
            "selected_task_count": len({t["case_id"] for t in evidence_trials}),
            "eligible_corpus_task_count": None,
            "denominator": (
                "three saved development repeats of one task; not a full-suite sample"
            ),
        },
        "evidence": {
            "package_digest": package.bundle_digest,
            "evidence_bytes": evidence_bytes,
            "source_bytes": source_bytes,
            "limits": package.limits.model_dump(),
            "trials": evidence_trials,
        },
        "slots": slots,
        "usage": {
            "charged": await store.usage_summary(id),
            "reported_model": {
                **{
                    name: value if not missing else None
                    for name, value in reported_totals.items()
                },
                "known_subtotal": reported_totals,
                "unavailable_attempts": missing,
                "basis": "reported"
                if not missing
                else "partial_reported"
                if known
                else "unavailable",
            },
        },
        "outcome": {
            "mining_completed": completed,
            "disposition": state.final_disposition,
            "weakness_count": weakness_count,
            "measured_improvement": None,
            "semantic_review": "not_performed",
        },
        "comparison": {
            "performed": False,
            "interpretation": "any previous-run comparison would be exploratory",
        },
    }
    output.mkdir(mode=0o700)
    encoded = canonical_json(metrics)
    _write(output / "metrics.json", encoded)
    lines = [
        "# Mining diagnostic",
        "",
        f"Diagnostic: {id}",
        f"Status: {state.status}",
        f"Disposition: {state.final_disposition or 'unavailable'}",
        "",
        "This is a mining-only diagnostic. Completion and weakness count "
        "do not establish measured improvement.",
        "Schema/citation checks establish membership and inspection, "
        "not semantic correctness.",
        "Reported usage, retained/conservative exposure and unknown values "
        "are separate in metrics.json.",
        "No verified price basis is available; no dollar estimate is reported.",
        "",
        "## Attempts",
        "",
    ]
    for slot_metrics in slots:
        lines.append(
            f"Slot {slot_metrics['ordinal']}: {slot_metrics['status']}; "
            f"{len(slot_metrics['attempts'])} attempt(s)."
        )
        for attempt in slot_metrics["attempts"]:
            tools = (
                attempt["tool_calls"]
                if attempt["tool_calls"] is not None
                else "unavailable"
            )
            lines.append(
                f"- {attempt['kind']}: {attempt['status']}; "
                f"termination={attempt['termination'] or 'unavailable'}; "
                f"tool calls={tools}."
            )
    report = ("\n".join(lines) + "\n").encode()
    _write(output / "report.md", report)
    files = {"metrics.json": _sha(encoded), "report.md": _sha(report)}
    _json(output / "report-manifest.json", files)
    return {**files, "report-manifest.json": _sha(canonical_json(files))}
