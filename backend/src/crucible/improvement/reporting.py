"""Deterministic positive-list report export. No raw worker or grader contents."""

import hashlib
import json
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated
from urllib.parse import urlparse
from uuid import UUID

from pydantic import Field

from crucible.benchmarks.contracts import BenchmarkCase, Sha256
from crucible.benchmarks.manifests import case_digest
from crucible.improvement.contracts import Contract, ExperimentPaths
from crucible.improvement.final import FinalEvaluator
from crucible.improvement.manifest import CaseRegistry, manifest_digest
from crucible.improvement.recovery_guidance import recovery_actions
from crucible.improvement.source import atomic_source_file, safe_path
from crucible.improvement.store import ExperimentStore, RevisionConflict
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.gateway import SAFE_MODEL_ERRORS
from crucible.repair.identity import canonical_json, digest


class ReportBundle(Contract):
    experiment_id: UUID
    seal_digest: Sha256
    report_digest: Sha256
    files: Annotated[dict[str, Sha256], Field(min_length=2, max_length=2)]
    complete: bool


PUBLIC_EVENTS = frozenset(
    {
        "created",
        "trial_created",
        "trial_starting",
        "trial_solving",
        "trial_snapshotting",
        "trial_grading",
        "trial_complete",
        "trial_error",
        "trial_interrupted",
        "trial_reused",
        "trial_superseded",
        "role_saved",
        "round_started",
        "round_interrupted",
        "decision_recorded",
        "promotion_intent",
        "promotion_complete",
        "explicit_resume",
        "stop_requested",
        "execution_stopped",
        "effect_intent",
        "effect_active",
        "effect_stopped",
        "effect_uncertain",
        "command_approval",
        "command_approval_requested",
        "command_approval_wait",
        "search_outcome",
        "search_sealed",
        "final_measurement",
        "final_same_source",
        "interrupted_candidate_invalidated",
        "candidate_checks",
        "usage_reserved",
        "usage_settled",
        "usage_uncertain",
        "usage_partial",
        "usage_acknowledged",
        "recovery_completed",
        "trial_retry_authorized",
        "trial_retry_consumed",
        "request_dispatched",
        "request_terminal",
        "job_outcome",
    }
)


def _public_model(value: str) -> str:
    if not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9/_.:@+-]{0,127}", value
    ) or value.startswith(("sk-", "ghp_", "github_pat_", "AKIA")):
        return "redacted; see frozen model configuration digest"
    return value


def verify_bundle(bundle: ReportBundle, directory: Path) -> None:
    if set(bundle.files) != {
        "report.md",
        "metrics.json",
    } or bundle.report_digest != digest(
        {
            "files": bundle.files,
            "seal_digest": bundle.seal_digest,
            "complete": bundle.complete,
        }
    ):
        raise RevisionConflict("report manifest drift")
    for name, sha in bundle.files.items():
        if (
            hashlib.sha256(
                read_regular_artifact(directory / name, 32 * 1024 * 1024)
            ).hexdigest()
            != sha
        ):
            raise RevisionConflict("report file drift")
    metrics = json.loads(
        read_regular_artifact(directory / "metrics.json", 32 * 1024 * 1024)
    )
    if (
        metrics["experiment_id"] != str(bundle.experiment_id)
        or metrics["seal_digest"] != bundle.seal_digest
        or metrics["complete"] != bundle.complete
    ):
        raise RevisionConflict("report identity differs")


class ReportExporter:
    def __init__(self, store: ExperimentStore, paths: ExperimentPaths) -> None:
        self.store, self.paths = store, paths

    async def _provenance(self, id: UUID) -> list[dict[str, object]]:
        manifest = (await self.store.get(id)).manifest
        registry_file = self.paths.registry_root / "registry.json"
        registry = (
            CaseRegistry.model_validate_json(
                read_regular_artifact(registry_file, 1_048_576)
            )
            if registry_file.exists()
            else None
        )
        if registry and (
            registry.pending_slots
            or set(registry.cases) != {c.case_id for c in manifest.cases}
        ):
            raise RevisionConflict("report registry differs from frozen cases")
        rows: list[dict[str, object]] = []
        for ref in manifest.cases:
            row: dict[str, object] = {
                "case_id": ref.case_id,
                "split": ref.split,
                "case_digest": ref.case_digest,
                "admission_digest": ref.admission_digest,
                "repository": None,
                "base_commit": None,
                "task_image": None,
                "task_platform": None,
                "source_url": None,
                "license": None,
                "license_basis": "not recorded in verified admission",
                "metadata_verification": "unavailable: registry not present",
            }
            if registry:
                entry = registry.cases[ref.case_id]
                case = BenchmarkCase.model_validate_json(
                    read_regular_artifact(entry.case_manifest, 1_048_576)
                )
                admission_raw = read_regular_artifact(entry.admission_record, 1_048_576)
                if (
                    entry.retired
                    or case_digest(case) != ref.case_digest
                    or hashlib.sha256(admission_raw).hexdigest() != ref.admission_digest
                ):
                    raise RevisionConflict("report admitted provenance drift")
                admission = json.loads(admission_raw)
                repo = (
                    case.repo
                    if re.fullmatch(
                        r"[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}", case.repo
                    )
                    else None
                )
                row.update(
                    repository=repo,
                    base_commit=case.base_commit,
                    task_image=case.image,
                    task_platform=case.platform,
                    source_url=f"https://github.com/{repo}/tree/{case.base_commit}"
                    if repo
                    else None,
                    metadata_verification="frozen case and admission digests verified",
                )
                metadata = admission.get("provenance", {})
                if isinstance(metadata, dict):
                    license = metadata.get("license")
                    if isinstance(license, str) and re.fullmatch(
                        r"[A-Za-z0-9][A-Za-z0-9.+-]{0,99}"
                        r"(?: (?:AND|OR) [A-Za-z0-9][A-Za-z0-9.+-]{0,99}){0,5}",
                        license,
                    ):
                        row.update(
                            license=license,
                            license_basis=(
                                "verified admission provenance; no legal inference"
                            ),
                        )
                    source = metadata.get("source_url")
                    if isinstance(source, str) and len(source) <= 1000:
                        url = urlparse(source)
                        if (
                            url.scheme == "https"
                            and url.hostname in {"github.com", "huggingface.co"}
                            and not (
                                url.username
                                or url.password
                                or url.query
                                or url.fragment
                            )
                        ):
                            row["source_url"] = source
            rows.append(row)
        return rows

    async def export_report(self, id: UUID, destination: Path) -> ReportBundle:
        run = await self.store.get(id)
        seal = await self.store.search_seal(id)
        if seal is None:
            raise RevisionConflict("report requires sealed search")
        measurement = await FinalEvaluator(self.store, self.paths, None).measurement(id)
        events = await self.store.audit_events(id)
        public_events = []
        for event in events:
            if event["type"] not in PUBLIC_EVENTS:
                continue
            item = {k: event[k] for k in ("sequence", "type", "created_at")}
            payload = json.loads(str(event["payload_json"]))
            item["payload_digest"] = digest(payload)
            if event["type"] == "command_approval":
                item["approved"] = payload.get("approved") is True
            public_events.append(item)
        summary = await self.store.usage_summary(id)
        actual = summary["confirmed"]
        conservative = summary["conservative"]
        reserved = summary["retained"]
        assert (
            isinstance(actual, dict)
            and isinstance(conservative, dict)
            and isinstance(reserved, dict)
        )
        unsettled = sum(
            r["status"] not in ("settled", "conservative")
            for r in await self.store.usage_records(id)
        )
        cleanup = all(e["status"] == "stopped" for e in await self.store.effects(id))
        required_snapshots = {
            str(seal.initial_snapshot_id),
            str(seal.selected_final_snapshot_id),
        }
        failed = {
            str(r["id"])
            for r in await self.store.selected_trials(id)
            if r["snapshot"] in required_snapshots
            and r["status"] in ("error", "interrupted")
        }
        authorized = {
            str(e["trial_id"])
            for e in await self.store.events(id, "trial_retry_authorized")
        }
        consumed = {
            str(e["trial_id"])
            for e in await self.store.events(id, "trial_retry_consumed")
        }
        remaining = summary["remaining"]
        assert isinstance(remaining, dict)
        actions = recovery_actions(
            run.status,
            cleanup_confirmed=cleanup,
            unsettled_usage=bool(unsettled),
            remaining=remaining,
            retry_required=bool(failed - (authorized - consumed)),
            work_complete=measurement.complete,
        )
        action = actions[0] if actions else "none"
        retry_links = [
            {
                "original_trial_id": str(UUID(str(e["original_trial_id"]))),
                "replacement_trial_id": str(UUID(str(e["replacement_trial_id"]))),
            }
            for e in await self.store.events(id, "trial_superseded")
        ]
        job_facts = []
        for e in await self.store.events(id, "job_outcome"):
            try:
                job_id = str(UUID(str(e.get("job_id"))))
            except ValueError:
                continue
            category = e.get("failure_category")
            job_facts.append(
                {
                    "job_id": job_id,
                    "termination": e.get("termination")
                    if e.get("termination")
                    in ("completed", "budget_exhausted", "failed", "interrupted")
                    else "failed",
                    "cleanup_confirmed": e.get("cleanup_confirmed") is True,
                    "failure_category": category
                    if category is None
                    or (
                        isinstance(category, str)
                        and category
                        in SAFE_MODEL_ERRORS
                        | {"execution", "cancellation", "cleanup", "unknown"}
                    )
                    else "unknown",
                }
            )
        trials = await self.store.trials(id)
        rounds = await self.store.rounds(id)
        role_facts = []
        for round in rounds:
            for role in await self.store.roles(round.id):
                execution = role.execution
                role_facts.append(
                    {
                        "role_id": str(execution.role_id),
                        "round_id": str(round.id),
                        "kind": execution.kind,
                        "attempt": role.attempt,
                        "status": execution.status,
                        "source_digest": execution.source_digest,
                        "input_digest": execution.input_digest,
                        "output_digest": execution.output_digest,
                        "error_digest": digest(execution.error_code)
                        if execution.error_code
                        else None,
                    }
                )
        rates = {}
        for split in measurement.splits:

            def gain(
                before: int | None, after: int | None, denominator: int
            ) -> float | None:
                return (
                    100 * (after - before) / denominator
                    if before is not None and after is not None and denominator
                    else None
                )

            rates[split.split] = {
                "trial_denominator_per_version": split.expected_trials_per_version,
                "task_denominator_per_version": split.expected_tasks,
                "trial_gain_percentage_points": gain(
                    split.initial_passed_trials,
                    split.final_passed_trials,
                    split.expected_trials_per_version,
                ),
                "task_gain_percentage_points": gain(
                    split.initial_successful_tasks,
                    split.final_successful_tasks,
                    split.expected_tasks,
                ),
            }
        wall = None
        if public_events:
            wall = (
                datetime.fromisoformat(str(public_events[-1]["created_at"]))
                - datetime.fromisoformat(str(public_events[0]["created_at"]))
            ).total_seconds()
        complete = measurement.complete and not unsettled
        config = {
            "manifest_digest": manifest_digest(run.manifest),
            "model": _public_model(run.manifest.model.model_id),
            "model_digest": digest(run.manifest.model),
            "trial_limits": run.manifest.trial_limits.model_dump(mode="json"),
            "role_limits": {
                k: v.model_dump(mode="json")
                for k, v in run.manifest.role_limits.items()
            },
            "budget": run.manifest.budget.model_dump(mode="json"),
            "repeat_count": 3,
            "max_rounds": 3,
            "controller_digest": run.manifest.controller_digest,
            "dependency_lock_digest": run.manifest.dependency_lock_digest,
            "worker_image_digest": run.manifest.worker_image.split("@")[1],
            "worker_platform": run.manifest.worker_platform,
            "relay_image_digest": run.manifest.relay_image.split("@")[1],
            "grader_digest": run.manifest.grader.driver_digest,
            "grader_commit": run.manifest.grader.driver_commit,
            "grader_limits": {
                key: getattr(run.manifest.grader, key)
                for key in (
                    "timeout_seconds",
                    "memory_bytes",
                    "cpus",
                    "output_limit_bytes",
                    "network",
                )
            },
            "boundary_suite_digest": run.manifest.boundary_suite_digest,
            "scoring_version": run.manifest.scoring_version,
            "network_policy": run.manifest.network_policy,
            "command_policy": run.manifest.command_policy.model_dump(mode="json"),
            "disposable_commands_authorized": (
                run.manifest.disposable_commands_authorized
            ),
        }
        data = {
            "schema_version": 1,
            "experiment_id": str(id),
            "seal_digest": digest(asdict(seal)),
            "complete": complete,
            "seal": json.loads(canonical_json(asdict(seal))),
            "configuration": config,
            "measurement": measurement.model_dump(mode="json"),
            "rates": rates,
            "provenance": await self._provenance(id),
            "snapshots": [
                {
                    key: row[key]
                    for key in (
                        "id",
                        "parent_commit",
                        "kind",
                        "source_digest",
                        "export_digest",
                        "patch_digest",
                        "status",
                    )
                }
                for row in await self.store.snapshots(id)
            ],
            "accepted_lineage": [
                json.loads(canonical_json(asdict(p)))
                for p in await self.store.promotions(id)
            ],
            "rounds": [
                {
                    "round_id": str(r.id),
                    "ordinal": r.ordinal,
                    "status": r.status,
                    "baseline_commit": r.baseline_commit,
                    "decision_digest": r.decision_digest,
                }
                for r in rounds
            ],
            "decisions": [
                d.model_dump(mode="json") for d in await self.store.decisions(id)
            ],
            "roles": role_facts,
            "recovery": {
                "cleanup_confirmed": cleanup,
                "required_operator_action": action,
                "required_operator_actions": actions,
            },
            "trial_retry_links": retry_links,
            "job_outcomes": job_facts,
            "trial_attempts": [
                {
                    "trial_id": t["id"],
                    "snapshot_id": t["snapshot"],
                    "case_id": t["case_id"],
                    "repeat_index": t["repeat_index"],
                    "generation": t["generation"],
                    "status": t["status"],
                    "grade_verdict": t["grade_verdict"],
                    "error_digest": digest(t["error_code"])
                    if t["error_code"]
                    else None,
                }
                for t in trials
            ],
            "audit_events": public_events,
            "execution_counts": {
                "actual_trial_generations": len(trials),
                "role_executions": len(role_facts),
                "result_cache_reuse_links": sum(
                    e["type"] == "trial_reused" for e in events
                ),
                "same_source_final_links": sum(
                    e["type"] == "final_same_source" for e in events
                ),
                "explicit_resumes": sum(e["type"] == "explicit_resume" for e in events),
                "human_approval_decisions": sum(
                    e["type"] == "command_approval" for e in events
                ),
            },
            "usage": {
                "known_actual": actual,
                "conservative_charges": conservative,
                "unsettled_reserved": reserved,
                "unsettled_reservations": unsettled,
                "complete": not unsettled,
                "dollars": None,
                "price_basis": None,
                "provider_cache_tokens": None,
            },
            "time": {
                "wall_elapsed_seconds": wall,
                "setup_seconds": None,
                "active_seconds": actual["active_seconds"],
                "basis": (
                    "recorded experiment events and settled resource ledger; "
                    "setup not separately measured"
                ),
            },
            "limitations": [
                "Scripted tests and seeded probes are excluded from benchmark "
                "measurements.",
                "Three stochastic repeats do not establish causal, statistical or"
                " generalization claims.",
                "Public benchmark tasks may be contaminated by training or prior "
                "exposure.",
                "Publishing final task identities retires their unseen status for"
                " later experiments.",
                "Calibration and admission evidence are not scored as improvement"
                " trials.",
                "A real isolated worker proof and complete admitted corpus are "
                "separate run prerequisites.",
                "Licenses and provider cache pricing are unavailable unless "
                "independently recorded; no values are inferred.",
            ],
        }
        lines = [
            f"# Crucible improvement report {id}",
            "",
            "**COMPLETE MEASUREMENT**"
            if complete
            else "**INCONCLUSIVE — measurement or usage evidence is incomplete.**",
            "",
            f"Evaluated code commit: `{seal.selected_final_commit}`.",
            f"Search stop reason: `{seal.stop_reason}`. "
            f"Seal: `{digest(asdict(seal))}`.",
            "",
            "Each case has three trials per version. A task succeeds at least "
            "twice out of three; trial and task denominators are reported "
            "separately.",
            "",
        ]
        for case in measurement.cases:
            before = (
                "unavailable"
                if case.initial_passes is None
                else str(case.initial_passes)
            )
            after = (
                "unavailable" if case.final_passes is None else str(case.final_passes)
            )
            lines.append(
                f"- {case.split}: `{case.case_id}` — "
                f"initial {before}/3; final {after}/3."
            )
        lines += [
            "",
            "Every round, decision, trial attempt, role status, resume and "
            "recorded approval is retained in metrics.json. Raw prompts, traces, "
            "grader logs, reference code and private paths are excluded.",
            "",
            f"Actual trial generations: {len(trials)}. "
            f"Reported tokens: {actual['tokens']}. "
            f"Conservative charges: {conservative}. "
            f"Unsettled reservations: {unsettled}. "
            "Dollars and provider cached-token totals: unavailable.",
            "",
            *data["limitations"],
        ]
        files = {
            "metrics.json": canonical_json(data) + b"\n",
            "report.md": ("\n".join(lines) + "\n").encode(),
        }
        hashes = {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()}
        seal_sha = digest(asdict(seal))
        bundle = ReportBundle(
            experiment_id=id,
            seal_digest=seal_sha,
            report_digest=digest(
                {"files": hashes, "seal_digest": seal_sha, "complete": complete}
            ),
            files=hashes,
            complete=complete,
        )
        safe_path(destination)
        if destination.resolve().is_relative_to(
            self.paths.root.resolve()
        ) or self.paths.root.resolve().is_relative_to(destination.resolve()):
            raise ValueError("report destination overlaps private state")
        destination.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name, raw in files.items():
            path = destination / name
            safe_path(path)
            if path.exists() and read_regular_artifact(path, 32 * 1024 * 1024) != raw:
                raise RevisionConflict(
                    "existing report differs; choose a new export destination"
                )
            if not path.exists():
                with TemporaryDirectory(
                    prefix="report-write-", dir=destination
                ) as staging:
                    atomic_source_file(path, (0o100644, raw), Path(staging))
        verify_bundle(bundle, destination)
        return bundle
