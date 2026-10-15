"""Public aggregates are constructed separately from private repair provenance."""

import hashlib
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.repair.identity import canonical_json
from crucible.repair.trials import private_artifact


@dataclass(frozen=True)
class RepairReport:
    public_body: str
    private_artifact_id: UUID


def public_report(
    *,
    accepted: int,
    attempted: int,
    roles: dict[str, int],
    elapsed_seconds: int,
    mechanisms: list[str],
    usage: dict[str, int] | None = None,
    observations: int = 0,
) -> str:
    # Only controller-defined categories enter public prose. Model prose, case
    # names, errors, paths and evidence bodies never cross this boundary.
    allowed = {"tool behavior", "context selection", "conversation compaction"}
    summary = ", ".join(sorted(set(mechanisms) & allowed)) or "harness behavior"
    counts = ", ".join(
        f"{role}: {roles.get(role, 0)}" for role in ("diagnosis", "review", "proposal")
    )
    usage = usage or {}
    tokens = "; ".join(
        f"{basis} input/output tokens: {usage.get(basis + '_input', 0)}/"
        f"{usage.get(basis + '_output', 0)}"
        for basis in ("reported", "estimated")
    )
    return (
        f"## Repair summary\n\nAccepted fixes: {accepted}; "
        f"diagnosed fixes: {attempted}.\n"
        f"Mechanisms: {summary}.\n\n"
        f"Charged role executions: {counts}. "
        f"Observed wall time: {elapsed_seconds} seconds.\n\n"
        "Accepted fixes passed protected reproduction "
        "and per-case no-regression gates. "
        "Unaccepted fixes are excluded from this branch.\n\n"
        f"Accepted validation observations: {observations}. {tokens}.\n\n"
        "Token totals cover recorded role requests and unique accepted validation "
        "evidence, including reused evidence; they are not total billed cost.\n\n"
        "## Limits and checks\n\n"
        "Deterministic adapter tests establish orchestration behavior; "
        "they are not evidence of natural improvement. "
        "Active execution time is not aggregated in this report. "
        "Review the diff and repository CI checks before manual merge. "
        "After merging, update main and restart the harness manually.\n"
    )


class RepairReporter:
    def __init__(
        self,
        factory: Callable[[], UnitOfWork],
        artifacts: ArtifactService,
        clock: Clock,
    ) -> None:
        self.factory, self.artifacts, self.clock = factory, artifacts, clock

    async def build(self, repair_id: UUID) -> RepairReport:
        async with self.factory() as uow:
            repair = await uow.repairs.get(repair_id)
            fixes = await uow.repairs.list_fixes(repair_id)
            attempts = await uow.repairs.list_attempts(repair_id)
            acceptances = await uow.repairs.list_acceptances(repair_id)
            role_usage = [
                row
                for attempt in attempts
                if attempt.harness_run_id
                for row in await uow.evals.list_usage(attempt.harness_run_id)
            ]
        if repair is None:
            raise ValueError("Repair missing")
        usage: Counter[str] = Counter()
        for row in role_usage:
            basis = "reported" if row.source == "reported" else "estimated"
            usage[basis + "_input"] += row.input_tokens
            usage[basis + "_output"] += row.output_tokens
        observations = set()
        mechanisms = []
        for acceptance in acceptances:
            if acceptance.status != "accepted":
                continue
            from crucible.repair.cases import ApprovedCase

            metadata, raw = await self.artifacts.read(
                UUID(str(acceptance.result_refs_json["case_artifact"]))
            )
            if (
                metadata.task_id != repair.source_task_id
                or metadata.sensitivity != "private"
                or hashlib.sha256(raw).hexdigest() != metadata.content_hash
            ):
                raise ValueError("Report case integrity failure")
            case = ApprovedCase.model_validate_json(raw)
            evidence_ids = acceptance.result_refs_json.get("evidence_ids", [])
            # The second artifact is the host-protected baseline/candidate batch.
            if isinstance(evidence_ids, list) and len(evidence_ids) == 2:
                metadata, evidence_raw = await self.artifacts.read(
                    UUID(str(evidence_ids[1]))
                )
                if (
                    metadata.task_id != repair.source_task_id
                    or metadata.sensitivity != "private"
                    or hashlib.sha256(evidence_raw).hexdigest() != metadata.content_hash
                ):
                    raise ValueError("Report evidence integrity failure")
                batches = json.loads(evidence_raw)
                for batch in batches.values():
                    for partition in (
                        "custom",
                        "development",
                        "held_out",
                        "regressions",
                    ):
                        for rows in batch.get(partition, {}).values():
                            for row in rows:
                                if row["id"] in observations:
                                    continue
                                observations.add(row["id"])
                                observed = row["usage"]
                                basis = observed.get("basis")
                                if basis in {"reported", "estimated"}:
                                    for direction in ("input", "output"):
                                        count = observed.get(direction + "_tokens", 0)
                                        if type(count) is not int or count < 0:
                                            raise ValueError(
                                                "Invalid trusted token count"
                                            )
                                        usage[basis + "_" + direction] += count
            mechanisms.append(
                {
                    "tool": "tool behavior",
                    "context_selection": "context selection",
                    "compaction": "conversation compaction",
                }.get(case.case.kind, "harness behavior")
            )
        private = await private_artifact(
            self.artifacts,
            repair.source_task_id,
            canonical_json(
                {
                    "repair": asdict(repair),
                    "fixes": [asdict(f) for f in fixes],
                    "attempts": [asdict(a) for a in attempts],
                    "acceptances": [asdict(a) for a in acceptances],
                }
            ),
        )
        return RepairReport(
            public_report(
                accepted=sum(f.status == "accepted" for f in fixes),
                attempted=len(fixes),
                roles=dict(Counter(a.role for a in attempts if a.charged)),
                elapsed_seconds=max(
                    0, int((self.clock.now() - repair.created_at).total_seconds())
                ),
                mechanisms=mechanisms,
                usage=dict(usage),
                observations=len(observations),
            ),
            private,
        )
