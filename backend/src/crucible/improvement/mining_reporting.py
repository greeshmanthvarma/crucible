"""Positive-list diagnostic status and deterministic local report artifacts."""

from pathlib import Path
from uuid import UUID

from crucible.improvement.mining_contracts import MiningDiagnosticStatus
from crucible.improvement.mining_diagnostic import _json, _sha, _write
from crucible.improvement.mining_store import MiningDiagnosticStore
from crucible.improvement.source import safe_path
from crucible.repair.identity import canonical_json


async def status(store: MiningDiagnosticStore, id: UUID) -> MiningDiagnosticStatus:
    return await store.diagnostic_status(id)


async def write_mining_report(
    store: MiningDiagnosticStore, id: UUID, output: Path
) -> dict[str, str]:
    safe_path(output)
    if output.exists():
        raise ValueError("new report output required")
    state = await status(store, id)
    output.mkdir(mode=0o700)
    metrics = canonical_json(state)
    _write(output / "metrics.json", metrics)
    report = (
        "# Mining diagnostic\n\n"
        f"Diagnostic: {state.diagnostic_id}\n\nStatus: {state.status}\n\n"
        f"Disposition: {state.final_disposition or 'unavailable'}\n\n"
        "Schema and citation checks establish membership and inspection, "
        "not semantic correctness.\n"
    ).encode()
    _write(output / "report.md", report)
    files = {"metrics.json": _sha(metrics), "report.md": _sha(report)}
    _json(output / "report-manifest.json", files)
    return {**files, "report-manifest.json": _sha(canonical_json(files))}
