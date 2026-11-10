"""Exercise the measured round's case mining and proposal handoff offline."""

import asyncio
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import text

from crucible.domain.tools import ToolResultStatus
from crucible.engine.gateway import ModelStop, ModelStopReason, ModelUsage, TextDelta
from crucible.improvement.evidence_views import DevelopmentViewBuilder
from crucible.improvement.measured_mining import MeasuredCaseMiner
from crucible.improvement.roles import RoleRunner
from crucible.improvement.search import LocalSearchExecution
from crucible.improvement.store import BudgetDenied
from crucible.repair.identity import canonical_json
from tests.unit import test_improvement_evidence_views as evidence
from tests.unit import test_improvement_search_store as manifests
from tests.unit import test_trajectory as trajectory

evidence_setup = evidence.evidence_setup
parallel_records = trajectory.parallel_records


@pytest.fixture(autouse=True)
def mining_limits(monkeypatch, request):
    original = manifests.search_manifest

    def configured():
        manifest = original()
        return manifest.model_copy(
            update={
                "model": manifest.model.model_copy(
                    update={"input_limit": 8_000, "output_reserve": 2_000}
                ),
                "role_limits": {
                    **manifest.role_limits,
                    "mining": manifest.role_limits["mining"].model_copy(
                        update={"max_model_tokens": 24_000}
                    ),
                },
                "budget": manifest.budget.model_copy(
                    update={
                        "max_reported_and_reserved_tokens": (
                            100
                            if request.node.callspec.params.get("mode") == "quota"
                            else 24_000
                        ),
                        "max_total_output_bytes": 128_000,
                    }
                ),
                "command_policy": manifest.command_policy.model_copy(
                    update={"max_output_bytes_per_call": 64_000}
                ),
            }
        )

    monkeypatch.setattr(manifests, "search_manifest", configured)


async def install_v2_traces(store, root, ids, trace):
    for trial_id in ids:
        path = root / "trials" / str(trial_id) / "result.json"
        envelope = json.loads(path.read_bytes())
        solver = envelope["result"]["solver"]
        task_id, run_id = UUID(solver["task_id"]), UUID(solver["run_id"])
        result, other = trace["tool_results"]
        raw = canonical_json(
            {
                **trace,
                "messages": tuple(
                    replace(item, task_id=task_id, run_id=run_id)
                    for item in trace["messages"]
                ),
                "tool_calls": tuple(
                    replace(item, task_id=task_id, run_id=run_id, name="apply_patch")
                    for item in trace["tool_calls"]
                ),
                "tool_results": (
                    replace(
                        result,
                        task_id=task_id,
                        run_id=run_id,
                        status=ToolResultStatus.FAILED,
                        error_code="patch_rejected",
                    ),
                    replace(other, task_id=task_id, run_id=run_id),
                ),
            }
        )
        (path.parent / "trace.json").write_bytes(raw)
        envelope["artifacts"]["trace.json"] = hashlib.sha256(raw).hexdigest()
        path.write_bytes(canonical_json(envelope))
        async with store.engine.begin() as connection:
            await connection.execute(
                text("UPDATE experiment_trials SET artifact_ref=:ref WHERE id=:id"),
                {
                    "id": str(trial_id),
                    "ref": json.dumps(
                        {
                            "path": str(path.relative_to(root)),
                            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        }
                    ),
                },
            )


@pytest.mark.parametrize(
    "mode", ["linked", "recovered", "quota", "unreported_usage", "stop"]
)
async def test_measured_case_mining_checkpoint(
    evidence_setup, parallel_records, tmp_path, monkeypatch, mode
):
    store, root, run, snapshot, ids = evidence_setup
    await install_v2_traces(store, root, ids, parallel_records)
    round = await store.begin_round(run.id, snapshot.snapshot_id, run.revision)
    requests = []
    streaming = asyncio.Event()
    closed = asyncio.Event()

    class Gateway:
        async def stream(self, request):
            requests.append(request)
            assert request.tools == ()
            assert request.response_format["strict"] is True
            assert "HIDDEN_TRACE_CANARY" not in repr(request.messages)
            if mode == "stop":
                try:
                    streaming.set()
                    await asyncio.Future()
                finally:
                    closed.set()
            yield TextDelta(
                json.dumps(
                    {
                        "result": {
                            "diagnosis": {
                                "observed_behavior": "Patch was rejected.",
                                "relationship": "recovered"
                                if mode == "recovered"
                                else "linked",
                                "evidence_refs": ["E6"],
                                "uncertainty": "The cause remains open.",
                            }
                        }
                    }
                )
            )
            if mode != "unreported_usage":
                yield ModelUsage(1000, 100)
            yield ModelStop(ModelStopReason.COMPLETE)

    views = DevelopmentViewBuilder(store, root)
    roles = RoleRunner(store, root, None, views)
    miner = MeasuredCaseMiner(store, root, views, Gateway())
    local = LocalSearchExecution(
        store,
        SimpleNamespace(root=root),
        tmp_path,
        roles,
        None,
        views,
        None,
        None,
        None,
        tmp_path,
        case_miner=miner,
    )
    if mode == "stop":
        task = asyncio.create_task(local.mine(round))
        async with asyncio.timeout(5):
            await streaming.wait()
            await store.request_stop(run.id)
            with pytest.raises(asyncio.CancelledError):
                await task
        assert closed.is_set()
        assert await store.unsettled_usage(run.id)
        (role,) = await store.roles(round.id)
        assert role.execution.status == "interrupted"
        assert not await store.events(run.id, "case_mining_report")
        return
    if mode == "quota":
        with pytest.raises(BudgetDenied, match="case mining budget"):
            await local.mine(round)
        assert not requests
    elif mode == "unreported_usage":
        with pytest.raises(ValueError, match="usage requires reconciliation"):
            await local.mine(round)
        assert await store.unsettled_usage(run.id)
    else:
        result = await local.mine(round)
        assert await local.mine(round) == result
        assert len(requests) == 1
        if mode == "recovered":
            assert result.disposition == "no_actionable_weakness"
            assert not result.weaknesses
        else:
            (weakness,) = result.weaknesses
            assert weakness.id == "apply_patch_failure"
            assert [ref.trial_id for ref in weakness.evidence] == [ids[0]]

            async def proposal_dispatch(round_record, view, kind, role_input):
                assert round_record.id == round.id
                assert view.baseline_snapshot_id == snapshot.snapshot_id
                assert kind == "proposal"
                assert role_input["mining"] == result.model_dump(mode="json")
                return "proposal dispatched"

            monkeypatch.setattr(roles, "_run", proposal_dispatch)
            view = local._views[round.id]
            assert await roles.propose(round.id, result, view) == "proposal dispatched"
            path = root / (await store.roles(round.id))[0].execution.output_ref
            path.write_text("{}")
            with pytest.raises(ValueError, match="recorded mining"):
                await roles.propose(round.id, result, view)
    (role,) = await store.roles(round.id)
    assert role.execution.status == (
        "error" if mode in {"quota", "unreported_usage"} else "complete"
    )
    (report,) = await store.events(run.id, "case_mining_report")
    raw = (root / report["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == report["sha256"]
    attempt = json.loads(raw)["attempts"][0]
    assert attempt["trial_id"] == str(ids[0])
    assert attempt["requests_sent"] == len(requests)
    assert len(await store.events(run.id, "request_terminal")) == len(requests)
    if mode == "quota":
        assert attempt["reserved_tokens"] == 0
