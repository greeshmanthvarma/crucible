"""In-process authority plumbing, using scripted upstream model transport."""

import pytest

from crucible.improvement.broker import ImprovementBroker, WorkspaceBinding
from crucible.improvement.store import ExperimentStore
from tests.contract.test_improvement_worker import make_job
from tests.unit.test_improvement_manifest import make_manifest


async def test_worker_cannot_request_unbound_workspace(tmp_path):
    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    try:
        broker = ImprovementBroker(store, None, None)
        job = make_job()
        with pytest.raises(ValueError, match="workspace"):
            broker.issue(
                job,
                make_manifest(),
                WorkspaceBinding(tmp_path / "other", "wrong", job.solver_case.platform),
            )
    finally:
        await store.close()


async def setup_broker(tmp_path, upstream, executor=None):
    from uuid import uuid4

    from crucible.domain.clock import SystemClock
    from crucible.improvement.contracts import HarnessSnapshot
    from crucible.repair.gateway import ScopedModelGateway

    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    manifest = make_manifest()
    manifest = manifest.model_copy(
        update={
            "trial_limits": manifest.trial_limits.model_copy(
                update={"max_model_tokens": 2000}
            ),
            "budget": manifest.budget.model_copy(
                update={
                    "max_reported_and_reserved_tokens": 500,
                    "max_total_output_bytes": 4096,
                }
            ),
            "command_policy": manifest.command_policy.model_copy(
                update={"mode": "preauthorized-disposable"}
            ),
            "disposable_commands_authorized": True,
        }
    )
    await store.create(manifest, "repo", "key")
    job = make_job().model_copy(
        update={
            "experiment_id": manifest.experiment_id,
            "limits": manifest.trial_limits,
        }
    )
    snapshot = HarnessSnapshot(
        snapshot_id=uuid4(),
        parent_commit="a" * 40,
        source_digest=job.harness_digest,
        export_digest=job.export_digest,
        patch_digest=None,
        kind="baseline",
    )
    await store.register_snapshot(manifest.experiment_id, snapshot, "snapshot.json")
    trial = await store.create_trial(
        manifest.experiment_id, snapshot.snapshot_id, "development", 0, "key"
    )
    job = job.model_copy(update={"trial_id": trial})
    gateway = ScopedModelGateway(
        upstream,
        clock=SystemClock(),
        endpoint="http://broker/internal/improvement/model",
    )
    broker = ImprovementBroker(store, gateway, executor)
    credential = broker.issue(
        job,
        manifest,
        WorkspaceBinding(tmp_path, job.solver_case.image, job.solver_case.platform),
    )
    return store, broker, job, credential


async def test_parallel_requests_cannot_exceed_reservation(tmp_path):
    import asyncio
    from uuid import uuid4, uuid5

    from crucible.engine.gateway import (
        ModelMessage,
        ModelPart,
        ModelRole,
        ModelStop,
        ModelStopReason,
        ModelUsage,
        PreparedModelRequest,
    )
    from crucible.improvement.store import BudgetDenied

    entered = asyncio.Event()
    release = asyncio.Event()

    class Upstream:
        calls = 0

        async def stream(self, request):
            self.calls += 1
            entered.set()
            await release.wait()
            yield ModelUsage(10, 1)
            yield ModelStop(ModelStopReason.COMPLETE)

    upstream = Upstream()
    store, broker, job, credential = await setup_broker(tmp_path, upstream)
    request = PreparedModelRequest(
        uuid5(job.job_id, "run"),
        uuid4(),
        job.model.model_id,
        (ModelMessage(ModelRole.USER, (ModelPart("text", "x" * 1000),)),),
        (),
        100,
    )

    async def consume():
        return [e async for e in broker.model(credential.token, job.job_id, request)]

    first = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        second_job = job.model_copy(update={"job_id": uuid4()})
        session = broker.sessions[job.job_id]
        second_credential = broker.issue(second_job, session.manifest, session.binding)
        second_request = request.__class__(
            uuid5(second_job.job_id, "run"),
            request.step_id,
            request.model,
            request.messages,
            request.tools,
            request.max_output_tokens,
        )
        with pytest.raises(BudgetDenied):
            _ = [
                e
                async for e in broker.model(
                    second_credential.token, second_job.job_id, second_request
                )
            ]
        assert upstream.calls == 1
        release.set()
        await first
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)
        await store.close()


async def test_wrong_token_and_job_rejected(tmp_path):
    from uuid import uuid4

    from crucible.repair.gateway import GatewayDenied

    store, broker, job, credential = await setup_broker(tmp_path, None)
    try:
        with pytest.raises(GatewayDenied):
            broker._authorize("wrong", job.job_id)
        with pytest.raises(GatewayDenied):
            broker._authorize(credential.token, uuid4())
        broker.revoke(job.job_id)
        with pytest.raises(GatewayDenied):
            broker._authorize(credential.token, job.job_id)
    finally:
        await store.close()


async def test_replay_does_not_start_second_command(tmp_path):
    from datetime import UTC, datetime
    from uuid import uuid4

    from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
    from crucible.sandbox.protocol import SandboxOutcome, SandboxTermination

    class Commands:
        calls = 0

        async def execute(self, job, binding, command_id, spec):
            self.calls += 1
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

        async def stop(self, job_id):
            pass

    commands = Commands()
    store, broker, job, credential = await setup_broker(tmp_path, None, commands)
    spec = CommandSpec(
        "true",
        (),
        ".",
        5,
        CommandNetwork.NONE,
        {},
        job.solver_case.image,
        "check",
        CommandLimits(1, 1024, 64, 100),
    )
    identity = uuid4()
    try:
        first = await broker.command(credential.token, job.job_id, identity, spec)
        assert (
            await broker.command(credential.token, job.job_id, identity, spec) == first
        )
        assert commands.calls == 1
    finally:
        await store.close()


async def test_unknown_model_usage_remains_reserved(tmp_path):
    from uuid import uuid4, uuid5

    from crucible.engine.gateway import (
        ModelMessage,
        ModelPart,
        ModelRole,
        ModelStop,
        ModelStopReason,
        PreparedModelRequest,
    )
    from crucible.improvement.contracts import UsageAmounts
    from crucible.improvement.store import BudgetDenied

    class Upstream:
        async def stream(self, request):
            yield ModelStop(ModelStopReason.COMPLETE)

    store, broker, job, credential = await setup_broker(tmp_path, Upstream())
    request = PreparedModelRequest(
        uuid5(job.job_id, "run"),
        uuid4(),
        job.model.model_id,
        (ModelMessage(ModelRole.USER, (ModelPart("text", "hello"),)),),
        (),
        10,
    )
    try:
        _ = [e async for e in broker.model(credential.token, job.job_id, request)]
        assert (await store.get(job.experiment_id)).status == "error"
        with pytest.raises(BudgetDenied):
            await store.reserve_usage(job.experiment_id, None, UsageAmounts(tokens=1))
    finally:
        await store.close()


async def test_fixed_routes_only_and_wrong_bearer_denied(tmp_path):
    import httpx

    from crucible.improvement.broker import broker_app

    store, broker, job, credential = await setup_broker(tmp_path, None)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=broker_app(broker)),
            base_url="http://broker",
        ) as client:
            response = await client.get("/anything")
            assert response.status_code == 404
            response = await client.post("/internal/improvement/model", json={})
            assert response.status_code == 401
    finally:
        await store.close()


async def test_compaction_uses_same_protected_model_accounting(tmp_path):
    import json
    from uuid import uuid4

    import httpx

    from crucible.context.compaction import CompactionRequest
    from crucible.engine.compaction_gateway import ModelCompactionGateway
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.improvement.broker import broker_app
    from crucible.improvement.worker_gateway import WorkerModelGateway
    from crucible.repair.gateway import WorkerCredential

    class Upstream:
        calls = 0

        async def stream(self, request):
            self.calls += 1
            yield TextDelta(
                json.dumps(
                    {
                        k: "retained"
                        for k in [
                            "objective_and_constraints",
                            "decisions",
                            "repository_facts",
                            "changes",
                            "commands_and_validation",
                            "unresolved_problems",
                            "execution_state",
                            "important_paths_and_symbols",
                        ]
                    }
                )
            )
            yield ModelUsage(10, 10)
            yield ModelStop(ModelStopReason.COMPLETE)

    upstream = Upstream()
    store, broker, job, credential = await setup_broker(tmp_path, upstream)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=broker_app(broker)),
            base_url="http://broker",
        ) as client:
            worker = WorkerModelGateway(
                WorkerCredential(
                    credential.token, "http://broker/internal/improvement/model"
                ),
                job.job_id,
                client=client,
            )
            summary = await ModelCompactionGateway(
                worker, max_output_tokens=job.model.output_reserve
            ).compact(CompactionRequest(uuid4(), (), None, job.model.model_id, "v1"))
        assert summary.decisions == "retained"
        assert upstream.calls == 1
        assert broker.gateway.usage(job.job_id).input_tokens == 10
    finally:
        await store.close()


async def test_pending_approval_pauses_worker_and_blocks_parallel_model(tmp_path):
    import asyncio
    from datetime import UTC, datetime
    from uuid import uuid4

    from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        PreparedModelRequest,
    )
    from crucible.sandbox.protocol import SandboxOutcome, SandboxTermination

    entered = asyncio.Event()
    release = asyncio.Event()
    pauses = []

    class Upstream:
        calls = 0

        async def stream(self, request):
            self.calls += 1
            yield ModelUsage(1, 1)
            yield ModelStop(ModelStopReason.COMPLETE)

    class Commands:
        async def execute(self, job, binding, command_id, spec):
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

        async def stop(self, job_id):
            pass

    upstream = Upstream()
    store, broker, job, credential = await setup_broker(tmp_path, upstream, Commands())
    session = broker.sessions[job.job_id]
    session.manifest = session.manifest.model_copy(
        update={
            "command_policy": session.manifest.command_policy.model_copy(
                update={"mode": "interactive"}
            )
        }
    )

    async def pause(job_id, paused):
        pauses.append(paused)

    async def approve(job, spec):
        entered.set()
        await release.wait()
        return True

    broker.pause_worker = pause
    broker.approval = approve
    spec = CommandSpec(
        "true",
        (),
        ".",
        5,
        CommandNetwork.NONE,
        {},
        job.solver_case.image,
        "check",
        CommandLimits(1, 1024, 64, 100),
    )
    command = asyncio.create_task(
        broker.command(credential.token, job.job_id, uuid4(), spec)
    )

    async def consume():
        request = PreparedModelRequest(uuid4(), uuid4(), job.model.model_id, (), (), 10)
        return [e async for e in broker.model(credential.token, job.job_id, request)]

    model = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        model = asyncio.create_task(consume())
        await asyncio.sleep(0)
        assert pauses == [True]
        assert upstream.calls == 0
        release.set()
        await command
        await model
        assert pauses == [True, False]
        assert upstream.calls == 1
    finally:
        release.set()
        await asyncio.gather(
            command, *([model] if model else []), return_exceptions=True
        )
        await store.close()
