"""In-process HTTP tests with a deterministic upstream; no provider charges."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from crucible.api.repair_gateway import router
from crucible.engine.gateway import ModelStop, ModelStopReason, ModelUsage, TextDelta
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.worker_gateway import WorkerModelGateway
from crucible.repair.worker_protocol import encode_request
from tests.unit.repair.test_worker_protocol import prepared_request


class Clock:
    value = datetime(2026, 10, 14, tzinfo=UTC)

    def now(self):
        return self.value


class Upstream:
    def __init__(self, *, report_usage=True):
        self.requests = []
        self.report_usage = report_usage
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def stream(self, request):
        self.requests.append(request)
        self.entered.set()
        await self.release.wait()
        yield TextDelta("deterministic café")
        if self.report_usage:
            yield ModelUsage(10, 2)
        yield ModelStop(ModelStopReason.COMPLETE)


@pytest.fixture
def setup_gateway():
    clock, upstream = Clock(), Upstream()
    service = ScopedModelGateway(
        upstream, clock=clock, endpoint="http://test/internal/repair-model/responses"
    )
    app = FastAPI()
    app.state.repair_gateway = service
    app.include_router(router)
    return clock, upstream, service, app


def issue(service, job_id, **budgets):
    return service.issue(
        job_id,
        "test-model",
        {"max_requests": 1, "max_tokens": 256} | budgets,
        timedelta(seconds=30),
    )


async def post(client, credential, job_id, *, payload=None, token=None):
    return await client.post(
        "/internal/repair-model/responses",
        headers={
            "Authorization": f"Bearer {credential.token if token is None else token}"
        },
        json={
            "schema_version": 1,
            "job_id": str(job_id),
            "request": encode_request(prepared_request())
            if payload is None
            else payload,
        },
    )


async def test_gateway_denies_expired_wrong_model_and_exhausted_session(setup_gateway):
    clock, upstream, service, app = setup_gateway
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        wrong_model = encode_request(prepared_request()) | {"model": "other"}
        assert (
            await post(client, credential, job_id, payload=wrong_model)
        ).status_code == 403
        assert not upstream.requests
        assert (await post(client, credential, job_id)).status_code == 200
        assert (await post(client, credential, job_id)).status_code == 429
        expired_id = uuid4()
        expired = issue(service, expired_id)
        clock.value += timedelta(seconds=31)
        assert (await post(client, expired, expired_id)).status_code == 401
    assert len(upstream.requests) == 1
    usage = service.usage(job_id)
    assert usage.requests == 1
    assert usage.input_tokens == 10 and usage.output_tokens == 2
    assert usage.basis == "reported"


async def test_gateway_requires_bearer_matching_job_and_revokes_on_restart(
    setup_gateway,
):
    clock, upstream, service, app = setup_gateway
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (
            await post(client, credential, job_id, token="invalid")
        ).status_code == 401
        assert (await post(client, credential, uuid4())).status_code in {401, 403}
        missing = await client.post("/internal/repair-model/responses", json={})
        assert missing.status_code == 401
        service.revoke(job_id)
        denied = await post(client, credential, job_id)
        assert denied.status_code == 401
        assert credential.token not in denied.text
        app.state.repair_gateway = ScopedModelGateway(
            upstream, clock=clock, endpoint=credential.endpoint
        )
        assert (await post(client, credential, job_id)).status_code == 401
    assert not upstream.requests
    assert credential.token not in repr(credential)


async def test_gateway_reserves_token_budget_before_dispatch(setup_gateway):
    _, upstream, service, app = setup_gateway
    job_id = uuid4()
    credential = issue(service, job_id, max_tokens=1)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await post(client, credential, job_id)).status_code == 429
    assert not upstream.requests


async def test_concurrent_requests_cannot_overdraw_request_quota(setup_gateway):
    _, upstream, service, app = setup_gateway
    upstream.release.clear()
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = asyncio.create_task(post(client, credential, job_id))
        try:
            await asyncio.wait_for(upstream.entered.wait(), 2)
            assert (await post(client, credential, job_id)).status_code == 429
        finally:
            upstream.release.set()
            await first
    assert len(upstream.requests) == 1


async def test_worker_gateway_reconstructs_stream_and_never_serializes_token(
    setup_gateway,
):
    _, upstream, service, app = setup_gateway
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        worker = WorkerModelGateway(credential, job_id, client=client)
        items = [item async for item in worker.stream(prepared_request())]
    assert items == [
        TextDelta("deterministic café"),
        ModelUsage(10, 2),
        ModelStop(ModelStopReason.COMPLETE),
    ]
    assert len(upstream.requests) == 1
    assert credential.token not in repr(service.usage(job_id))


async def test_ndjson_has_versioned_events_and_no_credentials(setup_gateway):
    _, _, service, app = setup_gateway
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await post(client, credential, job_id)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert all(
        json.loads(line)["schema_version"] == 1 for line in response.text.splitlines()
    )
    assert credential.token not in response.text


async def test_missing_provider_usage_is_explicitly_estimated(setup_gateway):
    _, upstream, service, app = setup_gateway
    upstream.report_usage = False
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await post(client, credential, job_id)).status_code == 200
    usage = service.usage(job_id)
    assert usage.basis == "estimated"
    assert usage.input_tokens > 0 and usage.output_tokens > 0


async def test_concurrent_output_reservations_cannot_overdraw_token_budget(
    setup_gateway,
):
    _, upstream, service, app = setup_gateway
    upstream.release.clear()
    job_id = uuid4()
    credential = issue(service, job_id, max_requests=2, max_tokens=700)
    payload = encode_request(prepared_request()) | {"max_output_tokens": 500}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = asyncio.create_task(post(client, credential, job_id, payload=payload))
        try:
            await asyncio.wait_for(upstream.entered.wait(), 2)
            assert (
                await post(client, credential, job_id, payload=payload)
            ).status_code == 429
        finally:
            upstream.release.set()
            await first
    assert len(upstream.requests) == 1


async def test_revoke_cancels_a_stalled_provider_stream(setup_gateway):
    _, upstream, service, app = setup_gateway
    upstream.release.clear()
    job_id = uuid4()
    credential = issue(service, job_id)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        task = asyncio.create_task(post(client, credential, job_id))
        try:
            await asyncio.wait_for(upstream.entered.wait(), 2)
            service.revoke(job_id)
            response = await asyncio.wait_for(task, 2)
        finally:
            upstream.release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    assert "worker_session_expired" in response.text
    assert service.usage(job_id).basis == "estimated"
