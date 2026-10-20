"""Shared host broker lifecycle; no search or measured-suite admission policy."""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import uvicorn

from crucible.improvement.broker import Approval, ImprovementBroker, broker_app
from crucible.improvement.contracts import ExecutionManifest
from crucible.improvement.store import ExecutionStore
from crucible.improvement.worker_supervisor import (
    DockerCommands,
    DockerPort,
    WorkerSupervisor,
)
from crucible.repair.gateway import ScopedModelGateway


@asynccontextmanager
async def execution_runtime(
    store: ExecutionStore,
    root: Path,
    manifest: ExecutionManifest,
    docker: DockerPort,
    gateway: ScopedModelGateway,
    approval: Approval,
) -> AsyncIterator[WorkerSupervisor]:
    broker = ImprovementBroker(store, gateway, DockerCommands(store, docker), approval)
    listener = socket.socket()
    listener.bind(("0.0.0.0", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]

    class BrokerServer(uvicorn.Server):
        @contextmanager
        def capture_signals(self):  # type: ignore[no-untyped-def]
            yield

    server = BrokerServer(
        uvicorn.Config(
            broker_app(broker), log_level="critical", access_log=False, lifespan="off"
        )
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(100):
            if server.started:
                break
            if task.done():
                await task
            await asyncio.sleep(0.01)
        if not server.started:
            raise RuntimeError("local broker did not start")
        yield WorkerSupervisor(
            store, broker, docker, root, manifest, f"http://host.docker.internal:{port}"
        )
    finally:
        outcomes = await asyncio.gather(
            *(broker.stop(job_id) for job_id in tuple(broker.sessions)),
            return_exceptions=True,
        )
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 10)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        listener.close()
        if any(isinstance(result, BaseException) for result in outcomes):
            raise RuntimeError("broker cleanup uncertain")
