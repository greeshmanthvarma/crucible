"""Bounded wire adapter with improvement-specific quota termination."""

from collections.abc import AsyncIterator

from crucible.engine.gateway import ModelError, ModelStreamItem, PreparedModelRequest
from crucible.engine.run_engine import RunBudgetExceeded
from crucible.repair.worker_gateway import WorkerModelGateway as BaseWorkerModelGateway


class WorkerModelGateway(BaseWorkerModelGateway):
    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        async for event in super().stream(request):
            if (
                isinstance(event, ModelError)
                and event.code == "improvement_budget_exhausted"
            ):
                raise RunBudgetExceeded("Host model quota exhausted")
            yield event
