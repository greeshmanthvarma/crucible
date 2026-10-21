from uuid import uuid4

import pytest

from crucible.benchmarks.contracts import TrialLimits
from crucible.benchmarks.trials import BudgetedTrialGateway
from crucible.engine.gateway import (
    ModelStop,
    ModelStopReason,
    ModelUsage,
    PreparedModelRequest,
)
from crucible.engine.run_engine import RunBudgetExceeded
from crucible.models.token_counting import estimate_input_tokens


async def test_all_model_requests_share_one_token_ledger() -> None:
    class Upstream:
        calls = 0

        async def stream(self, request):
            self.calls += 1
            yield ModelUsage(10, 10)
            yield ModelStop(ModelStopReason.COMPLETE)

    upstream = Upstream()
    limits = TrialLimits(
        max_steps=10,
        max_tool_calls=10,
        # Admit two calls after settling the first one's 20 actual tokens,
        # but not a third. Include the full local input reservation.
        max_model_tokens=20 + estimate_input_tokens((), ()) + 20,
        max_active_seconds=30,
        wall_timeout_seconds=30,
    )
    gateway = BudgetedTrialGateway(upstream, "model", limits, uuid4())
    for _ in range(2):
        request = PreparedModelRequest(uuid4(), uuid4(), "model", (), (), 20)
        assert [item async for item in gateway.stream(request)]
    with pytest.raises(RunBudgetExceeded):
        request = PreparedModelRequest(uuid4(), uuid4(), "model", (), (), 20)
        _ = [item async for item in gateway.stream(request)]
    assert upstream.calls == 2
    assert gateway.usage().input_tokens + gateway.usage().output_tokens == 40
