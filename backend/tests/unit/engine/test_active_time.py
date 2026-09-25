import asyncio

import pytest

from crucible.engine.active_time import ActiveTimeBudget


async def test_paused_human_wait_does_not_consume_active_time() -> None:
    budget = ActiveTimeBudget(0.04)

    async with budget.track():
        await asyncio.sleep(0.01)
        async with budget.pause():
            await asyncio.sleep(0.06)
        await asyncio.sleep(0.01)


async def test_active_work_still_expires_budget() -> None:
    budget = ActiveTimeBudget(0.01)

    with pytest.raises(TimeoutError):
        async with budget.track():
            await asyncio.sleep(0.03)
