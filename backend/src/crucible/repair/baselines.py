"""Durable lookup of compatible completed baseline evidence."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from crucible.repair.trials import TrialEvidence


class BaselineCache:
    def __init__(self, unit_of_work: Callable[[], UnitOfWork]) -> None:
        self.factory = unit_of_work

    async def get(
        self, fingerprint: str, case_id: str, repeat_index: int
    ) -> TrialEvidence | None:
        async with self.factory() as uow:
            for result in await uow.repair_trials.list_results(
                fingerprint, case_id, "baseline"
            ):
                job = await uow.repair_trials.get_job(result.id)
                if (
                    result.repeat_index == repeat_index
                    and result.verdict in {"pass", "fail"}
                    and job is not None
                    and job.status == "completed"
                ):
                    return result
        return None


if TYPE_CHECKING:
    from crucible.application.ports import UnitOfWork
