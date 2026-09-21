import asyncio
from collections import defaultdict
from uuid import UUID


class TaskEventNotifier:
    def __init__(self) -> None:
        self._conditions: defaultdict[UUID, asyncio.Condition] = defaultdict(
            asyncio.Condition
        )
        self._generations: defaultdict[UUID, int] = defaultdict(int)

    def generation(self, task_id: UUID) -> int:
        return self._generations[task_id]

    async def notify(self, task_id: UUID) -> None:
        condition = self._conditions[task_id]
        async with condition:
            self._generations[task_id] += 1
            condition.notify_all()

    async def wait(self, task_id: UUID, generation: int, timeout: float) -> bool:
        condition = self._conditions[task_id]
        async with condition:
            if self._generations[task_id] != generation:
                return True
            try:
                await asyncio.wait_for(
                    condition.wait_for(
                        lambda: self._generations[task_id] != generation
                    ),
                    timeout,
                )
            except TimeoutError:
                return False
            return True
