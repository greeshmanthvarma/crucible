import asyncio
from uuid import uuid4

from crucible.engine.notifier import TaskEventNotifier


async def test_notifier_wakes_followers_and_times_out() -> None:
    notifier = TaskEventNotifier()
    task_id = uuid4()
    generation = notifier.generation(task_id)
    waiter = asyncio.create_task(notifier.wait(task_id, generation, 1))

    await asyncio.sleep(0)
    await notifier.notify(task_id)

    assert await waiter is True
    assert notifier.generation(task_id) == generation + 1
    assert await notifier.wait(task_id, generation + 1, 0.001) is False
