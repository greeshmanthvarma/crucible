from dataclasses import replace
from datetime import UTC, datetime
from types import TracebackType
from typing import Self

import pytest

from crucible.domain.events import EventType
from crucible.domain.ids import new_id
from crucible.engine.journal import EventSpec, JournalMutation, RunJournal

NOW = datetime(2026, 9, 21, tzinfo=UTC)


class RecordingEvents:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions

    async def append(self, event):
        self.actions.append(f"event:{event.type}")
        sequence = sum(action.startswith("event:") for action in self.actions)
        return replace(event, task_sequence=sequence, run_sequence=sequence)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class RecordingEvals:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions

    async def rebuild_summary(self, run_id, now) -> None:
        self.actions.append("summary")


class RecordingUnitOfWork:
    def __init__(self, actions: list[str], *, fail_commit: bool = False) -> None:
        self.actions = actions
        self.fail_commit = fail_commit
        self.events = RecordingEvents(actions)
        self.evals = RecordingEvals(actions)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        self.actions.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit failed")

    async def rollback(self) -> None:
        self.actions.append("rollback")


class RecordingNotifier:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions

    async def notify(self, task_id) -> None:
        self.actions.append("notify")


async def test_journal_applies_mutation_appends_events_commits_then_notifies() -> None:
    actions: list[str] = []
    task_id, run_id = new_id(), new_id()

    async def apply(uow) -> None:
        actions.append("mutation")

    mutation = JournalMutation(
        task_id=task_id,
        run_id=run_id,
        apply=apply,
        events=(
            EventSpec(EventType.RUN_STARTED),
            EventSpec(EventType.RUN_COMPLETED),
        ),
    )
    uow = RecordingUnitOfWork(actions)
    journal = RunJournal(lambda: uow, FixedClock(), RecordingNotifier(actions))

    events = await journal.record(mutation)

    assert [event.run_sequence for event in events] == [1, 2]
    assert actions == [
        "mutation",
        "event:run.started",
        "event:run.completed",
        "summary",
        "commit",
        "notify",
    ]


async def test_journal_does_not_notify_when_commit_fails() -> None:
    actions: list[str] = []

    async def apply(uow) -> None:
        actions.append("mutation")

    journal = RunJournal(
        lambda: RecordingUnitOfWork(actions, fail_commit=True),
        FixedClock(),
        RecordingNotifier(actions),
    )
    mutation = JournalMutation(
        task_id=new_id(),
        run_id=new_id(),
        apply=apply,
        events=(EventSpec(EventType.RUN_STARTED),),
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await journal.record(mutation)

    assert actions == ["mutation", "event:run.started", "commit"]
