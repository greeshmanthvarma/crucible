import asyncio

from pydantic import Field

from crucible.domain.ids import new_id
from crucible.engine.gateway import CompleteToolCall, ModelToolDefinition
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from crucible.tools.definitions import ToolArguments, ToolContext, ToolOutcome
from crucible.tools.dispatcher import DispatchContext, ToolDispatcher
from crucible.tools.registry import ToolRegistry
from tests.integration.engine.conftest import FixedClock
from tests.integration.storage.test_tool_loop_storage import seed_exchange


class LabelArguments(ToolArguments):
    label: str = Field(min_length=1)


class ControlledTool:
    argument_model = LabelArguments

    def __init__(
        self,
        name: str,
        actions: list[str],
        *,
        parallel_safe: bool,
        release_first: asyncio.Event | None = None,
    ) -> None:
        self.definition = ModelToolDefinition(
            name, name, LabelArguments.model_json_schema()
        )
        self.parallel_safe = parallel_safe
        self.actions = actions
        self.release_first = release_first

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = LabelArguments.model_validate(arguments)
        self.actions.append(f"start:{args.label}")
        if self.release_first is not None and args.label == "first":
            await self.release_first.wait()
        if self.release_first is not None and args.label == "second":
            self.release_first.set()
        self.actions.append(f"finish:{args.label}")
        return ToolOutcome({"label": args.label}, args.label)


async def prepared(database: Database, suffix: str):
    uow = SqlAlchemyUnitOfWork(database)
    async with uow:
        task, run, step, message = await seed_exchange(uow, suffix)
        await uow.commit()
    return task, run, step, message


async def test_parallel_batch_records_completion_order_but_returns_source_order(
    database: Database,
) -> None:
    task, run, step, message = await prepared(database, "parallel")
    actions: list[str] = []
    release = asyncio.Event()
    tool = ControlledTool("read", actions, parallel_safe=True, release_first=release)
    dispatcher = ToolDispatcher(
        ToolRegistry((tool,)), lambda: SqlAlchemyUnitOfWork(database), FixedClock()
    )
    calls = (
        CompleteToolCall(new_id(), "read", {"label": "first"}),
        CompleteToolCall(new_id(), "read", {"label": "second"}),
    )

    results = await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, task.workspace_path),
        calls,
    )

    assert [result.tool_call_id for result in results] == [calls[0].id, calls[1].id]
    assert actions.index("start:second") < actions.index("finish:first")
    async with SqlAlchemyUnitOfWork(database) as uow:
        completed = await uow.tool_results.list_for_step(step.id)
    assert [result.tool_call_id for result in completed] == [calls[1].id, calls[0].id]


async def test_mixed_batch_runs_entirely_sequentially(database: Database) -> None:
    task, run, step, message = await prepared(database, "mixed")
    actions: list[str] = []
    read = ControlledTool("read", actions, parallel_safe=True)
    write = ControlledTool("write", actions, parallel_safe=False)
    dispatcher = ToolDispatcher(
        ToolRegistry((read, write)),
        lambda: SqlAlchemyUnitOfWork(database),
        FixedClock(),
    )

    await dispatcher.execute_batch(
        DispatchContext(task.id, run.id, step.id, message.id, task.workspace_path),
        (
            CompleteToolCall(new_id(), "read", {"label": "first"}),
            CompleteToolCall(new_id(), "write", {"label": "second"}),
        ),
    )

    assert actions == ["start:first", "finish:first", "start:second", "finish:second"]
