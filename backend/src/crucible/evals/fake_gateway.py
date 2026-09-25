"""Versioned, deterministic model stand-in for development smoke evaluations."""

from collections.abc import AsyncIterator

from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelMessage,
    ModelRole,
    ModelStop,
    ModelStopReason,
    ModelStreamItem,
    PreparedModelRequest,
    TextDelta,
)

SOLUTIONS = {
    "tiny": ("answer.txt", "ready\n"),
    "greet": ("greet.sh", "#!/bin/sh\nprintf 'Hello, %s!\\n' \"$1\"\n"),
    "sum": ("sum.sh", "#!/bin/sh\nprintf '%s\\n' \"$(($1 + $2))\"\n"),
}


def _case_id(messages: tuple[ModelMessage, ...]) -> str:
    for message in reversed(messages):
        if message.role is not ModelRole.USER:
            continue
        for part in message.parts:
            text = part.text_content or ""
            for case_id in SOLUTIONS:
                if f"EVAL_CASE:{case_id}" in text:
                    return case_id
    raise ValueError("Deterministic eval gateway requires a known Case marker")


class DeterministicEvalGateway:
    """Exercise the normal Run and tool loop without a provider dependency."""

    def __init__(self) -> None:
        self._written_runs: set[str] = set()

    async def stream(
        self, request: PreparedModelRequest
    ) -> AsyncIterator[ModelStreamItem]:
        run_id = str(request.run_id)
        if run_id in self._written_runs:
            yield TextDelta("Done.")
            yield ModelStop(ModelStopReason.COMPLETE)
            return
        case_id = _case_id(request.messages)
        path, content = SOLUTIONS[case_id]
        self._written_runs.add(run_id)
        yield CompleteToolCall(
            new_id(), "write_file", {"path": path, "content": content}
        )
        yield ModelStop(ModelStopReason.TOOL_CALLS)
