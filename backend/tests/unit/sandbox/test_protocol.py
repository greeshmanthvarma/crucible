from datetime import UTC, datetime
from pathlib import Path

from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.domain.ids import new_id
from crucible.sandbox.fake import FakeSandboxBackend
from crucible.sandbox.protocol import (
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxRequest,
    SandboxTermination,
)

NOW = datetime(2026, 9, 23, tzinfo=UTC)


def request() -> SandboxRequest:
    return SandboxRequest(
        new_id(),
        new_id(),
        new_id(),
        Path("/work/task"),
        CommandSpec(
            "python",
            ("-V",),
            ".",
            30,
            CommandNetwork.NONE,
            {},
            "runner@sha256:" + "a" * 64,
            "version",
            CommandLimits(1, 1024, 16, 100),
        ),
        (),
    )


async def test_fake_backend_is_deterministic_and_nonzero_exit_is_completed() -> None:
    outcome = SandboxOutcome(
        7,
        SandboxTermination.COMPLETED,
        NOW,
        NOW,
        "sha256:" + "a" * 64,
        "container-id",
        3,
        3,
        False,
    )
    backend = FakeSandboxBackend(
        outcome, (OutputChunk(OutputStream.STDERR, 1, b"bad"),)
    )
    received = []
    sandbox_request = request()

    actual = await backend.execute(sandbox_request, received.append)

    assert actual == outcome
    assert received[0].data == b"bad"
    assert backend.requests == [sandbox_request]
