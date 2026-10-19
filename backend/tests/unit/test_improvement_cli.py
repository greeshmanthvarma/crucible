import pytest

from crucible.improvement.cli import parser


def test_state_root_and_explicit_operation_required():
    with pytest.raises(SystemExit):
        parser().parse_args(["start"])
    with pytest.raises(SystemExit):
        parser().parse_args(["--state-root", "/tmp/state", "start"])


def test_resume_does_not_imply_evaluation():
    args = parser().parse_args(
        ["--state-root", "/tmp/state", "resume", "00000000-0000-0000-0000-000000000001"]
    )
    assert args.operation == "resume"
    assert not hasattr(args, "evaluate")


async def test_pending_approval_is_cancellable_without_executor(tmp_path, monkeypatch):
    import asyncio
    import os

    from crucible.improvement.lifecycle import ExperimentService

    read, write = os.pipe()
    original = os.read
    loop = asyncio.get_running_loop()
    callbacks = {}
    monkeypatch.setattr(os, "isatty", lambda fd: True)
    monkeypatch.setattr(
        loop, "add_reader", lambda fd, callback: callbacks.update({fd: callback})
    )
    monkeypatch.setattr(loop, "remove_reader", lambda fd: callbacks.pop(fd, None))
    monkeypatch.setattr(os, "read", lambda fd, size: original(read, size))
    service = ExperimentService(None, tmp_path)
    try:
        task = asyncio.create_task(service._approve(None, {}))
        await asyncio.sleep(0)
        assert callbacks
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not callbacks
    finally:
        os.close(read)
        os.close(write)


async def test_admission_subprocess_output_is_bounded(tmp_path):
    import sys

    from crucible.improvement.lifecycle import RecoveryRequired, bounded_process

    with pytest.raises(RecoveryRequired, match="too large"):
        await bounded_process(
            tmp_path, sys.executable, "-c", "print('x'*100000)", limit=100
        )


async def test_unknown_experiment_has_stable_exit_code(tmp_path, capsys):
    from crucible.improvement.cli import run_cli
    from crucible.improvement.store import ExperimentStore

    store = await ExperimentStore.open(tmp_path / "experiments.sqlite3")
    await store.close()
    assert (
        await run_cli(
            [
                "--state-root",
                str(tmp_path),
                "status",
                "00000000-0000-0000-0000-000000000001",
            ]
        )
        == 2
    )
    assert "conflicts" in capsys.readouterr().err


async def test_status_never_creates_missing_state(tmp_path):
    from crucible.improvement.cli import run_cli

    root = tmp_path / "absent"
    assert (
        await run_cli(
            [
                "--state-root",
                str(root),
                "status",
                "00000000-0000-0000-0000-000000000001",
            ]
        )
        == 2
    )
    assert not root.exists()
