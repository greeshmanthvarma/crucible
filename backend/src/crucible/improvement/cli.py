"""Explicit improvement commands. Nothing is scheduled or resumed implicitly."""

import argparse
import asyncio
import json
import signal
import sys
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

from sqlalchemy.exc import NoResultFound

from crucible.improvement.lifecycle import ExperimentService, RecoveryRequired
from crucible.improvement.manifest import ManifestDrift
from crucible.improvement.store import ExperimentStore, RevisionConflict


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="crucible improve")
    command.add_argument("--state-root", type=Path, required=True)
    operations = command.add_subparsers(dest="operation", required=True)
    start = operations.add_parser("start")
    start.add_argument("--config", type=Path, required=True)
    start.add_argument("--idempotency-key", required=True)
    start.add_argument("--authorize-disposable-commands", action="store_true")
    for operation in (
        "status",
        "baseline",
        "search",
        "finalize",
        "final-evaluate",
        "candidate-evaluate",
        "stop",
        "resume",
        "close",
    ):
        operations.add_parser(operation).add_argument("id", type=UUID)
    recover = operations.add_parser("recover")
    recover.add_argument("id", type=UUID)
    recover.add_argument(
        "--acknowledge-reservation", type=UUID, action="append", default=[]
    )
    retry = operations.add_parser("authorize-retry")
    retry.add_argument("id", type=UUID)
    retry.add_argument("--trial", type=UUID, action="append", required=True)
    stages = operations.add_parser("continue")
    stages.add_argument("id", type=UUID)
    stages.add_argument(
        "--stop-after", choices=("baseline", "mining", "search"), default="baseline"
    )
    report = operations.add_parser("report")
    report.add_argument("id", type=UUID)
    report.add_argument("--output", type=Path, required=True)
    publish = operations.add_parser("publish")
    publish.add_argument("id", type=UUID)
    publish.add_argument("--remote", default="origin")
    publish.add_argument("--base", default="main")
    publish.add_argument("--authorize-remote-write", action="store_true")
    publish.add_argument("--report-only", action="store_true")
    return command


async def run_cli(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv or [])
    # Permit required --state-root before or after the operation.
    if "--state-root" in arguments:
        index = arguments.index("--state-root")
        if index + 1 < len(arguments):
            root = arguments[index : index + 2]
            del arguments[index : index + 2]
            arguments = root + arguments
    args = parser().parse_args(arguments)
    path = args.state_root / "experiments.sqlite3"
    if args.operation != "start" and not path.exists():
        print("Improvement state does not exist.", file=sys.stderr)
        return 2
    store = await ExperimentStore.open(path)
    service = ExperimentService(store, args.state_root)
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    previous = {}
    if current:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.getsignal(sig)
            loop.add_signal_handler(sig, current.cancel)
    try:
        if args.operation == "start":
            record = await service.start(
                args.config, args.idempotency_key, args.authorize_disposable_commands
            )
            output = await service.status(record.id)
        elif args.operation == "recover":
            output = await service.recover(args.id, tuple(args.acknowledge_reservation))
        elif args.operation == "authorize-retry":
            output = await service.authorize_retry(args.id, tuple(args.trial))
        elif args.operation == "continue":
            output = await service.run_stages(args.id, args.stop_after)
        elif args.operation == "report":
            output = await service.report(args.id, args.output)
        elif args.operation == "publish":
            output = await service.publish(
                args.id,
                args.remote,
                args.base,
                args.authorize_remote_write,
                args.report_only,
            )
        else:
            action = {
                "status": service.status,
                "baseline": service.baseline,
                "search": service.search,
                "finalize": service.finalize,
                "final-evaluate": service.evaluate_final,
                "candidate-evaluate": service.evaluate_candidate,
                "stop": service.stop,
                "resume": service.resume,
                "close": service.close,
            }[args.operation]
            output = await action(args.id)
        print(json.dumps(output, sort_keys=True))
        if output.get("reason") == "operator_stopped":
            return 130
        if output.get("complete") is False:
            return 3
        return (
            3
            if output.get("status") == "error"
            or output.get("reason") in ("infrastructure_error", "budget_limit")
            else 0
        )
    except asyncio.CancelledError:
        if hasattr(args, "id"):
            try:
                await service.stop(args.id)
            except Exception:
                pass
        print(
            "Improvement operation interrupted; explicit recovery required.",
            file=sys.stderr,
        )
        return 130
    except RevisionConflict as error:
        if hasattr(args, "id"):
            try:
                output = await service.status(args.id)
            except (ValueError, NoResultFound):
                output = {}
            output.update(
                {
                    "operation_blocked": True,
                    "reason": "retry_authorization_required"
                    if "trial retry requires" in str(error)
                    else "state_conflict",
                }
            )
            print(json.dumps(output, sort_keys=True))
        else:
            print("Improvement state conflicts with frozen inputs.", file=sys.stderr)
        return 2
    except (ManifestDrift, ValueError, KeyError, NoResultFound):
        print(
            "Improvement configuration or state conflicts with frozen inputs.",
            file=sys.stderr,
        )
        return 2
    except (RecoveryRequired, OSError, RuntimeError):
        print(
            "Improvement infrastructure or recovery requires attention.",
            file=sys.stderr,
        )
        return 3
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            signal.signal(sig, handler)
        await store.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_cli(sys.argv[1:])))
