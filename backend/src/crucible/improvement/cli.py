"""Explicit improvement commands. Nothing is scheduled or resumed implicitly."""

import argparse
import asyncio
import json
import signal
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

from sqlalchemy.exc import NoResultFound

from crucible.improvement.manifest import ManifestDrift
from crucible.improvement.store import BudgetDenied, ExperimentStore, RevisionConflict


def parser(*, require_state_root: bool = True) -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="crucible improve")
    command.add_argument("--state-root", type=Path, required=require_state_root)
    operations = command.add_subparsers(dest="operation", required=True)
    export = operations.add_parser(
        "evidence-export", help="offline development evidence export"
    )
    export.add_argument("--source-state-root", type=Path, required=True)
    export.add_argument("--experiment", type=UUID, required=True)
    export.add_argument("--snapshot", type=UUID, required=True)
    export.add_argument("--selection-file", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    verify = operations.add_parser(
        "evidence-verify", help="offline package verification"
    )
    verify.add_argument("--package", type=Path, required=True)
    preview = operations.add_parser(
        "evidence-preview", help="offline case evidence request preview"
    )
    preview.add_argument("--package", type=Path, required=True)
    preview.add_argument("--bundle-digest", required=True)
    preview.add_argument("--trial", required=True)
    preview.add_argument("--detail", action="append", default=[])
    case_mining = operations.add_parser(
        "case-mine", help="diagnose pinned development attempts and group tool failures"
    )
    case_mining.add_argument(
        "--package",
        action="append",
        nargs=2,
        metavar=("PATH", "BUNDLE_DIGEST"),
        required=True,
    )
    case_mining.add_argument("--model", required=True)
    case_mining.add_argument("--batch-token-cap", type=int, required=True)
    case_mining.add_argument("--authorize-paid-models", action="store_true")
    diagnostic = operations.add_parser("mining-diagnostic")
    controls = diagnostic.add_subparsers(dest="control", required=True)
    prepare = controls.add_parser("prepare")
    prepare.add_argument("--config", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    for control in (
        "status",
        "continue",
        "stop",
        "recover",
        "authorize-retry",
        "report",
        "close",
    ):
        item = controls.add_parser(control)
        item.add_argument("--root", type=Path, required=True)
        if control == "continue":
            item.add_argument("--authorize-paid-models", action="store_true")
        elif control == "authorize-retry":
            item.add_argument("--slot", type=UUID, required=True)
            item.add_argument("--failed-attempt", type=UUID, required=True)
        elif control == "report":
            item.add_argument("--output", type=Path, required=True)
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
    operation_index = 2 if arguments[:1] == ["--state-root"] else 0
    operation = next(iter(arguments[operation_index : operation_index + 1]), "")
    args = parser(
        require_state_root=operation
        not in {
            "case-mine",
            "evidence-export",
            "evidence-preview",
            "evidence-verify",
            "mining-diagnostic",
        }
    ).parse_args(arguments)
    if args.operation == "evidence-preview":
        return _run_case_preview(args)
    if args.operation == "case-mine":
        return await _run_case_mining(args)
    if args.operation == "mining-diagnostic":
        return await _run_mining_command(args)
    if args.operation in ("evidence-export", "evidence-verify"):
        from crucible.improvement.trajectory_export import run_evidence_command

        return run_evidence_command(args)
    from crucible.improvement.lifecycle import ExperimentService, RecoveryRequired

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


def _run_case_preview(args: argparse.Namespace) -> int:
    from crucible.improvement.case_evidence import (
        CasePacketError,
        assemble_case_packet,
        expand_case_packet,
    )
    from crucible.repair.identity import canonical_json

    try:
        try:
            trial_id = UUID(args.trial)
        except ValueError:
            raise CasePacketError("invalid_target") from None
        prepared = assemble_case_packet(
            args.package, trial_id, expected_bundle_digest=args.bundle_digest
        )
        followup = (
            expand_case_packet(prepared, tuple(args.detail)) if args.detail else None
        )
    except CasePacketError as error:
        print(f"Evidence preview rejected ({error.code}).", file=sys.stderr)
        return 2
    previews = (prepared.initial, followup) if followup else (prepared.initial,)
    print(
        canonical_json(
            {
                "packet": prepared.packet,
                "initial": prepared.initial,
                "followup": followup,
                "total_reserved_tokens": sum(
                    preview.input_tokens + preview.output_reserve_tokens
                    for preview in previews
                ),
            }
        ).decode()
    )
    return 0


async def _run_case_mining(args: argparse.Namespace) -> int:
    if not args.authorize_paid_models:
        print("Case mining requires --authorize-paid-models.", file=sys.stderr)
        return 2

    from collections.abc import AsyncIterator
    from contextlib import aclosing

    import litellm

    from crucible.engine.gateway import ModelStreamItem, PreparedModelRequest
    from crucible.improvement.case_mining import PinnedPackage, mine_development_cases
    from crucible.models.responses_gateway import LiteLLMResponsesGateway
    from crucible.repair.identity import canonical_json

    # LiteLLM can print provider help from background work after the report.
    litellm.suppress_debug_info = True
    upstream = LiteLLMResponsesGateway(disable_retries=True)

    class TimedGateway:
        async def stream(
            self, request: PreparedModelRequest
        ) -> AsyncIterator[ModelStreamItem]:
            async with asyncio.timeout(90):
                async with aclosing(upstream.stream(request)) as stream:
                    async for item in stream:
                        yield item

    try:
        report = await mine_development_cases(
            tuple(PinnedPackage(Path(path), pin) for path, pin in args.package),
            gateway=TimedGateway(),
            model=args.model,
            batch_token_cap=args.batch_token_cap,
        )
    except ValueError as error:
        print(f"Case mining rejected ({error}).", file=sys.stderr)
        return 2
    print(canonical_json(report).decode())
    return 0


async def _run_mining_command(args: argparse.Namespace) -> int:
    # This gate precedes store, runtime and provider construction.
    if args.control == "continue" and not args.authorize_paid_models:
        print(
            "Diagnostic continuation requires --authorize-paid-models.", file=sys.stderr
        )
        return 2
    from crucible.improvement.mining_contracts import (
        MiningDiagnosticConfig,
        PreparedMiningDiagnostic,
    )
    from crucible.improvement.mining_diagnostic import (
        MiningDiagnosticRunner,
        freeze_mining_diagnostic,
    )
    from crucible.improvement.mining_store import MiningDiagnosticStore
    from crucible.improvement.worker_supervisor import read_regular_artifact

    store = None
    runner = None
    id = None
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    previous = {}
    if current:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.getsignal(sig)
            loop.add_signal_handler(sig, current.cancel)
    try:
        if args.control == "prepare":
            config = MiningDiagnosticConfig.model_validate_json(
                read_regular_artifact(args.config, 1_048_576)
            )
            prepared = freeze_mining_diagnostic(config, args.output)
            store = await MiningDiagnosticStore.open(
                prepared.root / "accounting.sqlite3"
            )
            runner = MiningDiagnosticRunner(store, prepared.root, None)
            await runner.initialize(prepared.diagnostic_id)
            print(prepared.model_dump_json())
            return 0
        prepared = PreparedMiningDiagnostic.model_validate_json(
            read_regular_artifact(args.root / "prepared.json", 65536)
        )
        id = prepared.diagnostic_id
        path = args.root / "accounting.sqlite3"
        if not path.is_file():
            raise ValueError("diagnostic ledger does not exist")
        opener = (
            MiningDiagnosticStore.open_readonly
            if args.control in ("status", "report")
            else MiningDiagnosticStore.open
        )
        store = await opener(path)
        runner = MiningDiagnosticRunner(store, args.root, None)
        state = (
            await store.diagnostic_status(id)
            if args.control == "stop"
            else await runner.validate(id)
        )
        if args.control == "continue":
            if state.status not in ("ready", "running"):
                print(state.model_dump_json())
                return (
                    130
                    if state.status == "interrupted"
                    else 3
                    if state.status == "blocked"
                    else 0
                )
            # Check the loaded host controller BEFORE constructing any provider port.
            from crucible.domain.clock import SystemClock
            from crucible.improvement.contracts import EDITABLE_PATHS
            from crucible.improvement.runtime import execution_runtime
            from crucible.improvement.source import _digest, inspect_source
            from crucible.improvement.worker_supervisor import DockerRuntime
            from crucible.models.responses_gateway import LiteLLMResponsesGateway
            from crucible.repair.gateway import ScopedModelGateway

            manifest = (await store.get(id)).manifest
            source = inspect_source(Path(__file__).parents[4], manifest.initial_commit)
            if (
                source.changed_paths
                or source.source_digest != manifest.initial_source_digest
                or _digest(
                    {n: v for n, v in source.files.items() if n not in EDITABLE_PATHS}
                )
                != manifest.controller_digest
            ):
                raise ValueError(
                    "loaded controller differs from frozen executing source"
                )

            async def deny_command(job: object, spec: object) -> bool:
                return False

            gateway = ScopedModelGateway(
                LiteLLMResponsesGateway(disable_retries=True),
                clock=SystemClock(),
                endpoint="http://broker/internal/improvement/model",
            )
            async with execution_runtime(
                store, args.root, manifest, DockerRuntime(), gateway, deny_command
            ) as supervisor:
                runner = MiningDiagnosticRunner(store, args.root, supervisor)
                state = await runner.continue_run(id)
        elif args.control == "recover":
            state = await runner.recover(id)
        elif args.control == "stop":
            state = await runner.stop(id)
        elif args.control == "close":
            state = await runner.close(id)
        elif args.control == "authorize-retry":
            authorization = await runner.authorize_retry(
                id, args.slot, args.failed_attempt
            )
            print(json.dumps({"authorization_id": str(authorization)}, sort_keys=True))
            return 0
        elif args.control == "report":
            from crucible.improvement.mining_reporting import write_mining_report

            print(
                json.dumps(
                    await write_mining_report(store, id, args.output), sort_keys=True
                )
            )
            return 0
        print(state.model_dump_json())
        if args.control in ("status", "close"):
            return 0
        if args.control == "stop":
            return 0 if state.cleanup_confirmed else 3
        return (
            130
            if state.status == "interrupted"
            else 3
            if state.status == "blocked"
            else 0
        )
    except asyncio.CancelledError:
        if runner is not None and id is not None:
            await runner.stop(id)
        print("Diagnostic interrupted; explicit recovery required.", file=sys.stderr)
        return 130
    except BudgetDenied:
        print("Diagnostic budget or accounting blocks this operation.", file=sys.stderr)
        return 3
    except (
        ValueError,
        KeyError,
        NoResultFound,
        RevisionConflict,
        FileNotFoundError,
        sqlite3.Error,
    ):
        print(
            "Diagnostic configuration or state conflicts with frozen inputs.",
            file=sys.stderr,
        )
        return 2
    except (OSError, RuntimeError):
        print(
            "Diagnostic infrastructure or recovery requires attention.", file=sys.stderr
        )
        return 3
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            signal.signal(sig, handler)
        if store is not None:
            await store.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_cli(sys.argv[1:])))
