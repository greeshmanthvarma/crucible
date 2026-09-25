"""Local, explicitly invoked evaluation commands."""

import argparse
import asyncio
import hashlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from alembic import command
from alembic.config import Config

from crucible.application.approval_service import public_command_spec
from crucible.application.container import ApplicationContainer
from crucible.application.ports import UnitOfWork
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.evals import EvalBudgets, EvalResult, ResultVerdict
from crucible.domain.ids import new_id
from crucible.engine.gateway import ModelGateway
from crucible.evals.evaluators import EvaluatorRegistry
from crucible.evals.fake_gateway import DeterministicEvalGateway
from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import (
    EvalPartition,
    EvalSuiteDefinition,
    load_case,
    load_suite,
)
from crucible.evals.reporting import EvalReporter, load_price_table
from crucible.evals.runner import (
    ApprovalChoice,
    EvalRunner,
    TrialRequest,
    configuration_digest,
)
from crucible.sandbox.docker_client import DockerClient


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("trial count must be positive") from error
    if number < 1:
        raise argparse.ArgumentTypeError("trial count must be positive")
    return number


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crucible")
    commands = parser.add_subparsers(dest="command", required=True)
    eval_command = commands.add_parser("eval")
    eval_commands = eval_command.add_subparsers(dest="eval_command", required=True)
    run = eval_commands.add_parser("run")
    run.add_argument("name")
    run.add_argument("--trials", type=_positive_int, default=1)
    run.add_argument(
        "--partition",
        choices=[item.value for item in EvalPartition],
        default=EvalPartition.DEVELOPMENT.value,
    )
    run.add_argument("--held-out-root", type=Path)
    run.add_argument("--development-root", type=Path)
    run.add_argument("--price-table", type=Path)
    run.add_argument("--data-dir", type=Path)
    run.add_argument("--deterministic", action="store_true")
    show = eval_commands.add_parser("show")
    show.add_argument("invocation_id", type=UUID)
    show.add_argument("--data-dir", type=Path)
    return parser


async def reconcile_incomplete_trials(
    unit_of_work: Callable[[], UnitOfWork],
) -> tuple[UUID, ...]:
    """After Run reconciliation, retain uncertain Trials as interrupted evidence."""
    async with unit_of_work() as uow:
        incomplete = await uow.evals.list_incomplete_trials()
        for trial in incomplete:
            await uow.evals.update_trial(trial.interrupt(datetime.now(UTC)))
            if await uow.evals.get_result(trial.id) is None:
                await uow.evals.add_result(
                    EvalResult(
                        trial.id,
                        ResultVerdict.ERROR,
                        (
                            {
                                "code": "process_restarted",
                                "detail": "Trial outcome is uncertain",
                            },
                        ),
                        None,
                        datetime.now(UTC),
                    )
                )
        await uow.commit()
    return tuple(item.id for item in incomplete)


async def _prompt_for_approval(approval: Approval) -> ApprovalChoice:
    print(
        json.dumps(
            {
                "approval_id": str(approval.id),
                "command": public_command_spec(approval),
                "spec_digest": approval.spec_digest,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    while True:
        answer = (
            input("Approve this exact command? Type approve or deny: ").strip().lower()
        )
        if answer == "approve":
            return ApprovalChoice(ApprovalStatus.APPROVED, None, str(new_id()))
        if answer == "deny":
            reason = input("Denial reason (optional): ").strip() or None
            return ApprovalChoice(ApprovalStatus.DENIED, reason, str(new_id()))
        print("Type approve or deny; there is no default decision.", file=sys.stderr)


def _paths(data_dir_option: Path | None) -> tuple[Path, str]:
    data_dir = (
        (data_dir_option or Path(os.environ.get("CRUCIBLE_DATA_DIR", "data")))
        .expanduser()
        .resolve()
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    if data_dir_option is not None:
        database_url = f"sqlite+aiosqlite:///{data_dir / 'crucible.db'}"
    else:
        database_url = os.environ.get(
            "CRUCIBLE_DATABASE_URL", "sqlite+aiosqlite:///crucible.db"
        )
    return data_dir, database_url


async def _open(
    database_url: str,
    data_dir: Path,
    *,
    model_id: str | None = None,
    gateway_factory: Callable[[], ModelGateway] | None = None,
    docker_client: DockerClient | None = None,
    run_budgets: EvalBudgets | None = None,
    context_limits: tuple[int, int] | None = None,
) -> ApplicationContainer:
    config = Config(Path(__file__).parents[3] / "alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    return await ApplicationContainer.create(
        database_url,
        data_dir,
        model_id=model_id,
        gateway_factory=gateway_factory,
        docker_client=docker_client,
        run_budgets=run_budgets,
        context_limits=context_limits,
    )


async def _trial_row(
    container: ApplicationContainer, trial_id: UUID, data_dir: Path
) -> dict[str, object]:
    async with container.unit_of_work() as uow:
        trial = await uow.evals.get_trial(trial_id)
        result = await uow.evals.get_result(trial_id)
    if trial is None:
        raise ValueError("Trial disappeared")
    report_path = None
    if result is not None and result.report_artifact_id is not None:
        artifact = await container.artifact_service.get(result.report_artifact_id)
        report_path = str(data_dir / "artifacts" / artifact.storage_identity)
    return {
        "trial_id": str(trial.id),
        "repeat_index": trial.repeat_index,
        "status": trial.status.value,
        "verdict": result.verdict.value if result else None,
        "failure_code": trial.failure_code,
        "task_id": str(trial.task_id) if trial.task_id else None,
        "run_id": str(trial.run_id) if trial.run_id else None,
        "report_artifact_id": str(result.report_artifact_id)
        if result and result.report_artifact_id
        else None,
        "report_path": report_path,
    }


async def run_cli(
    argv: Sequence[str],
    *,
    gateway_factory: Callable[[], ModelGateway] | None = None,
    docker_client: DockerClient | None = None,
) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    data_dir, database_url = _paths(args.data_dir)
    if args.eval_command == "show":
        container = await _open(database_url, data_dir, docker_client=docker_client)
        try:
            async with container.unit_of_work() as uow:
                trials = await uow.evals.list_trials(args.invocation_id)
            if not trials:
                print("Invocation not found", file=sys.stderr)
                return 2
            rows = [await _trial_row(container, item.id, data_dir) for item in trials]
            print(
                json.dumps(
                    {"invocation_id": str(args.invocation_id), "trials": rows},
                    sort_keys=True,
                )
            )
            return 0
        finally:
            await container.close()
    partition = EvalPartition(args.partition)
    if args.deterministic and partition is not EvalPartition.DEVELOPMENT:
        parser.error("--deterministic is only available for development evaluations")
    if partition is EvalPartition.HELD_OUT and args.held_out_root is None:
        parser.error("held-out execution requires --held-out-root")
    if partition is EvalPartition.DEVELOPMENT and args.held_out_root is not None:
        parser.error("--held-out-root requires --partition held-out")
    root = (
        args.held_out_root
        if partition is EvalPartition.HELD_OUT
        else args.development_root
    )
    try:
        suite = load_suite(partition, args.name, root=root)
    except FileNotFoundError:
        try:
            case = load_case(partition, args.name, root=root)
        except FileNotFoundError:
            print(f"Unknown Eval Suite or Case: {args.name}", file=sys.stderr)
            return 2
        suite_id = f"case-{case.case_id}"
        digest = hashlib.sha256(
            f"case-suite-v1:{case.case_digest}".encode()
        ).hexdigest()
        suite = EvalSuiteDefinition(suite_id, (case,), digest, partition)
    if args.deterministic and any(case.model != "fake" for case in suite.cases):
        parser.error("--deterministic requires Cases using the fake model")
    gateway_version = "deterministic-eval-v1" if args.deterministic else "default"
    effective_gateway_factory = (
        gateway_factory
        if gateway_factory is not None
        else DeterministicEvalGateway
        if args.deterministic
        else None
    )
    price_table = load_price_table(args.price_table) if args.price_table else None
    invocation_id = new_id()
    run_rows: list[dict[str, object]] = []
    failed = False
    for case in suite.cases:
        container = await _open(
            database_url,
            data_dir,
            model_id=case.model,
            run_budgets=case.budgets,
            context_limits=(
                case.repository_settings.model_input_limit,
                case.repository_settings.model_output_reserve,
            ),
            gateway_factory=effective_gateway_factory,
            docker_client=docker_client,
        )
        try:
            async with container.unit_of_work() as uow:
                uncertain = await uow.evals.list_incomplete_trials()
            for trial in uncertain:
                if trial.run_id is not None:
                    await container.supervisor.cancel(trial.run_id)
            await reconcile_incomplete_trials(container.unit_of_work)
            await container.start()
            selected_root = (
                root or Path(__file__).parents[4] / "evals" / partition.value
            )
            runner = EvalRunner(
                container,
                suite,
                FixturePreparer(data_dir),
                EvaluatorRegistry(
                    data_dir=data_dir,
                    artifacts=container.artifact_service,
                    partition_root=selected_root,
                    sandbox=container.eval_sandbox,
                ),
                _prompt_for_approval,
                EvalReporter(
                    container.unit_of_work, container.artifact_service, price_table
                ),
                gateway_version,
            )
            for repeat_index in range(1, args.trials + 1):
                result = await runner.run_trial(
                    TrialRequest(
                        suite.suite_id,
                        case.case_id,
                        repeat_index,
                        partition,
                        configuration_digest(case, gateway_version),
                        invocation_id,
                    )
                )
                run_rows.append(await _trial_row(container, result.trial_id, data_dir))
                failed |= result.verdict is not ResultVerdict.PASSED
        finally:
            await container.close()
    print(
        json.dumps(
            {"invocation_id": str(invocation_id), "trials": run_rows}, sort_keys=True
        )
    )
    return 1 if failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(run_cli(sys.argv[1:] if argv is None else argv))
    except KeyboardInterrupt:
        print(
            "Evaluation interrupted; retained Trial evidence can be "
            "inspected with eval show.",
            file=sys.stderr,
        )
        return 130
