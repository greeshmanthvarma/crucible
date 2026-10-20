"""Explicit single-calibration commands; no optimization, retry or resume."""

import argparse
import asyncio
import json
import os
import signal
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Literal
from uuid import uuid4

from alembic import command
from alembic.config import Config
from pydantic import BaseModel, ConfigDict, field_validator

from crucible.application.approval_service import public_command_spec
from crucible.application.container import ApplicationContainer
from crucible.benchmarks.admission import (
    AdmissionConfig,
    run_admission_config,
    validate_admission,
)
from crucible.benchmarks.contracts import (
    BenchmarkCase,
    GraderConfig,
    ModelSettings,
    TrialIdentity,
    TrialLimits,
    TrialResult,
)
from crucible.benchmarks.evidence import TrialEvidence
from crucible.benchmarks.manifests import _git, case_digest, load_case
from crucible.benchmarks.swebench import SwebenchGrader
from crucible.benchmarks.trials import BenchmarkTrialRunner, BudgetedTrialGateway
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.evals import EvalBudgets
from crucible.evals.runner import ApprovalChoice
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.repair.identity import capture_runtime_identity, digest


class TrialConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[1]
    authorization: Literal["one-baseline-calibration"]
    approval_mode: Literal["interactive"]
    case_manifest: Path
    grader: GraderConfig
    admission_record: Path
    identity: TrialIdentity
    limits: TrialLimits
    model: ModelSettings
    data_dir: Path

    @field_validator("case_manifest", "admission_record", "data_dir")
    @classmethod
    def local_path(cls, value: Path) -> Path:
        if (
            not value.is_absolute()
            or ".." in value.parts
            or any(p.is_symlink() for p in [value, *value.parents])
        ):
            raise ValueError("absolute local paths without symlinks required")
        return value


def _load_config(path: Path) -> bytes:
    if path.is_symlink() or path.stat().st_size > 1_048_576:
        raise ValueError("invalid or oversized benchmark configuration")
    return path.read_bytes()


def validate_controller_layout(
    output: Path,
    case: BenchmarkCase,
    grader: GraderConfig,
    *,
    protected: tuple[Path, ...] = (),
    write_roots: tuple[Path, ...] = (),
) -> None:
    roots = (output, *write_roots)
    inputs = (
        case.fixture_path,
        case.prompt_path,
        case.grader_input_path,
        grader.driver_source,
        grader.driver_python,
        *protected,
    )
    for root in roots:
        if (
            not root.is_absolute()
            or ".." in root.parts
            or any(p.is_symlink() for p in [root, *root.parents])
        ):
            raise ValueError("invalid controller output layout")
        for source in inputs:
            if root.resolve().is_relative_to(
                source.resolve()
            ) or source.resolve().is_relative_to(root.resolve()):
                raise ValueError("controller output overlaps protected inputs")
    for other in write_roots:
        if output.resolve().is_relative_to(
            other.resolve()
        ) or other.resolve().is_relative_to(output.resolve()):
            raise ValueError("output overlaps private runtime or identity registry")


async def _operator_line() -> str:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[str] = loop.create_future()
    descriptor = sys.stdin.fileno()
    content = bytearray()

    def readable() -> None:
        try:
            data = os.read(descriptor, 1)
            if not data:
                future.set_exception(EOFError("operator input closed"))
            elif data == b"\n":
                future.set_result(content.decode("utf-8"))
            else:
                content.extend(data)
                if len(content) > 1024:
                    future.set_exception(ValueError("operator response too long"))
        except Exception as error:
            if not future.done():
                future.set_exception(error)
        if future.done():
            loop.remove_reader(descriptor)

    loop.add_reader(descriptor, readable)
    try:
        return await future
    finally:
        loop.remove_reader(descriptor)


async def _prompt_for_approval(approval: Approval) -> ApprovalChoice:
    print(
        json.dumps(
            {
                "approval_id": str(approval.id),
                "spec_digest": approval.spec_digest,
                "command": public_command_spec(approval),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    while True:
        print("Approve this exact command? Type approve or deny: ", end="", flush=True)
        answer = (await _operator_line()).strip().lower()
        if answer in {"approve", "deny"}:
            return ApprovalChoice(
                ApprovalStatus.APPROVED
                if answer == "approve"
                else ApprovalStatus.DENIED,
                "benchmark operator decision",
                str(uuid4()),
            )


async def run_trial_config(path: Path, output: Path) -> TrialResult:
    config = TrialConfig.model_validate_json(_load_config(path))
    case = load_case(config.case_manifest)
    validate_controller_layout(
        output,
        case,
        config.grader,
        protected=(config.admission_record,),
        write_roots=(
            config.data_dir / "benchmark-identities",
            config.data_dir / "benchmark-runtime" / str(config.identity.trial_id),
        ),
    )
    if case.split != "calibration":
        raise ValueError(
            "foundation external trials require a non-suite calibration case"
        )
    validate_admission(config.admission_record, case, config.grader)
    current = await capture_runtime_identity(Path(__file__).parents[4])
    if (
        _git(Path(__file__).parents[4], "status", "--porcelain")
        or current.commit != config.identity.harness_commit
        or current.source_digest != config.identity.harness_tree_digest
    ):
        raise ValueError("current source differs from requested baseline")
    if case_digest(case) != config.identity.case_digest:
        raise ValueError("case fingerprint mismatch")
    if (
        digest(config.model.model_dump()) != config.identity.model_config_digest
        or digest(config.limits) != config.identity.limits_digest
    ):
        raise ValueError("model or limits fingerprint mismatch")
    if config.model.output_reserve >= config.model.input_limit:
        raise ValueError("invalid context limits")
    evidence = TrialEvidence(
        output, {"operation": "trial", **config.model_dump(mode="json")}
    )
    container: ApplicationContainer | None = None
    try:
        registry = config.data_dir / "benchmark-identities"
        registry.mkdir(parents=True, exist_ok=True, mode=0o700)
        if any(p.is_symlink() for p in [registry, *registry.parents]):
            raise ValueError("invalid identity registry")
        identity_path = registry / f"{config.identity.trial_id}.json"
        with identity_path.open("x") as stream:
            json.dump(
                {"trial_id": str(config.identity.trial_id), "output": str(output)},
                stream,
            )
            stream.flush()
            os.fsync(stream.fileno())
        runtime = config.data_dir / "benchmark-runtime" / str(config.identity.trial_id)
        if any(p.is_symlink() for p in [runtime, *runtime.parents]):
            raise ValueError("invalid private runtime root")
        runtime.mkdir(parents=True, exist_ok=False, mode=0o700)
        database_url = f"sqlite+aiosqlite:///{runtime / 'crucible.db'}"
        backend = Path(__file__).parents[3]
        migration = Config(str(backend / "alembic.ini"))
        migration.set_main_option("script_location", str(backend / "migrations"))
        migration.set_main_option("sqlalchemy.url", database_url)
        await asyncio.to_thread(command.upgrade, migration, "head")
        limits = config.limits
        gateway = BudgetedTrialGateway(
            LiteLLMResponsesGateway(),
            config.model.model_id,
            config.limits,
            config.identity.trial_id,
        )
        container = await ApplicationContainer.create(
            database_url,
            runtime,
            model_id=config.model.model_id,
            gateway_factory=lambda: gateway,
            run_budgets=EvalBudgets(
                limits.max_steps,
                limits.max_tool_calls,
                limits.max_model_tokens,
                limits.max_active_seconds,
            ),
            context_limits=(config.model.input_limit, config.model.output_reserve),
            sandbox_environment=(case.image, case.platform),
        )

        runner = BenchmarkTrialRunner(
            container,
            SwebenchGrader(config.grader),
            _prompt_for_approval,
            runtime,
            admission_path=config.admission_record,
            model_configuration=config.model.model_dump(),
            stage_callback=evidence.stage,
            gateway_accounting=gateway,
        )
        result = await runner.run(case, config.identity, limits, output / "execution")
        evidence.finish(result, result.status)
        return result
    except asyncio.CancelledError:
        evidence.finish(
            {
                "status": "interrupted",
                "identity": config.identity,
                "error_code": "operator_cancelled",
            },
            "interrupted",
        )
        raise
    except BaseException as error:
        evidence.finish(
            {
                "status": "error",
                "identity": config.identity,
                "error_code": type(error).__name__,
            },
            "error",
        )
        raise
    finally:
        if container is not None:
            await container.close()


async def run_cli(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="crucible benchmark")
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("admit", "trial", "isolated-trial"):
        sub = commands.add_parser(operation)
        sub.add_argument("--config", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
    show = commands.add_parser("show")
    show.add_argument("path", type=Path)
    args = parser.parse_args(argv)
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGTERM)
    if task:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        if args.operation == "show":
            print(json.dumps(TrialEvidence.show(args.path), sort_keys=True))
            return 0
        if args.operation == "admit":
            config = AdmissionConfig.model_validate_json(_load_config(args.config))
            case = load_case(config.case_manifest)
            validate_controller_layout(
                args.output, case, config.grader, protected=(config.reference_patch,)
            )
            evidence = TrialEvidence(
                args.output, {"operation": "admit", **config.model_dump(mode="json")}
            )
            evidence.stage("grading")
            try:
                result = await run_admission_config(
                    args.config, args.output / "admission"
                )
                evidence.finish(
                    {
                        "status": "complete"
                        if result.status == "admitted"
                        else "error",
                        "admission_status": result.status,
                        "code": result.code,
                    },
                    "complete" if result.status == "admitted" else "error",
                )
                print(json.dumps({"status": result.status, "code": result.code}))
                return (
                    0
                    if result.status == "admitted"
                    else 2
                    if result.status == "rejected"
                    else 3
                )
            except asyncio.CancelledError:
                evidence.finish(
                    {"status": "interrupted", "error_code": "operator_cancelled"},
                    "interrupted",
                )
                raise
            except Exception as error:
                evidence.finish(
                    {"status": "error", "error_code": type(error).__name__}, "error"
                )
                raise
        if args.operation == "isolated-trial":
            from crucible.improvement.calibration import run_isolated_calibration

            trial = await run_isolated_calibration(args.config, args.output)
        else:
            trial = await run_trial_config(args.config, args.output)
        print(json.dumps(TrialEvidence.show(args.output), sort_keys=True))
        return (
            0
            if trial.status == "complete"
            else 130
            if trial.status == "interrupted"
            else 3
        )
    except asyncio.CancelledError:
        print("Benchmark interrupted; evidence retained.", file=sys.stderr)
        return 130
    except (ValueError, FileExistsError, FileNotFoundError):
        print(
            "Invalid benchmark input, admission, or existing output; "
            "no implicit retry.",
            file=sys.stderr,
        )
        return 2
    except Exception:
        print(
            "Benchmark infrastructure error; evidence retained when prepared.",
            file=sys.stderr,
        )
        return 3
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        signal.signal(signal.SIGTERM, previous)
