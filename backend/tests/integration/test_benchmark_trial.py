import asyncio
import json
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.application.container import ApplicationContainer
from crucible.benchmarks.contracts import (
    GraderConfig,
    GradeResult,
    TrialIdentity,
    TrialLimits,
)
from crucible.benchmarks.manifests import case_digest
from crucible.benchmarks.swebench import SwebenchGrader
from crucible.benchmarks.trials import (
    BenchmarkTrialRunner,
    BudgetedTrialGateway,
    solver_termination,
)
from crucible.domain.approvals import ApprovalStatus
from crucible.domain.evals import EvalBudgets
from crucible.domain.ids import new_id
from crucible.engine.gateway import (
    CompleteToolCall,
    ModelError,
    ModelStop,
    ModelStopReason,
)
from crucible.evals.runner import ApprovalChoice
from crucible.repair.identity import canonical_json, digest
from tests.integration.evals.test_runner import FakeDockerClient

pytest_plugins = ["tests.unit.test_benchmark_manifests"]


def test_budget_end_is_distinct_from_provider_error() -> None:
    assert solver_termination("failed", "budget_exhausted") == "budget_exhausted"
    assert solver_termination("failed", "provider_down") == "provider_error"
    assert solver_termination("completed", None) == "completed"


@pytest.mark.parametrize(
    "mode",
    [
        "budget",
        "provider",
        "complete",
        "admission_denied",
        "denied",
        "wall",
        "cleanup_error",
        "environment_mismatch",
    ],
)
async def test_ordinary_solver_then_independent_grade(
    case_fixture, database, database_url, tmp_path, monkeypatch, mode
):
    class Gateway:
        requests = []

        async def stream(self, request):
            self.requests.append(request)
            visible = canonical_json(request).decode()
            assert str(case_fixture.grader_input_path) not in visible
            assert "private-grader" not in visible
            if len(self.requests) == 1 and mode == "denied":
                yield CompleteToolCall(
                    new_id(),
                    "execute_command",
                    {
                        "executable": "python",
                        "arguments": ["-V"],
                        "reason": "Check interpreter",
                    },
                )
                yield ModelStop(ModelStopReason.TOOL_CALLS)
            elif len(self.requests) == 1 and mode != "complete":
                yield CompleteToolCall(
                    new_id(),
                    "write_file",
                    {"path": "answer.py", "content": "answer = 42\n"},
                )
                yield ModelStop(ModelStopReason.TOOL_CALLS)
            elif mode == "wall":
                await asyncio.Event().wait()
            elif mode == "provider":
                yield ModelError("provider_down", "unavailable")
            else:
                yield ModelStop(ModelStopReason.COMPLETE)

    gateway = Gateway()
    limits = TrialLimits(
        max_steps=1 if mode == "budget" else 2,
        max_tool_calls=5,
        max_model_tokens=100000,
        max_active_seconds=10,
        wall_timeout_seconds=1 if mode == "wall" else 20,
    )
    trial_id = uuid4()
    ledger = BudgetedTrialGateway(gateway, "test-model", limits, trial_id)
    data = tmp_path / "data"
    container = await ApplicationContainer.create(
        database_url,
        data,
        model_id="test-model",
        gateway_factory=lambda: ledger,
        docker_client=FakeDockerClient(),
        run_budgets=EvalBudgets(limits.max_steps, 5, 100000, 10),
        context_limits=(10000, 1000),
        sandbox_environment=(case_fixture.image, case_fixture.platform),
    )
    model = {
        "model_id": "test-model",
        "provider_defaults": "unspecified",
        "input_limit": 10000,
        "output_reserve": 1000,
    }
    identity = TrialIdentity(
        trial_id=trial_id,
        case_digest=case_digest(case_fixture),
        harness_commit=container.runtime_identity.commit,
        harness_tree_digest=container.runtime_identity.source_digest,
        model_config_digest=digest(model),
        limits_digest=digest(limits),
        repeat_index=0,
    )
    config = GraderConfig(
        driver_python=Path("/usr/bin/python3"),
        driver_source=tmp_path,
        driver_commit="a" * 40,
        driver_digest="b" * 64,
        timeout_seconds=10,
        memory_bytes=1024,
        cpus=1,
        output_limit_bytes=65536,
        network="none",
    )

    async def verified(*args):
        pass

    def admitted(*args):
        if mode == "admission_denied":
            raise ValueError("not admitted")

    monkeypatch.setattr("crucible.benchmarks.trials.verify_harness", verified)
    monkeypatch.setattr("crucible.benchmarks.trials.validate_admission", admitted)
    if mode == "cleanup_error":

        async def failed_cleanup(task_id):
            raise RuntimeError("cleanup failed")

        monkeypatch.setattr(
            container.command_sandbox, "stop_task_commands", failed_cleanup
        )
    if mode == "environment_mismatch":
        container.command_sandbox.repository_environment = (
            case_fixture.image,
            "linux/arm64",
        )
    grades = []

    async def grade(self, case, patch, trial_id, output_dir):
        grades.append(patch)
        if mode == "budget":
            assert "+answer = 42" in patch.path.read_text()
        assert not any(
            "private-grader" in str(p) for p in (data / "workspaces").rglob("*")
        )
        return GradeResult(
            "pass",
            "mocked-official",
            "evaluation",
            True,
            patch.sha256,
            output_dir / "report",
            output_dir / "log",
            "a" * 40,
            0,
        )

    monkeypatch.setattr(SwebenchGrader, "grade", grade)

    async def approval(approval):
        assert mode == "denied"
        return ApprovalChoice(ApprovalStatus.DENIED, "operator denied", "test-denial")

    runner = BenchmarkTrialRunner(
        container,
        SwebenchGrader(config),
        approval,
        data,
        admission_path=tmp_path / "admission.json",
        model_configuration=model,
        gateway_accounting=ledger,
    )
    try:
        if mode in {"admission_denied", "environment_mismatch"}:
            with pytest.raises(ValueError):
                await runner.run(case_fixture, identity, limits, tmp_path / "trial")
            assert not gateway.requests
            return
        result = await runner.run(case_fixture, identity, limits, tmp_path / "trial")
        assert result.solver is not None, result.error_code
        if mode in {"wall", "cleanup_error"}:
            assert result.status == "error"
            assert not grades
            assert result.solver.trace_path.exists()
            assert result.solver.usage_path.exists()
        elif mode == "provider":
            assert result.status == "error"
            assert result.solver.termination == "provider_error"
            assert not grades
            assert (result.solver.workspace / "answer.py").exists()
        else:
            assert result.status == "complete", result.error_code
            assert result.grade.verdict == "pass"
            assert result.solver.termination == (
                "budget_exhausted" if mode == "budget" else "completed"
            )
            assert json.loads(result.solver.usage_path.read_text())
            if mode == "denied":
                assert "approval_denied" in result.solver.trace_path.read_text()
            if mode == "complete":
                next_identity = identity.model_copy(
                    update={"trial_id": uuid4(), "repeat_index": 1}
                )
                next_ledger = BudgetedTrialGateway(
                    Gateway(), "test-model", limits, next_identity.trial_id
                )
                next_container = await ApplicationContainer.create(
                    database_url,
                    data,
                    model_id="test-model",
                    gateway_factory=lambda: next_ledger,
                    docker_client=FakeDockerClient(),
                    run_budgets=EvalBudgets(limits.max_steps, 5, 100000, 10),
                    context_limits=(10000, 1000),
                    sandbox_environment=(case_fixture.image, case_fixture.platform),
                )
                next_runner = BenchmarkTrialRunner(
                    next_container,
                    SwebenchGrader(config),
                    approval,
                    data,
                    admission_path=tmp_path / "admission.json",
                    model_configuration=model,
                    gateway_accounting=next_ledger,
                )
                try:
                    repeated = await next_runner.run(
                        case_fixture, next_identity, limits, tmp_path / "repeat"
                    )
                finally:
                    await next_container.close()
                assert repeated.status == "complete"
                assert repeated.solver.task_id != result.solver.task_id
                assert repeated.solver.workspace != result.solver.workspace
                assert (result.solver.workspace / ".git").is_dir()
                relocated = tmp_path / "relocated-testbed"
                shutil.copytree(result.solver.workspace, relocated)
                assert (
                    subprocess.check_output(
                        ["git", "-C", str(relocated), "rev-parse", "--git-common-dir"],
                        text=True,
                    ).strip()
                    == ".git"
                )
                subprocess.run(
                    ["git", "-C", str(relocated), "status", "--porcelain"],
                    check=True,
                    capture_output=True,
                )
    finally:
        await container.close()


async def test_solver_mounts_exclude_grader_inputs(database, tmp_path):
    from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
    from crucible.domain.tools import ToolCall, ToolExecutionMode
    from crucible.sandbox.docker import DockerSandboxBackend
    from crucible.sandbox.protocol import SandboxRequest
    from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
    from tests.contract.sandbox.test_docker_backend import CLOCK, NOW, FakeDocker
    from tests.integration.storage.test_tool_loop_storage import seed_exchange

    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, step, message = await seed_exchange(uow, "benchmark-platform")
        call_id = new_id()
        await uow.tool_calls.add(
            ToolCall(
                call_id,
                task.id,
                run.id,
                step.id,
                message.id,
                1,
                "execute_command",
                {},
                1,
                None,
                ToolExecutionMode.SEQUENTIAL,
                NOW,
            )
        )
        await uow.commit()
    image = "runner@sha256:" + "a" * 64
    docker = FakeDocker()
    backend = DockerSandboxBackend(
        docker,
        lambda: SqlAlchemyUnitOfWork(database),
        CLOCK,
        repository_environment=(image, "linux/amd64"),
    )
    command = CommandSpec(
        "python",
        ("-V",),
        ".",
        30,
        CommandNetwork.NONE,
        {},
        image,
        "Check environment",
        CommandLimits(1, 1024, 32, 1000),
    )
    workspace = tmp_path / "workspace"
    (workspace / ".git").mkdir(parents=True)
    await backend.execute(
        SandboxRequest(task.id, run.id, call_id, workspace, command, ()),
        lambda chunk: None,
    )
    args = docker.create_args
    assert args[args.index("--platform") + 1] == "linux/amd64"
    assert args[args.index("--workdir") + 1] == "/testbed"
    assert [args[i + 1] for i, arg in enumerate(args) if arg == "--mount"] == [
        f"type=bind,src={workspace},dst=/testbed",
        f"type=bind,src={workspace / '.git'},dst=/testbed/.git,readonly",
    ]
    assert not any(arg.startswith("PATH=") for arg in args)
    assert "--user" not in args


async def test_quiescence_removes_only_owned_task_containers(database):
    from crucible.domain.resources import ExternalResource
    from crucible.domain.tools import ToolCall, ToolExecutionMode
    from crucible.sandbox.docker import DockerSandboxBackend
    from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
    from tests.contract.sandbox.test_docker_backend import CLOCK, NOW, FakeDocker
    from tests.integration.storage.test_tool_loop_storage import seed_exchange

    tasks = []
    resources = []
    async with SqlAlchemyUnitOfWork(database) as uow:
        for label in ["owned", "unrelated"]:
            task, run, step, message = await seed_exchange(uow, "quiesce-" + label)
            call = new_id()
            await uow.tool_calls.add(
                ToolCall(
                    call,
                    task.id,
                    run.id,
                    step.id,
                    message.id,
                    1,
                    "execute_command",
                    {},
                    1,
                    None,
                    ToolExecutionMode.SEQUENTIAL,
                    NOW,
                )
            )
            resource = ExternalResource.container(
                new_id(),
                task.id,
                run.id,
                call,
                label + "-container",
                NOW,
                labels={},
                metadata={},
            )
            await uow.external_resources.add(resource)
            tasks.append(task)
            resources.append(resource)
        await uow.commit()
    docker = FakeDocker()
    backend = DockerSandboxBackend(
        docker, lambda: SqlAlchemyUnitOfWork(database), CLOCK
    )
    await backend.stop_task_commands(tasks[0].id)
    assert ("remove", "owned-container") in docker.cleaned
    assert not any(identity == "unrelated-container" for _, identity in docker.cleaned)
    async with SqlAlchemyUnitOfWork(database) as uow:
        unrelated = await uow.external_resources.get(resources[1].id)
    assert unrelated.status.value == "active"
