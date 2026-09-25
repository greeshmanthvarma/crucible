import subprocess
from datetime import UTC, datetime
from pathlib import Path

from crucible.domain.ids import new_id
from crucible.evals.evaluators import EvaluationContext, EvaluatorRegistry
from crucible.evals.manifests import EvaluatorDefinition
from crucible.sandbox.protocol import (
    OutputChunk,
    OutputStream,
    SandboxOutcome,
    SandboxTermination,
)


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


async def test_property_evaluators_inspect_resulting_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _git(workspace, "init", "-q")
    (workspace / "README.md").write_text("base\n")
    _git(workspace, "add", "README.md")
    _git(
        workspace,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        "base",
    )
    (workspace / "answer.py").write_text("def answer():\n    return 42\n")
    registry = EvaluatorRegistry(data_dir=tmp_path / "data")
    context = EvaluationContext(new_id(), new_id(), workspace, ())

    required = await registry.evaluate(
        context,
        EvaluatorDefinition("required_api", {"path": "answer.py", "symbol": "answer"}),
    )
    allowed = await registry.evaluate(
        context, EvaluatorDefinition("allowed_files", {"paths": ["answer.py"]})
    )
    forbidden = await registry.evaluate(
        context, EvaluatorDefinition("forbidden_file", {"path": "answer.py"})
    )
    assert required.verdict == "passed"
    assert allowed.verdict == "passed"
    assert forbidden.verdict == "failed"
    (workspace / "README.md").write_text("changed\n")
    changed = await registry.evaluate(
        context, EvaluatorDefinition("allowed_files", {"paths": ["answer.py"]})
    )
    assert changed.verdict == "failed"


async def test_hidden_check_uses_separate_snapshot_and_private_tests(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "answer.py").write_text("def answer(): return 42\n")
    partition = tmp_path / "partition"
    (partition / "tests").mkdir(parents=True)
    (partition / "tests" / "check.py").write_text("assert 42 == 42\n")
    sandbox = FakeEvalSandbox(exit_code=0)
    registry = EvaluatorRegistry(
        data_dir=tmp_path / "data", partition_root=partition, sandbox=sandbox
    )
    context = EvaluationContext(new_id(), new_id(), workspace, (), new_id())
    definition = EvaluatorDefinition(
        "hidden_command",
        {
            "test_path": "tests/check.py",
            "image": "python@sha256:" + "a" * 64,
            "executable": "python",
            "arguments": ["/tests/check.py"],
        },
    )
    outcome = await registry.evaluate(context, definition)
    assert outcome.verdict == "passed"
    assert sandbox.request is not None
    assert sandbox.request.source_path != workspace
    assert sandbox.request.tests_path == partition / "tests"
    assert not (workspace / "tests").exists()
    assert not (sandbox.request.source_path / "tests").exists()
    assert (
        sandbox.request.source_path / "answer.py"
    ).read_text() == "def answer(): return 42\n"


class FakeEvalSandbox:
    def __init__(
        self,
        exit_code: int | None,
        termination: SandboxTermination = SandboxTermination.COMPLETED,
    ) -> None:
        self.exit_code = exit_code
        self.termination = termination
        self.request = None

    async def evaluate(self, request, on_chunk):
        self.request = request
        await on_chunk(OutputChunk(OutputStream.STDOUT, 1, b"check output"))
        now = datetime.now(UTC)
        return SandboxOutcome(
            self.exit_code,
            self.termination,
            now,
            now,
            request.image,
            "container",
            12,
            12,
            False,
        )


async def test_hidden_check_failure_and_timeout_never_pass(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    partition = tmp_path / "partition"
    (partition / "tests").mkdir(parents=True)
    (partition / "tests" / "check.py").write_text("raise AssertionError\n")
    context = EvaluationContext(new_id(), new_id(), workspace, (), new_id())
    definition = EvaluatorDefinition(
        "hidden_command",
        {"test_path": "tests/check.py", "image": "python@sha256:" + "a" * 64},
    )
    failed = await EvaluatorRegistry(
        data_dir=tmp_path / "data",
        partition_root=partition,
        sandbox=FakeEvalSandbox(exit_code=1),
    ).evaluate(context, definition)
    timed_out = await EvaluatorRegistry(
        data_dir=tmp_path / "data",
        partition_root=partition,
        sandbox=FakeEvalSandbox(None, SandboxTermination.TIMED_OUT),
    ).evaluate(context, definition)
    assert failed.verdict == "failed"
    assert timed_out.verdict == "error"
    assert timed_out.code == "timed_out"
