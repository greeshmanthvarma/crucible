"""Independent property and private behavior checks after a terminal Run."""

import ast
import hashlib
import json
import os
import shutil
import subprocess
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.domain.evals import ResultVerdict
from crucible.domain.ids import new_id
from crucible.evals.manifests import EvaluatorDefinition
from crucible.sandbox.protocol import (
    EvalSandboxBackend,
    EvalSandboxRequest,
    OutputChunk,
    SandboxTermination,
)


@dataclass(frozen=True)
class EvaluationContext:
    task_id: UUID
    run_id: UUID
    workspace_path: Path
    evidence_ids: tuple[UUID, ...]
    trial_id: UUID | None = None


@dataclass(frozen=True)
class EvaluatorOutcome:
    verdict: ResultVerdict
    code: str
    evidence_artifact_id: UUID | None
    detail: str


class EvaluatorRegistry:
    def __init__(
        self,
        *,
        data_dir: Path,
        artifacts: ArtifactService | None = None,
        partition_root: Path | None = None,
        sandbox: EvalSandboxBackend | None = None,
    ) -> None:
        self._data_dir = data_dir
        self._artifacts = artifacts
        self._partition_root = partition_root
        self._sandbox = sandbox

    async def evaluate(
        self, context: EvaluationContext, definition: EvaluatorDefinition
    ) -> EvaluatorOutcome:
        try:
            if definition.kind == "hidden_command":
                verdict, code, detail, payload = await self._hidden(context, definition)
            else:
                verdict, code, detail = self._property(
                    context.workspace_path, definition
                )
                payload = json.dumps(
                    {"verdict": verdict.value, "code": code, "detail": detail},
                    sort_keys=True,
                ).encode()
        except (
            OSError,
            ValueError,
            SyntaxError,
            subprocess.CalledProcessError,
        ) as error:
            verdict, code, detail = ResultVerdict.ERROR, "evaluator_error", str(error)
            payload = json.dumps({"code": code, "detail": detail}).encode()
        evidence = None
        if self._artifacts is not None:

            async def chunks() -> AsyncIterator[bytes]:
                yield payload

            artifact = await self._artifacts.put(
                context.task_id, "application/json", "private", chunks()
            )
            evidence = artifact.id
        return EvaluatorOutcome(verdict, code, evidence, detail[:1000])

    async def _hidden(
        self, context: EvaluationContext, definition: EvaluatorDefinition
    ) -> tuple[ResultVerdict, str, str, bytes]:
        if (
            self._partition_root is None
            or self._sandbox is None
            or context.trial_id is None
        ):
            raise ValueError(
                "hidden evaluator requires a protected partition and sandbox"
            )
        relative = str(definition.options["test_path"])
        pure = PurePosixPath(relative)
        if pure.is_absolute() or pure.parts[:1] != ("tests",) or ".." in pure.parts:
            raise ValueError("hidden test path is outside protected tests")
        hidden = self._partition_root / relative
        if hidden.is_symlink() or not hidden.resolve().is_relative_to(
            self._partition_root.resolve()
        ):
            raise ValueError("hidden test path escapes partition")
        if not hidden.is_file():
            raise ValueError("hidden test file is missing")
        if definition.content_digest is not None:
            if (
                hashlib.sha256(hidden.read_bytes()).hexdigest()
                != definition.content_digest
            ):
                raise ValueError("hidden evaluator changed after manifest load")
        snapshot = self._snapshot(context)
        options = definition.options
        arguments = options.get("arguments", [f"/tests/{pure.relative_to('tests')}"])
        if not isinstance(arguments, list):
            raise ValueError("hidden evaluator arguments must be a list")
        request = EvalSandboxRequest(
            context.task_id,
            context.run_id,
            context.trial_id,
            snapshot,
            self._partition_root / "tests",
            str(options["image"]),
            str(options.get("executable", "python")),
            tuple(str(item) for item in arguments),
            int(options.get("timeout_seconds", 30)),
            int(options.get("output_bytes", 65_536)),
        )
        chunks: list[bytes] = []

        async def collect(chunk: OutputChunk) -> None:
            chunks.append(chunk.data)

        outcome = await self._sandbox.evaluate(request, collect)
        output = b"".join(chunks)
        detail = output.decode(errors="replace")[:1000]
        if outcome.termination is not SandboxTermination.COMPLETED:
            return ResultVerdict.ERROR, outcome.termination.value, detail, output
        verdict = (
            ResultVerdict.PASSED if outcome.exit_code == 0 else ResultVerdict.FAILED
        )
        code = "hidden_passed" if verdict is ResultVerdict.PASSED else "hidden_failed"
        return verdict, code, detail, output

    def _snapshot(self, context: EvaluationContext) -> Path:
        assert context.trial_id is not None
        destination = (
            self._data_dir / "eval-snapshots" / str(context.trial_id) / str(new_id())
        )
        for directory, names, files in os.walk(context.workspace_path):
            names[:] = [name for name in names if name != ".git"]
            if any((Path(directory) / name).is_symlink() for name in (*names, *files)):
                raise ValueError("Workspace symlink cannot enter evaluator snapshot")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            context.workspace_path, destination, ignore=shutil.ignore_patterns(".git")
        )
        return destination

    def _property(
        self, workspace: Path, definition: EvaluatorDefinition
    ) -> tuple[ResultVerdict, str, str]:
        options = definition.options
        kind = definition.kind
        if kind in {"required_file", "forbidden_file", "required_api"}:
            path = _workspace_file(workspace, str(options["path"]))
            exists = path.is_file()
            if kind == "required_file":
                contains = options.get("contains")
                passed = exists and (
                    contains is None or str(contains) in path.read_text()
                )
            elif kind == "forbidden_file":
                passed = not exists
            else:
                if not exists:
                    passed = False
                else:
                    syntax = ast.parse(path.read_text())
                    symbol = str(options["symbol"])
                    passed = any(
                        isinstance(
                            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                        )
                        and node.name == symbol
                        for node in syntax.body
                    )
            return (
                (ResultVerdict.PASSED if passed else ResultVerdict.FAILED),
                "property_passed" if passed else "property_failed",
                f"{kind}: {options.get('path')}",
            )
        if kind in {"allowed_files", "diff_constraints"}:
            changed = _changed_files(workspace)
            if kind == "allowed_files":
                allowed = {str(item) for item in options["paths"]}
                passed = changed <= allowed
            else:
                max_files = int(options.get("max_files", 0))
                passed = len(changed) <= max_files
            return (
                ResultVerdict.PASSED if passed else ResultVerdict.FAILED,
                "property_passed" if passed else "property_failed",
                f"changed files: {', '.join(sorted(changed))}",
            )
        raise ValueError(f"unsupported evaluator kind: {kind}")


def _workspace_file(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts or "\\" in relative:
        raise ValueError("evaluator path must be Workspace-relative")
    path = root / relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("evaluator path escapes Workspace")
    return path


def _changed_files(root: Path) -> set[str]:
    changed = subprocess.check_output(
        ["git", "-C", str(root), "diff", "--name-only", "HEAD"], text=True
    ).splitlines()
    untracked = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--others", "--exclude-standard"],
        text=True,
    ).splitlines()
    return set(changed + untracked)
