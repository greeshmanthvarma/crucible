"""Opt-in, model-free source and native dependency checks in admitted images."""

import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from crucible.benchmarks.manifests import export_solver_case, load_case
from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
from crucible.improvement.broker import WorkspaceBinding
from crucible.improvement.worker_supervisor import (
    DockerCommands,
    DockerRuntime,
    labels,
    remove_owned_effect,
)
from tests.contract.test_improvement_worker import make_job


@pytest.mark.docker
@pytest.mark.parametrize(
    "case_id,package,package_path,native",
    [
        ("sympy__sympy-18199", "sympy", "sympy", "mpmath"),
        (
            "scikit-learn__scikit-learn-12682",
            "sklearn",
            "sklearn",
            "sklearn.__check_build._check_build",
        ),
        ("sphinx-doc__sphinx-10323", "sphinx", "sphinx", "docutils"),
        ("pytest-dev__pytest-10051", "_pytest", "src/_pytest", "pluggy"),
        ("astropy__astropy-13579", "astropy", "astropy", "astropy.wcs._wcs"),
    ],
)
async def test_edited_workspace_runs_in_admitted_image(
    tmp_path, case_id, package, package_path, native
):
    registry_path = os.environ.get("CRUCIBLE_TASK_CASE_REGISTRY")
    if not registry_path:
        pytest.skip("CRUCIBLE_TASK_CASE_REGISTRY is not configured")
    registry = json.loads(Path(registry_path).read_text())
    case = load_case(Path(registry["cases"][case_id]["case_manifest"]))
    view = export_solver_case(case, tmp_path / "workspace")
    workspace = view.workspace
    marker = workspace / package_path / "_crucible_probe.py"
    marker.write_text("VALUE = 'edited-workspace'\n")
    probe = (
        f"import {package}\n"
        f"from {package}._crucible_probe import VALUE\n"
        f"import {native}\n"
        "def test_workspace_source():\n"
        "    assert VALUE == 'edited-workspace'\n"
        f"    assert {package}.__file__.startswith('/workspace/')\n"
    )
    if package == "sympy":
        test_path = "sympy/ntheory/tests/test_crucible_env.py"
        args = ("bin/test", "-C", "--verbose", test_path)
    else:
        directory = {
            "sklearn": "sklearn/decomposition/tests",
            "sphinx": "tests",
            "_pytest": "testing/logging",
            "astropy": "astropy/wcs/wcsapi/wrappers/tests",
        }[package]
        test_path = directory + "/test_crucible_env.py"
        args = ("-q", test_path)
    (workspace / test_path).write_text(probe)
    metadata = {
        p.relative_to(workspace / ".git"): p.read_bytes()
        for p in (workspace / ".git").rglob("*")
        if p.is_file()
    }

    class Store:
        async def begin_effect(self, *args):
            return uuid4()

        async def effect_state(self, *args):
            pass

    job = make_job()
    job = job.model_copy(
        update={
            "solver_case": job.solver_case.model_copy(
                update={"image": case.image, "platform": case.platform}
            )
        }
    )
    spec = CommandSpec(
        "python" if package == "sympy" else "pytest",
        args,
        ".",
        60,
        CommandNetwork.NONE,
        {},
        case.image,
        "Verify edited workspace and native task dependencies",
        CommandLimits(2, 2 * 1024**3, 128, 8192),
    )
    commands = DockerCommands(Store(), DockerRuntime())
    try:
        chunks, outcome = await commands.execute(
            job, WorkspaceBinding(workspace, case.image, case.platform), uuid4(), spec
        )
    finally:
        for _, name, _ in commands.active.get(job.job_id, []):
            await remove_owned_effect(commands.docker, "command", name, labels(job))
    output = b"".join(c.data for c in chunks).decode()
    assert outcome.exit_code == 0, output
    assert "1 passed" in output, output
    assert metadata == {
        p.relative_to(workspace / ".git"): p.read_bytes()
        for p in (workspace / ".git").rglob("*")
        if p.is_file()
    }

    untracked = subprocess.check_output(
        [
            "git",
            "-C",
            str(workspace),
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        ]
    )
    assert set(filter(None, untracked.decode().split("\0"))) == {
        marker.relative_to(workspace).as_posix(),
        test_path,
    }
