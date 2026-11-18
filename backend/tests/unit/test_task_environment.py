"""Runtime hydration may add dependencies, never rewrite solver source or Git."""

import subprocess

import pytest


def git(root, *args):
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True
    ).stdout


@pytest.fixture
def roots(tmp_path):
    image, workspace = tmp_path / "image", tmp_path / "workspace"
    for root in (image, workspace):
        root.mkdir()
        git(root, "init", "-q")
        (root / "pkg").mkdir()
        (root / ".gitignore").write_text("*.so\n*_version.py\n*.egg-info/\n*.c\n")
        (root / "pkg/source.py").write_text("original")
        git(root, "add", ".")
    return image, workspace


def test_restores_ignored_runtime_files_without_changing_existing_source(roots):
    from crucible.improvement.task_environment import prepare_runtime_artifacts

    image, workspace = roots
    (image / "pkg/lib.so").write_bytes(b"native")
    (image / "pkg/_version.py").write_text("VERSION = 1")
    (image / "pkg/generated.c").write_text("not a runtime artifact")
    (image / "pkg/new_version.py").write_text("must not restore deleted tracked file")
    (workspace / "pkg/new_version.py").write_text("tracked")
    git(workspace, "add", "-f", "pkg/new_version.py")
    (workspace / "pkg/new_version.py").unlink()
    (workspace / "pkg/source.py").write_text("solver edit")
    (workspace / "pkg/_version.py").write_text("existing version")
    (image / "pkg.egg-info").mkdir()
    (image / "pkg.egg-info/PKG-INFO").write_text("metadata")
    before = git(workspace, "diff")
    prepare_runtime_artifacts(image, workspace)
    prepare_runtime_artifacts(image, workspace)
    assert (workspace / "pkg/lib.so").read_bytes() == b"native"
    assert (workspace / "pkg.egg-info/PKG-INFO").read_text() == "metadata"
    assert (workspace / "pkg/_version.py").read_text() == "existing version"
    assert (workspace / "pkg/source.py").read_text() == "solver edit"
    assert not (workspace / "pkg/new_version.py").exists()
    assert not (workspace / "pkg/generated.c").exists()
    assert git(workspace, "diff") == before
    assert git(workspace, "ls-files", "--others", "--exclude-standard") == b""


@pytest.mark.parametrize("side", ["image", "workspace"])
def test_rejects_symlink_artifact_paths(roots, tmp_path, side):
    from crucible.improvement.task_environment import prepare_runtime_artifacts

    image, workspace = roots
    (image / "pkg/lib.so").write_bytes(b"native")
    outside = tmp_path / "outside"
    outside.mkdir()
    if side == "image":
        (image / "pkg/lib.so").unlink()
        (outside / "lib.so").write_bytes(b"native")
        (image / "pkg/lib.so").symlink_to(outside / "lib.so")
    else:
        (workspace / "pkg/source.py").unlink()
        (workspace / "pkg").rmdir()
        (workspace / "pkg").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        prepare_runtime_artifacts(image, workspace)
    assert not (outside / "source.py").exists()


async def test_trial_prompt_explains_native_runner_without_changing_problem(
    monkeypatch,
):
    from crucible.improvement import worker
    from tests.contract.test_improvement_worker import make_job

    captured = {}

    async def seed(job_id, prompt, *args):
        captured["prompt"] = prompt
        return job_id

    monkeypatch.setattr(worker, "seed_task", seed)
    job = make_job()
    assert await worker.seed_solver_task(job, None) == job.job_id
    assert job.solver_case.prompt in captured["prompt"]
    assert "bin/test" in captured["prompt"]
    assert "do not assume pytest" in captured["prompt"]


@pytest.mark.parametrize(
    "relative", ["/outside/lib.so", "../outside.so", ".git/config.py"]
)
def test_rejects_artifact_path_escape(tmp_path, relative):
    from crucible.improvement.task_environment import _checked

    with pytest.raises(ValueError, match="unsafe"):
        _checked(tmp_path, relative)


def test_bootstrap_preserves_command_arguments_and_cwd(tmp_path, monkeypatch):
    from crucible.improvement import task_environment

    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        task_environment, "prepare_runtime_artifacts", lambda *a: calls.append(a)
    )
    monkeypatch.setattr(task_environment.os, "execvpe", lambda *a: calls.append(a))
    original = ["python", "-c", "print('spaces; $literal')", "two words"]
    task_environment.main(["/testbed", "/workspace", *original])
    assert tuple(map(str, calls[0])) == ("/testbed", "/workspace")
    assert calls[1][0:2] == ("python", original)
    assert task_environment.os.getcwd() == str(tmp_path)


def test_image_inventory_hashes_only_runtime_artifacts(roots):
    import hashlib

    from crucible.improvement.task_environment import runtime_inventory

    image, _ = roots
    (image / "pkg/lib.so").write_bytes(b"native")
    (image / "pkg/_version.py").write_text("version")
    (image / "pkg/generated.c").write_text("build source")
    result = runtime_inventory(image)
    assert set(result) == {"pkg/lib.so", "pkg/_version.py"}
    assert result["pkg/lib.so"] == {
        "size_bytes": 6,
        "sha256": hashlib.sha256(b"native").hexdigest(),
    }
