import subprocess

import pytest

from crucible.improvement.contracts import EDITABLE_PATHS
from crucible.improvement.source import ScopeRejected, stage_harness


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


@pytest.fixture
def harness(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.name", "Test")
    git(root, "config", "user.email", "test@localhost")
    for name, content in [
        (EDITABLE_PATHS[0], "value = 1\n"),
        ("backend/src/crucible/protected.py", "safe = True\n"),
        ("backend/uv.lock", "pinned"),
        ("backend/pyproject.toml", "[project]\n"),
        ("docs/private.md", "hidden"),
        ("tests/hidden.py", "hidden"),
    ]:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(root, "add", ".")
    git(root, "commit", "-qm", "base")
    return root, git(root, "rev-parse", "HEAD")


def test_protected_change_rejected_before_import(harness, tmp_path):
    root, commit = harness
    marker = tmp_path / "executed"
    (root / EDITABLE_PATHS[0]).write_text(
        f"open({str(marker)!r},'w').write('unsafe')\n"
    )
    (root / "backend/src/crucible/protected.py").write_text("unsafe=True\n")
    with pytest.raises(ScopeRejected, match="protected"):
        stage_harness(root, commit, tmp_path / "snapshot", EDITABLE_PATHS)
    assert not marker.exists()


def test_new_allowlisted_file_is_in_patch(harness, tmp_path):
    root, commit = harness
    added = root / EDITABLE_PATHS[-1]
    added.parent.mkdir(parents=True, exist_ok=True)
    added.write_text('def coding_guidance(): return ""\n')
    snapshot = stage_harness(root, commit, tmp_path / "snapshot", EDITABLE_PATHS)
    assert snapshot.kind == "candidate"
    assert "coding_behavior.py" in (tmp_path / "snapshot/candidate.diff").read_text()


def test_export_omits_private_files_and_keeps_executable_bits(harness, tmp_path):
    root, commit = harness
    source = root / EDITABLE_PATHS[0]
    source.chmod(0o755)
    snapshot = stage_harness(root, commit, tmp_path / "snapshot", EDITABLE_PATHS)
    export = tmp_path / "snapshot/harness"
    assert snapshot.kind == "candidate"
    assert (export / EDITABLE_PATHS[0]).stat().st_mode & 0o111
    assert not (export / "docs").exists()
    assert not (export / "tests").exists()
    assert not (export / ".git").exists()
    source.write_text("changed after export")
    assert (export / EDITABLE_PATHS[0]).read_text() == "value = 1\n"


@pytest.mark.parametrize("kind", ["dependency", "symlink", "oversize", "ignored"])
def test_unsafe_sources_rejected(harness, tmp_path, kind):
    root, commit = harness
    if kind == "dependency":
        (root / "backend/uv.lock").write_text("changed")
    elif kind == "symlink":
        (root / EDITABLE_PATHS[0]).unlink()
        (root / EDITABLE_PATHS[0]).symlink_to(tmp_path / "secret")
    elif kind == "oversize":
        (root / EDITABLE_PATHS[0]).write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    else:
        (root / ".git/info/exclude").write_text("secret.py\n")
        (root / "backend/src/crucible/secret.py").write_text("private")
    with pytest.raises(ScopeRejected):
        stage_harness(root, commit, tmp_path / "snapshot", EDITABLE_PATHS)


def test_no_effect_baseline_exports_identically(harness, tmp_path):
    root, commit = harness
    a = stage_harness(root, commit, tmp_path / "a", EDITABLE_PATHS)
    b = stage_harness(root, commit, tmp_path / "b", EDITABLE_PATHS)
    assert a.kind == b.kind == "baseline"
    assert a.patch_digest is None
    assert a.source_digest == b.source_digest
    assert a.export_digest == b.export_digest


def test_template_and_ignored_bytecode_do_not_enter_export(harness, tmp_path):
    root, _ = harness
    (root / ".env.example").write_text("KEY=\n")
    (root / ".gitignore").write_text("__pycache__/\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "template")
    commit = git(root, "rev-parse", "HEAD")
    cache = root / "backend/src/crucible/__pycache__"
    cache.mkdir()
    (cache / "module.pyc").write_bytes(b"bytecode")
    stage_harness(root, commit, tmp_path / "snapshot", EDITABLE_PATHS)
    export = tmp_path / "snapshot/harness"
    assert not (export / ".env.example").exists()
    assert not (export / "backend/src/crucible/__pycache__").exists()


def test_host_controller_and_grader_are_not_exported(harness, tmp_path):
    root, _ = harness
    for name in [
        "benchmarks/swebench.py",
        "benchmarks/admission.py",
        "improvement/store.py",
        "repair/trials.py",
        "application/container.py",
    ]:
        path = root / "backend/src/crucible" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("HOST_AUTHORITY = True\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "protected host modules")
    stage_harness(
        root, git(root, "rev-parse", "HEAD"), tmp_path / "snapshot", EDITABLE_PATHS
    )
    export = tmp_path / "snapshot/harness/backend/src/crucible"
    assert not (export / "benchmarks/swebench.py").exists()
    assert not (export / "benchmarks/admission.py").exists()
    assert not (export / "improvement/store.py").exists()
    assert not (export / "repair/trials.py").exists()
    assert not (export / "application/container.py").exists()
