import subprocess
from pathlib import Path

import pytest

from crucible.benchmarks.patches import collect_solution_patch


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def test_untracked_file_in_solution_patch(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    git(root, "init", "-q")
    git(
        root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "--allow-empty",
        "-qm",
        "base",
    )
    tree = git(root, "rev-parse", "HEAD^{tree}")
    commit = git(root, "rev-parse", "HEAD")
    (root / "new.py").write_text("answer = 42\n")
    patch = collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)
    assert "new.py" in patch.path.read_text()
    assert "+answer = 42" in patch.path.read_text()
    assert git(root, "status", "--porcelain") == "?? new.py"


@pytest.fixture
def workspace(tmp_path: Path) -> tuple[Path, str, str]:
    root = tmp_path / "workspace"
    root.mkdir()
    git(root, "init", "-q")
    (root / "app.py").write_text("old = True\n")
    (root / ".gitignore").write_text("secret\n")
    git(root, "add", ".")
    git(
        root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        "base",
    )
    return root, git(root, "rev-parse", "HEAD^{tree}"), git(root, "rev-parse", "HEAD")


@pytest.mark.parametrize("action", ["edit", "delete", "empty", "ignored"])
def test_supported_changes(
    workspace: tuple[Path, str, str], tmp_path: Path, action: str
) -> None:
    root, tree, commit = workspace
    if action == "edit":
        (root / "app.py").write_text("new = True\n")
    elif action == "delete":
        (root / "app.py").unlink()
    elif action == "ignored":
        (root / "secret").write_text("private")
    index_before = (root / ".git/index").read_bytes()
    patch = collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)
    content = patch.path.read_text()
    assert (root / ".git/index").read_bytes() == index_before
    if action in {"empty", "ignored"}:
        assert content == ""
    elif action == "delete":
        assert "deleted file mode" in content
    else:
        assert "+new = True" in content
    assert "private" not in content


@pytest.mark.parametrize(
    "kind", ["binary", "non_utf8", "symlink", "large", "base", "submodule"]
)
def test_unsupported_changes_rejected(
    workspace: tuple[Path, str, str], tmp_path: Path, kind: str
) -> None:
    root, tree, commit = workspace
    if kind == "binary":
        (root / "app.py").write_bytes(b"a\0b")
    elif kind == "non_utf8":
        (root / "app.py").write_bytes(b"\xff")
    elif kind == "symlink":
        (root / "new").symlink_to(tmp_path)
    elif kind == "large":
        (root / "app.py").write_text("a" * 500)
    elif kind == "base":
        (root / "app.py").write_text("changed")
        git(root, "add", ".")
        git(
            root,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "changed",
        )
    else:
        git(root, "update-index", "--add", "--cacheinfo", "160000", commit, "submodule")
        git(
            root,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "submodule",
        )
        tree = git(root, "rev-parse", "HEAD^{tree}")
    with pytest.raises(
        ValueError,
        match={
            "binary": "binary",
            "non_utf8": "UTF8",
            "symlink": "symlink",
            "large": "limit",
            "base": "changed base",
            "submodule": "submodule",
        }[kind],
    ):
        collect_solution_patch(
            root, tree, tmp_path / "p.diff", base_commit=commit, max_bytes=200
        )


def test_unchanged_binary_is_not_a_solution_change(
    workspace: tuple[Path, str, str], tmp_path: Path
) -> None:
    root, _, commit = workspace
    (root / "image.png").write_bytes(b"\x89PNG\0data")
    git(root, "add", ".")
    git(
        root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        "asset",
    )
    tree = git(root, "rev-parse", "HEAD^{tree}")
    (root / "app.py").write_text("answer = 42\n")
    patch = collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)
    assert "image.png" not in patch.path.read_text()


@pytest.mark.parametrize("direction", ["file_to_directory", "directory_to_file"])
def test_regular_file_replacements(
    workspace: tuple[Path, str, str], tmp_path: Path, direction: str
) -> None:
    root, tree, commit = workspace
    if direction == "file_to_directory":
        (root / "app.py").unlink()
        (root / "app.py").mkdir()
        (root / "app.py/new.py").write_text("new\n")
    else:
        (root / "pkg").mkdir()
        (root / "pkg/old.py").write_text("old\n")
        git(root, "add", ".")
        git(
            root,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "package",
        )
        tree = git(root, "rev-parse", "HEAD^{tree}")
        (root / "pkg/old.py").unlink()
        (root / "pkg").rmdir()
        (root / "pkg").write_text("replacement\n")
    patch = collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)
    assert (
        "+new" in patch.path.read_text()
        if direction == "file_to_directory"
        else "+replacement" in patch.path.read_text()
    )


def test_git_config_cannot_redirect_snapshot(
    workspace: tuple[Path, str, str], tmp_path: Path
) -> None:
    root, tree, commit = workspace
    git(root, "config", "core.worktree", str(tmp_path))
    (root / "app.py").write_text("actual = True\n")
    patch = collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)
    assert "+actual = True" in patch.path.read_text()


def test_symlinked_git_directory_rejected(
    workspace: tuple[Path, str, str], tmp_path: Path
) -> None:
    root, tree, commit = workspace
    (root / ".git").rename(tmp_path / "metadata")
    (root / ".git").symlink_to(tmp_path / "metadata", target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        collect_solution_patch(root, tree, tmp_path / "p.diff", base_commit=commit)


def test_candidate_sparse_file_rejected_before_host_read(workspace, tmp_path):
    workspace, _, _ = workspace
    path = workspace / "large.py"
    with path.open("wb") as stream:
        stream.truncate(1024**3)
    import subprocess

    tree = subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "HEAD^{tree}"], text=True
    ).strip()
    commit = subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], text=True
    ).strip()
    with pytest.raises(ValueError, match="snapshot limit"):
        collect_solution_patch(
            workspace,
            tree,
            tmp_path / "p.diff",
            base_commit=commit,
            snapshot_limits=(1024 * 1024, 2 * 1024 * 1024, 1000),
        )


def inventory_file(root, path, content):
    import hashlib

    from crucible.benchmarks.runtime_artifacts import (
        RuntimeArtifact,
        RuntimeArtifactInventory,
    )

    return RuntimeArtifactInventory(
        image="task@sha256:" + "a" * 64,
        files={
            path: RuntimeArtifact(
                size_bytes=len(content), sha256=hashlib.sha256(content).hexdigest()
            )
        },
    )


def test_inventory_excludes_large_unchanged_dependency_before_source_bounds(
    workspace, tmp_path
):
    root, tree, commit = workspace
    content = b"\0" * 20000
    (root / "secret").write_bytes(content)
    (root / "app.py").write_text("changed = True\n")
    patch = collect_solution_patch(
        root,
        tree,
        tmp_path / "p.diff",
        base_commit=commit,
        snapshot_limits=(1024, 2048, 100),
        runtime_inventory=inventory_file(root, "secret", content),
    )
    assert "+changed = True" in patch.path.read_text()
    assert "secret" not in patch.path.read_text()


@pytest.mark.parametrize(
    "change", [b"x" * 20000, b"\0" * 20001], ids=["content", "size"]
)
def test_inventory_modified_dependency_rejected(workspace, tmp_path, change):
    root, tree, commit = workspace
    inventory = inventory_file(root, "secret", b"\0" * 20000)
    (root / "secret").write_bytes(change)
    with pytest.raises(ValueError, match="runtime artifact"):
        collect_solution_patch(
            root,
            tree,
            tmp_path / "p.diff",
            base_commit=commit,
            runtime_inventory=inventory,
        )


@pytest.mark.parametrize("delete", [False, True])
def test_inventory_never_hides_tracked_source(workspace, tmp_path, delete):
    root, tree, commit = workspace
    inventory = inventory_file(root, "app.py", b"old = True\n")
    if delete:
        (root / "app.py").unlink()
    else:
        (root / "app.py").write_text("modified = True\n")
    patch = collect_solution_patch(
        root, tree, tmp_path / "p.diff", base_commit=commit, runtime_inventory=inventory
    )
    assert "-old = True" in patch.path.read_text()


def test_candidate_ignore_rules_cannot_authorize_inventory_skip(workspace, tmp_path):
    root, tree, commit = workspace
    (root / ".gitignore").write_text("secret\nnew.py\n")
    (root / "new.py").write_text("new_source = True\n")
    inventory = inventory_file(root, "new.py", (root / "new.py").read_bytes())
    # It is not ignored in the trusted base; it must remain in source accounting.
    with pytest.raises(ValueError, match="workspace file exceeds"):
        collect_solution_patch(
            root,
            tree,
            tmp_path / "p.diff",
            base_commit=commit,
            snapshot_limits=(16, 2048, 100),
            runtime_inventory=inventory,
        )


def test_inventory_keeps_source_bloat_guard(workspace, tmp_path):
    root, tree, commit = workspace
    (root / "secret").write_bytes(b"\0" * 20000)
    (root / "app.py").write_bytes(b"x" * 3000)
    with pytest.raises(ValueError, match="workspace file exceeds"):
        collect_solution_patch(
            root,
            tree,
            tmp_path / "p.diff",
            base_commit=commit,
            snapshot_limits=(1024, 2048, 100),
            runtime_inventory=inventory_file(root, "secret", b"\0" * 20000),
        )


def test_inventory_preserves_deleted_tracked_ignore_file(workspace, tmp_path):
    root, tree, commit = workspace
    (root / ".gitignore").unlink()
    (root / "secret").write_bytes(b"\0" * 20000)
    patch = collect_solution_patch(
        root,
        tree,
        tmp_path / "p.diff",
        base_commit=commit,
        runtime_inventory=inventory_file(root, "secret", b"\0" * 20000),
    )
    assert "deleted file" in patch.path.read_text()
    assert "-secret" in patch.path.read_text()


def test_inventory_dependency_symlink_is_still_rejected(workspace, tmp_path):
    root, tree, commit = workspace
    (tmp_path / "outside").write_bytes(b"\0" * 20000)
    (root / "secret").symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="symlink"):
        collect_solution_patch(
            root,
            tree,
            tmp_path / "p.diff",
            base_commit=commit,
            runtime_inventory=inventory_file(root, "secret", b"\0" * 20000),
        )


@pytest.mark.parametrize(
    "name", [".", "../bad.so", "/bad.so", ".git/config.py", "a/../b.so", "a//b.so"]
)
def test_inventory_contract_rejects_unsafe_paths(name):
    with pytest.raises(ValueError, match="unsafe runtime artifact"):
        inventory_file(None, name, b"")
