import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from crucible.benchmarks.contracts import BenchmarkCase
from crucible.benchmarks.manifests import (
    case_digest,
    export_solver_case,
    fixture_digest,
    load_case,
)


def git(path: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


@pytest.fixture
def case_fixture(tmp_path: Path) -> BenchmarkCase:
    source = tmp_path / "fixture"
    source.mkdir()
    git(source, "init", "-q")
    git(source, "config", "user.email", "test@example.com")
    git(source, "config", "user.name", "Test")
    (source / "app.py").write_text("answer = 1\n")
    git(source, "add", "app.py")
    git(source, "commit", "-qm", "base")
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Fix the answer")
    private = tmp_path / "private-grader.json"
    private.write_text("{}")
    case = BenchmarkCase(
        case_id="astropy__astropy-12907",
        split="calibration",
        repo="astropy/astropy",
        base_commit=git(source, "rev-parse", "HEAD"),
        fixture_path=source,
        fixture_digest=fixture_digest(source),
        prompt_path=prompt,
        prompt_digest=hashlib.sha256(prompt.read_bytes()).hexdigest(),
        image="example/image@sha256:" + "a" * 64,
        platform="linux/amd64",
        grader_input_path=private,
        grader_input_digest=hashlib.sha256(b"{}").hexdigest(),
    )
    return case


def test_solver_view_excludes_protected_fields(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    case = case_fixture
    view = export_solver_case(case, tmp_path / "solver")
    assert (view.workspace / "app.py").read_text() == "answer = 1\n"
    assert not (view.workspace / "private-grader.json").exists()
    assert "grader_input_path" not in view.__dataclass_fields__
    assert view.base_commit == case.base_commit
    assert git(view.workspace, "rev-list", "--count", "HEAD") == "1"


def test_manifest_rejects_mutable_image(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    payload = case_fixture.model_dump(mode="json")
    payload["image"] = "example/image:latest"
    path = tmp_path / "manifest.json"
    import json

    path.write_text(json.dumps(payload))
    with pytest.raises(ValidationError):
        load_case(path)


def test_digest_mismatch_rejected(case_fixture: BenchmarkCase, tmp_path: Path) -> None:
    case_fixture.prompt_path.write_text("changed")
    with pytest.raises(ValueError, match="digest"):
        export_solver_case(case_fixture, tmp_path / "solver")


def test_symlink_export_escape_rejected(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    (tmp_path / "link").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        export_solver_case(case_fixture, tmp_path / "link" / "solver")


def test_protected_file_in_fixture_rejected(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    root = case_fixture.fixture_path
    (root / "private-grader.json").write_text("{}")
    git(root, "add", ".")
    git(root, "commit", "-qm", "corrupt fixture")
    case = case_fixture.model_copy(
        update={
            "base_commit": git(root, "rev-parse", "HEAD"),
            "fixture_digest": fixture_digest(root),
        }
    )
    with pytest.raises(ValueError, match="protected"):
        export_solver_case(case, tmp_path / "solver")


def test_same_content_moved_path_has_same_digest(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    moved = tmp_path / "moved"
    shutil.copytree(case_fixture.fixture_path, moved)
    case = case_fixture.model_copy(update={"fixture_path": moved})
    assert case_digest(case) == case_digest(case_fixture)


@pytest.mark.parametrize(
    "kind", ["ignored", "dirty", "symlink", "prompt_inside", "large"]
)
def test_invalid_fixture_rejected(
    case_fixture: BenchmarkCase, tmp_path: Path, kind: str
) -> None:
    root = case_fixture.fixture_path
    case = case_fixture
    if kind == "ignored":
        (root / ".gitignore").write_text("secret\n")
        git(root, "add", ".gitignore")
        git(root, "commit", "-qm", "ignore")
        (root / "secret").write_text("private")
    elif kind == "dirty":
        (root / "app.py").write_text("changed")
    elif kind == "symlink":
        (root / "link").symlink_to(case.prompt_path)
        git(root, "add", "link")
        git(root, "commit", "-qm", "link")
    elif kind == "prompt_inside":
        case = case.model_copy(update={"prompt_path": root / "app.py"})
    else:
        case.prompt_path.write_bytes(b"a" * 1_048_577)
        case = case.model_copy(
            update={
                "prompt_digest": hashlib.sha256(
                    case.prompt_path.read_bytes()
                ).hexdigest()
            }
        )
    if kind in {"ignored", "symlink"}:
        case = case.model_copy(update={"base_commit": git(root, "rev-parse", "HEAD")})
    with pytest.raises(
        ValueError,
        match={
            "ignored": "clean",
            "dirty": "clean",
            "symlink": "symlink",
            "prompt_inside": "protected",
            "large": "limit",
        }[kind],
    ):
        export_solver_case(case, tmp_path / "solver")


def test_assume_unchanged_edit_rejected(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    root = case_fixture.fixture_path
    git(root, "update-index", "--assume-unchanged", "app.py")
    (root / "app.py").write_text("tampered\n")
    case = case_fixture.model_copy(update={"fixture_digest": fixture_digest(root)})
    with pytest.raises(ValueError, match="pinned"):
        export_solver_case(case, tmp_path / "solver")


def test_tracked_ignored_file_preserved(
    case_fixture: BenchmarkCase, tmp_path: Path
) -> None:
    root = case_fixture.fixture_path
    (root / ".gitignore").write_text("app.py\n")
    git(root, "add", ".gitignore")
    git(root, "commit", "-qm", "ignore tracked file")
    case = case_fixture.model_copy(
        update={
            "base_commit": git(root, "rev-parse", "HEAD"),
            "fixture_digest": fixture_digest(root),
        }
    )
    view = export_solver_case(case, tmp_path / "solver")
    assert git(view.workspace, "show", "HEAD:app.py") == "answer = 1"
