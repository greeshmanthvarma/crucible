import subprocess
from pathlib import Path

import pytest

from crucible.evals.manifests import EvalPartition, load_case, load_suite


def _git(*args: str, cwd: Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def _case(
    root: Path, *, case_id: str = "tiny", evaluator: str = "required_file"
) -> Path:
    fixture = root / "fixtures" / "tiny"
    fixture.mkdir(parents=True)
    _git("init", "-q", cwd=fixture)
    (fixture / "README.md").write_text("starter\n")
    _git("add", ".", cwd=fixture)
    _git(
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        "base",
        cwd=fixture,
    )
    revision = _git("rev-parse", "HEAD", cwd=fixture)
    cases = root / "cases"
    cases.mkdir()
    path = cases / "tiny.toml"
    path.write_text(
        f'version = 1\nid = "{case_id}"\nfixture = "fixtures/tiny"\n'
        f'revision = "{revision}"\nprompt = "Write a file"\nmodel = "fake"\n'
        f'[[evaluators]]\nkind = "{evaluator}"\npath = "answer.txt"\n'
    )
    return path


def test_case_digest_tracks_fixture_and_definition(tmp_path: Path) -> None:
    path = _case(tmp_path)
    first = load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path)
    assert (
        first.case_digest
        == load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path).case_digest
    )
    path.write_text(path.read_text().replace("Write a file", "Write two files"))
    assert (
        load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path).case_digest
        != first.case_digest
    )


def test_rejects_unknown_evaluator_and_unpinned_revision(tmp_path: Path) -> None:
    path = _case(tmp_path, evaluator="arbitrary_python")
    with pytest.raises(ValueError, match="evaluator"):
        load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path)
    path.write_text(
        path.read_text()
        .replace("arbitrary_python", "required_file")
        .replace('revision = "', 'revision = "bad')
    )
    with pytest.raises(ValueError, match="revision"):
        load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path)


def test_rejects_escape_and_duplicate_suite_members(tmp_path: Path) -> None:
    _case(tmp_path)
    suites = tmp_path / "suites"
    suites.mkdir()
    (suites / "smoke.toml").write_text(
        'version = 1\nid = "smoke"\ncases = ["tiny", "tiny"]\n'
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_suite(EvalPartition.DEVELOPMENT, "smoke", root=tmp_path)
    (tmp_path / "fixtures" / "tiny").rename(tmp_path / "fixture-real")
    (tmp_path / "fixtures" / "tiny").symlink_to(tmp_path / "fixture-real")
    with pytest.raises(ValueError, match="escape|symlink"):
        load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path)


def test_development_lookup_cannot_load_held_out_case(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_case(EvalPartition.DEVELOPMENT, "secret", root=tmp_path)


def test_case_accepts_budgets_setup_and_validation(tmp_path: Path) -> None:
    path = _case(tmp_path)
    path.write_text(
        path.read_text()
        + "\n[budgets]\nmax_steps = 3\nmax_tool_calls = 4\n"
        + '[setup]\nrequired_paths = ["README.md"]\n'
        + "[settings]\nvalidation_repair_limit = 1\n"
        + '[[settings.validation_commands]]\nexecutable = "sh"\n'
        + 'arguments = ["-c", "true"]\ncwd = "."\ntimeout_seconds = 10\n'
        + 'network = "none"\nenvironment = {}\n'
        + 'image = "alpine@sha256:'
        + "a" * 64
        + '"\n'
        + 'reason = "validate"\n'
        + "[settings.validation_commands.limits]\ncpus = 1\n"
        + "memory_bytes = 67108864\npids = 32\noutput_bytes = 1024\n"
    )
    case = load_case(EvalPartition.DEVELOPMENT, "tiny", root=tmp_path)
    assert case.budgets.max_steps == 3
    assert case.budgets.max_tool_calls == 4
    assert case.setup_required_paths == ("README.md",)
    assert case.repository_settings.validation_commands[0].executable == "sh"
