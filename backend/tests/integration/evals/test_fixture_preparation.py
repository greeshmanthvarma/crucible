import subprocess
from pathlib import Path
from uuid import uuid4

from crucible.evals.fixtures import FixturePreparer
from crucible.evals.manifests import EvalPartition, load_case


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


async def test_each_trial_gets_clean_pinned_fixture(tmp_path: Path) -> None:
    partition = tmp_path / "development"
    source = partition / "fixtures" / "tiny"
    source.mkdir(parents=True)
    _git(source, "init", "-q")
    (source / "README.md").write_text("base\n")
    _git(source, "add", "README.md")
    _git(
        source,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        "base",
    )
    revision = _git(source, "rev-parse", "HEAD")
    cases = partition / "cases"
    cases.mkdir()
    (cases / "tiny.toml").write_text(
        f'version = 1\nid = "tiny"\nfixture = "fixtures/tiny"\n'
        f'revision = "{revision}"\nprompt = "Edit README"\nmodel = "fake"\n'
        '[[evaluators]]\nkind = "required_file"\npath = "README.md"\n'
    )
    case = load_case(EvalPartition.DEVELOPMENT, "tiny", root=partition)
    preparer = FixturePreparer(tmp_path / "app-data")
    first = preparer.prepare(case, uuid4())
    (first.root / "README.md").write_text("dirty\n")
    (first.root / "untracked.txt").write_text("leak")
    (source / "uncommitted.txt").write_text("source leak")
    second = preparer.prepare(case, uuid4())
    assert first.root != second.root
    assert second.source_commit == first.source_commit == revision
    assert second.content_digest == first.content_digest
    assert (second.root / "README.md").read_text() == "base\n"
    assert not (second.root / "untracked.txt").exists()
    assert not (second.root / "uncommitted.txt").exists()
    assert _git(second.root, "status", "--porcelain") == ""


async def test_checked_in_bundle_prepares_without_hidden_material(
    tmp_path: Path,
) -> None:
    case = load_case(EvalPartition.DEVELOPMENT, "tiny")
    prepared = FixturePreparer(tmp_path).prepare(case, uuid4())
    assert prepared.source_commit == case.fixture_revision
    assert (prepared.root / "README.md").exists()
    assert not (prepared.root / "tests").exists()
    assert not (prepared.root / "cases").exists()
