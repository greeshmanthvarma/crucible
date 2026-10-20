"""Publication fault injection uses a local bare remote and scripted GitHub port."""

import re

import pytest

from crucible.improvement.publication import (
    ImprovementPR,
    LocalPublicationPort,
    PublicationService,
)
from crucible.improvement.reporting import ReportExporter
from crucible.improvement.selection import decide_candidate
from crucible.improvement.store import RevisionConflict
from crucible.improvement.trials import build_evaluation_set
from crucible.repair.identity import digest
from tests.unit.test_improvement_source import git

pytest_plugins = ["tests.integration.test_improvement_final"]


@pytest.fixture
async def publication_setup(promotion_setup, tmp_path):
    promotions, store, paths, _, round, baseline, candidate, checks = promotion_setup
    id = round.experiment_id
    before = await build_evaluation_set(store, paths, id, baseline.snapshot_id)
    after = await build_evaluation_set(store, paths, id, candidate.snapshot_id)
    decision = decide_candidate(before, after, checks)
    await store.record_decision(round.id, decision, (await store.get(id)).revision)
    promotion = await promotions.promote(round.id, digest(decision))
    await store.record_event(
        id,
        "search_outcome",
        {
            "ready_for_final": True,
            "reason": "round_limit",
            "accepted_commit": promotion.commit_sha,
        },
    )
    await store.seal_search(id, promotion.commit_sha, "round_limit")
    directory = tmp_path / "report"
    bundle = await ReportExporter(store, paths).export_report(id, directory)
    bare = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", "-q", str(bare))
    git(paths.repository, "remote", "add", "origin", str(bare))
    port = ScriptedGitHub(paths.repository)
    yield (
        PublicationService(store, paths, port),
        store,
        paths,
        port,
        bundle,
        directory,
        bare,
    )


class ScriptedGitHub(LocalPublicationPort):
    def __init__(self, root):
        super().__init__(root)
        self.pr = None
        self.creations = self.pushes = self.label_calls = 0
        self.crash = None

    async def push_expected_branch(self, remote, branch, head, expected):
        self.pushes += 1
        await super().push_expected_branch(remote, branch, head, expected)
        if self.crash == "push":
            self.crash = None
            raise RuntimeError("crash after push")

    async def find_pr(self, branch, base):
        return self.pr

    async def create_pr(self, branch, base, title, body):
        self.creations += 1
        self.pr = ImprovementPR(
            number=1,
            url="https://github.com/test/repo/pull/1",
            state="open",
            head_sha=await self.remote_head("origin", branch),
            head_branch=branch,
            base_branch=base,
            ownership_digest=re.search(
                r"Crucible publication: ([0-9a-f]{64})", body
            ).group(1),
            labels=(),
        )
        if self.crash == "pr":
            self.crash = None
            raise RuntimeError("crash after PR")
        return self.pr

    async def ensure_label(self, number, label):
        self.label_calls += 1
        assert self.pr.number == number
        self.pr = self.pr.model_copy(update={"labels": (label,)})


async def test_remote_writes_require_explicit_authorization(publication_setup):
    service, store, _, port, bundle, directory, _ = publication_setup
    with pytest.raises(RevisionConflict, match="explicit remote write"):
        await service.publish(bundle.experiment_id, bundle, directory)
    assert not await store.publication(bundle.experiment_id)
    assert port.pushes == port.creations == 0


@pytest.mark.parametrize("crash", ["push", "pr"])
async def test_publication_reconciles_crash_without_duplicate_effect(
    publication_setup, crash
):
    service, store, paths, port, bundle, directory, _ = publication_setup
    id = bundle.experiment_id
    main_before = git(paths.repository, "rev-parse", "HEAD")
    code_commit = (await store.search_seal(id)).selected_final_commit
    port.crash = crash
    with pytest.raises(RuntimeError, match="crash after"):
        await service.publish(id, bundle, directory, authorize_remote_write=True)
    intent = await store.publication(id)
    assert intent.last_error == "publication_failed"
    result = await service.publish(id, bundle, directory, authorize_remote_write=True)
    assert result.status == "published"
    assert port.pushes == port.creations == 1
    assert port.pr.labels == ("crucible-improvement",)
    assert (
        await service.publish(id, bundle, directory, authorize_remote_write=True)
    ).expected_head == result.expected_head
    assert port.pushes == port.creations == 1
    assert git(paths.repository, "rev-parse", "HEAD") == main_before
    assert git(paths.repository, "rev-parse", result.branch) == code_commit
    changed = git(
        paths.repository,
        "diff-tree",
        "--no-commit-id",
        "--name-only",
        "-r",
        code_commit,
        result.expected_head,
    ).splitlines()
    assert set(changed) == {
        f"reports/self-improvement/{id}/report.md",
        f"reports/self-improvement/{id}/metrics.json",
    }


async def test_unrelated_pr_is_never_adopted(publication_setup):
    service, store, _, port, bundle, directory, _ = publication_setup
    port.crash = "pr"
    with pytest.raises(RuntimeError):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    port.pr = port.pr.model_copy(update={"ownership_digest": "f" * 64})
    with pytest.raises(RevisionConflict, match="unrelated"):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    assert port.label_calls == 0
    assert (await store.publication(bundle.experiment_id)).status != "published"


async def test_remote_advance_is_preserved(publication_setup):
    service, store, _, port, bundle, directory, bare = publication_setup
    id = bundle.experiment_id
    port.crash = "push"
    with pytest.raises(RuntimeError):
        await service.publish(id, bundle, directory, authorize_remote_write=True)
    intent = await store.publication(id)
    code_commit = (await store.search_seal(id)).selected_final_commit
    git(bare, "update-ref", "refs/heads/" + intent.branch, code_commit)
    with pytest.raises(RevisionConflict, match="advanced unexpectedly"):
        await service.publish(id, bundle, directory, authorize_remote_write=True)
    assert await port.remote_head("origin", intent.branch) == code_commit
    assert port.pushes == 1


async def test_report_tampering_is_denied_before_push(publication_setup):
    service, _, _, port, bundle, directory, _ = publication_setup
    (directory / "report.md").write_text("CANARY_ARBITRARY_CONTENT")
    with pytest.raises(RevisionConflict, match="drift"):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    assert port.pushes == 0


async def test_no_patch_requires_explicit_report_only_publication(
    final_setup, tmp_path
):
    evaluator, _, store, manifest, _, _, _, _ = final_setup
    paths, id = evaluator.paths, manifest.experiment_id
    directory = tmp_path / "unchanged-report"
    bundle = await ReportExporter(store, paths).export_report(id, directory)
    bare = tmp_path / "unchanged-remote.git"
    git(tmp_path, "init", "--bare", "-q", str(bare))
    git(paths.repository, "remote", "add", "origin", str(bare))
    port = ScriptedGitHub(paths.repository)
    service = PublicationService(store, paths, port)
    with pytest.raises(RevisionConflict, match="no accepted improvement"):
        await service.publish(id, bundle, directory, authorize_remote_write=True)
    assert port.pushes == port.creations == 0
    record = await service.publish(
        id, bundle, directory, authorize_remote_write=True, report_only=True
    )
    assert record.branch == f"codex/improve-report/{id}"
    assert record.status == "published"


async def test_report_file_creation_crash_reuses_complete_atomic_files(
    publication_setup, monkeypatch
):
    import crucible.improvement.publication as module

    service, store, _, port, bundle, directory, _ = publication_setup
    original = module.atomic_source_file
    crashed = False

    def crash_once(*args):
        nonlocal crashed
        original(*args)
        if not crashed:
            crashed = True
            raise RuntimeError("crash after atomic report write")

    monkeypatch.setattr(module, "atomic_source_file", crash_once)
    with pytest.raises(RuntimeError, match="atomic report write"):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    assert await store.publication(bundle.experiment_id) is None
    assert port.pushes == 0
    record = await service.publish(
        bundle.experiment_id, bundle, directory, authorize_remote_write=True
    )
    assert record.status == "published"
    assert port.pushes == port.creations == 1


@pytest.mark.parametrize("pr_base", ["main", "other-base"])
async def test_preexisting_unrelated_pr_blocks_push(publication_setup, pr_base):
    service, store, _, port, bundle, directory, _ = publication_setup
    seal = await store.search_seal(bundle.experiment_id)
    port.pr = ImprovementPR(
        number=2,
        url="https://github.com/test/repo/pull/2",
        state="open",
        head_sha=seal.selected_final_commit,
        head_branch=f"codex/improve/{bundle.experiment_id}",
        base_branch=pr_base,
        ownership_digest=None,
        labels=(),
    )
    with pytest.raises(RevisionConflict, match="pre-existing PR"):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    assert port.pushes == port.creations == 0
    assert await store.publication(bundle.experiment_id) is None


async def test_exact_publication_can_recover_without_relaxing_other_pr_admission(
    publication_setup,
):
    from crucible.improvement.publication import ownership_digest, owns_publication_pr

    service, store, _, port, bundle, directory, _ = publication_setup
    port.crash = "pr"
    with pytest.raises(RuntimeError):
        await service.publish(
            bundle.experiment_id, bundle, directory, authorize_remote_write=True
        )
    record = await store.publication(bundle.experiment_id)
    pr = {
        "number": 1,
        "html_url": port.pr.url,
        "state": "open",
        "body": f"Crucible publication: {ownership_digest(record)}",
        "head": {
            "ref": record.branch,
            "sha": record.expected_head,
            "repo": {"full_name": "test/repo"},
        },
        "base": {"ref": record.base_branch, "repo": {"full_name": "test/repo"}},
    }
    assert owns_publication_pr(record, pr, "test/repo")
    assert not owns_publication_pr(record, {**pr, "body": "unrelated PR"}, "test/repo")
    assert not owns_publication_pr(record, pr, "test/other")
    assert not owns_publication_pr(
        record, {**pr, "head": {**pr["head"], "sha": "f" * 40}}, "test/repo"
    )


async def test_pr_lookup_includes_other_bases(tmp_path):
    import json

    class GitHubReadPort(LocalPublicationPort):
        async def _repository(self):
            return "test/repo"

        async def _run(self, *argv):
            # The scripted API reproduces GitHub's optional base filter.
            if "base=main" in argv:
                return "[]"
            return json.dumps(
                [
                    {
                        "number": 1,
                        "html_url": "https://github.com/test/repo/pull/1",
                        "state": "open",
                        "body": "unrelated",
                        "head": {
                            "ref": "branch",
                            "sha": "a" * 40,
                            "repo": {"full_name": "test/repo"},
                        },
                        "base": {
                            "ref": "other-base",
                            "repo": {"full_name": "test/repo"},
                        },
                        "labels": [],
                    }
                ]
            )

    pr = await GitHubReadPort(tmp_path).find_pr("branch", "main")
    assert pr is not None and pr.base_branch == "other-base"
