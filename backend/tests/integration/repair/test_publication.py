from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.domain.repair import RepairFix
from crucible.repair.ports import PRIdentity
from crucible.repair.publication import PublicationService
from crucible.repair.reporting import RepairReporter
from tests.integration.repair.test_git_acceptance import git_environment
from tests.integration.storage.test_tool_loop_storage import CLOCK


class GitHub:
    def __init__(self, fail=None):
        self.fail, self.pr, self.creates, self.labels, self.pushes = (
            fail,
            None,
            0,
            set(),
            [],
        )

    async def push_branch(self, branch, sha):
        self.pushes.append((branch, sha))
        if self.fail == "push":
            self.fail = None
            raise OSError("PRIVATE_NETWORK_ERROR")

    async def find_pr(self, head, base):
        return self.pr

    async def create_pr(self, head, base, title, body):
        self.creates += 1
        self.pr = PRIdentity(
            123, "https://github.com/example/repo/pull/123", "open", self.pushes[-1][1]
        )
        if self.fail == "create":
            self.fail = None
            raise OSError("lost response")
        return self.pr

    async def ensure_label(self, number, label):
        if self.fail == "label":
            self.fail = None
            raise OSError("label unavailable")
        self.labels.add((number, label))


async def test_no_accepted_commits_never_publishes(database, tmp_path):
    repair, factory, _, _ = await git_environment(database, tmp_path)
    github = GitHub()
    publisher = PublicationService(factory, CLOCK, github, None)
    assert await publisher.publish(repair.id) is None
    assert github.pushes == [] and github.creates == 0


@pytest.mark.parametrize("failure", ["push", "create", "label", None])
async def test_partial_success_and_lost_responses_reuse_same_pr(
    database, tmp_path, failure
):
    repair, factory, root, git = await git_environment(database, tmp_path)
    # Use the ordinary artifact service composition from admission's environment.
    from crucible.application.artifact_service import ArtifactService
    from crucible.artifacts.store import LocalArtifactStore

    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )
    accepted = RepairFix(
        uuid4(), repair.id, 0, "private", None, "accepted", repair.last_accepted_commit
    )
    async with factory() as uow:
        await uow.repairs.add_fix(accepted)
        await uow.repairs.add_fix(
            replace(
                accepted,
                id=uuid4(),
                ordinal=1,
                status="exhausted",
                accepted_commit=None,
            )
        )
        await uow.commit()
    github = GitHub(failure)
    publisher = PublicationService(
        factory, CLOCK, github, RepairReporter(factory, artifacts, CLOCK)
    )
    if failure:
        with pytest.raises(OSError):
            await publisher.publish(repair.id)
    pr = await publisher.publish(repair.id)
    assert pr.number == 123 and github.creates == 1
    assert github.labels == {(123, "crucible-repair")}
    assert github.pushes[-1] == (repair.branch, repair.last_accepted_commit)
    async with factory() as uow:
        saved = await uow.repairs.get_publication(repair.id)
    assert saved.status == "published" and saved.pr_url == pr.url


async def test_github_adapter_uses_exact_object_no_force_and_body_file(tmp_path):
    import json
    from pathlib import Path

    from crucible.repair.github import LocalRepairGitHub

    calls = []

    class Process:
        async def run(self, root, *argv):
            calls.append(argv)
            if argv[:3] == ("git", "remote", "get-url"):
                return "git@github.com:example/repo.git"
            if argv[:3] == ("gh", "pr", "create"):
                body = Path(argv[argv.index("--body-file") + 1])
                assert body.read_text() == "literal `text` $(no-shell)\nnext line"
                return "https://github.com/example/repo/pull/123"
            if argv[:3] == ("gh", "pr", "list"):
                return json.dumps(
                    [
                        {
                            "number": 123,
                            "url": "https://github.com/example/repo/pull/123",
                            "state": "OPEN",
                            "headRefOid": "a" * 40,
                        }
                    ]
                )
            return ""

    adapter = LocalRepairGitHub(tmp_path, Process())
    branch = f"repair/{uuid4()}"
    await adapter.push_branch(branch, "a" * 40)
    pr = await adapter.create_pr(
        branch, "main", "title", "literal `text` $(no-shell)\nnext line"
    )
    await adapter.ensure_label(pr.number, "crucible-repair")
    assert ("git", "push", "origin", f"{'a' * 40}:refs/heads/{branch}") in calls
    assert not any("--force" in call for call in calls if call[:2] == ("git", "push"))


async def test_private_provenance_and_unknown_mechanisms_never_enter_public_body(
    database, tmp_path
):
    from crucible.application.artifact_service import ArtifactService
    from crucible.artifacts.store import LocalArtifactStore
    from crucible.domain.repair import RepairAttempt
    from crucible.repair.reporting import public_report

    repair, factory, _, _ = await git_environment(database, tmp_path)
    sentinel = "PRIVATE_HELDOUT_TRACE_secret_path_tool_arguments"
    async with factory() as uow:
        await uow.repairs.add_fix(
            RepairFix(uuid4(), repair.id, 0, sentinel, None, "exhausted")
        )
        await uow.repairs.add_attempt(
            RepairAttempt(
                uuid4(),
                repair.id,
                None,
                "diagnosis",
                1,
                None,
                sentinel,
                True,
                CLOCK.now(),
            )
        )
        await uow.commit()
    artifacts = ArtifactService(
        LocalArtifactStore(tmp_path / "artifacts", CLOCK), factory
    )
    report = await RepairReporter(factory, artifacts, CLOCK).build(repair.id)
    assert sentinel not in report.public_body
    metadata, private = await artifacts.read(report.private_artifact_id)
    assert sentinel.encode() in private and metadata.sensitivity == "private"
    assert sentinel not in public_report(
        accepted=0,
        attempted=1,
        roles={sentinel: 1},
        elapsed_seconds=0,
        mechanisms=[sentinel],
    )


async def test_real_acceptance_publishes_a_only_after_b_exhausts(database, tmp_path):
    import copy
    import subprocess
    from dataclasses import replace

    from crucible.repair.git import LocalRepairGit, RepairAcceptanceService
    from crucible.repair.incident_suite import IncidentSuite
    from tests.integration.repair.test_cycle import cycle_environment

    (
        service,
        repair,
        factory,
        controller,
        provider,
        worker,
        root,
        editable,
        diagnosis,
        review,
    ) = await cycle_environment(database, tmp_path)
    branch = f"repair/{repair.id}"
    subprocess.run(["git", "branch", "-m", branch], cwd=root, check=True)
    async with factory() as uow:
        await uow.repairs.update(replace(repair, branch=branch))
        await uow.commit()
    second = copy.deepcopy(diagnosis["fix_items"][0])
    second["proposed_case"]["case_id"] = "second_custom"
    diagnosis["fix_items"].append(second)
    tail = provider.outputs[2:]
    from crucible.engine.gateway import CompleteToolCall

    final_write = [
        replace(event, id=uuid4()) if isinstance(event, CompleteToolCall) else event
        for event in tail[-2]
    ]
    provider.outputs = [diagnosis, review, review, *tail, final_write, tail[-1]]
    run = worker.run
    second_baselines = []

    async def deterministic_worker(job, source, fixture, credential):
        result = await run(job, source, fixture, credential)
        if job.case_id == "second_custom":
            if job.purpose == "baseline":
                second_baselines.append((root / editable).read_text())
            else:
                result = replace(
                    result,
                    observation=result.observation.model_copy(
                        update={"tool_result": {"output_text": "broken"}}
                    ),
                )
        return result

    worker.run = deterministic_worker
    incidents = IncidentSuite(
        controller.evaluation.data_dir / "repair-incidents",
        factory,
        controller.artifacts,
        CLOCK,
        fixtures={},
    )
    controller.acceptance = RepairAcceptanceService(
        factory, LocalRepairGit(root, tmp_path), controller.artifacts, CLOCK, incidents
    )
    github = GitHub()
    reporter = RepairReporter(factory, controller.artifacts, CLOCK)
    controller.publication = PublicationService(factory, CLOCK, github, reporter)
    await controller.execute(repair.id)
    current = await service.get(repair.id)
    assert current.status == "exhausted" and github.creates == 1
    assert (root / editable).read_text() == "# candidate one\n"
    assert second_baselines == ["# candidate one\n"]
    assert github.pushes == [(branch, current.last_accepted_commit)]
    assert len(await incidents.list_accepted()) == 1
    report = await reporter.build(repair.id)
    assert "Accepted fixes: 1; diagnosed fixes: 2" in report.public_body
    for private in (
        "second_custom",
        "heldout_private_case",
        "# candidate one",
        "note.txt",
        "HELDOUT_SENTINEL",
    ):
        assert private not in report.public_body
