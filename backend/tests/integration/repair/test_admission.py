"""Deterministic admission plumbing; no model calls or remote credentials."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest

from crucible.application.errors import IdempotencyConflict
from crucible.application.repair_service import RepairService
from crucible.domain.run import RunStatus
from crucible.domain.task import TaskStatus
from crucible.repair.identity import RuntimeIdentity
from crucible.repair.ports import AdmissionCheck, WorktreeIdentity
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import CLOCK, seed_exchange

IDENTITY = RuntimeIdentity("a" * 40, "source", "locks")


class Git:
    check = AdmissionCheck(True, None, "Ready")
    calls = 0

    async def main_identity(self):
        return IDENTITY

    async def check_fresh_main(self):
        return self.check

    async def create_worktree(self, repair_id, baseline_commit):
        self.calls += 1
        return WorktreeIdentity(f"/repair/{repair_id}", f"repair/{repair_id}")


class GitHub:
    prs = ()

    async def list_open_repairs(self):
        return self.prs


class Scheduler:
    calls = 0
    fail = False

    async def submit(self, repair_id):
        self.calls += 1
        if self.fail:
            raise RuntimeError("private scheduler exception")

    async def stop(self, repair_id):
        pass


async def environment(database, suffix="admission", *, status=RunStatus.FAILED):
    def factory():
        return SqlAlchemyUnitOfWork(database)

    async with factory() as uow:
        task, run, _, _ = await seed_exchange(uow, suffix)
        await uow.tasks.update(replace(task, status=TaskStatus.ACTIVE))
        await uow.runs.update(replace(run, status=status))
        await uow.commit()
    git, github, scheduler = Git(), GitHub(), Scheduler()
    service = RepairService(factory, CLOCK, git, github, IDENTITY, scheduler=scheduler)
    return service, task, factory, git, github, scheduler


async def test_only_one_global_repair_and_start_retry_is_idempotent(database):
    service, task, factory, git, _, scheduler = await environment(database)
    first = await service.command(task.id, "/repair", "start-1")
    repeated = await service.command(task.id, "/repair", "start-1")
    assert first.status == "admitted"
    assert repeated == first
    assert git.calls == scheduler.calls == 1
    assert (
        await service.command(task.id, "/repair", "other")
    ).code == "resume_or_close"
    with pytest.raises(IdempotencyConflict):
        await service.command(task.id, "/repair close", "start-1")
    other, task2, *_ = await environment(database, "second")
    assert (await other.command(task2.id, "/repair", "second")).code == "repair_busy"
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id == first.repair_id
        events = await uow.events.list_after(task.id, 0)
        assert events[-1].type == "repair.updated"
        assert events[-1].run_id is None


@pytest.mark.parametrize(
    ("status", "expected"),
    [(RunStatus.COMPLETED, "nothing_eligible"), (RunStatus.RUNNING, "blocked")],
)
async def test_no_failed_or_active_run_never_provisions(database, status, expected):
    service, task, _, git, *_ = await environment(database, status=status)
    assert (await service.command(task.id, "/repair", "request")).status == expected
    assert git.calls == 0


async def test_separate_service_connection_race(database):
    first, task, factory, git, github, scheduler = await environment(database)
    second = RepairService(factory, CLOCK, git, github, IDENTITY, scheduler=scheduler)
    results = await asyncio.gather(
        first.command(task.id, "/repair", "one"),
        second.command(task.id, "/repair", "two"),
    )
    assert sorted(r.status for r in results) == ["admitted", "blocked"]
    assert git.calls == 1


async def test_scheduler_failure_preserves_queued_owner(database):
    service, task, factory, _, _, scheduler = await environment(database)
    scheduler.fail = True
    result = await service.command(task.id, "/repair", "key")
    async with factory() as uow:
        assert (await uow.repairs.get(result.repair_id)).status == "queued"
        assert (await uow.repairs.owner()).repair_id == result.repair_id


@pytest.mark.parametrize(
    "code",
    [
        "dirty_main",
        "not_main",
        "runtime_mismatch",
        "main_behind",
        "freshness_unavailable",
    ],
)
async def test_admission_failures(database, code):
    service, task, _, git, *_ = await environment(database)
    git.check = AdmissionCheck(
        False, code, "Safe admission failure", retryable=code == "freshness_unavailable"
    )
    result = await service.command(task.id, "/repair", "key")
    assert result.status == "blocked" and result.code == code
    assert git.calls == 0


async def test_github_failure_is_safe_retryable(database):
    service, task, _, _, github, _ = await environment(database)

    async def fail():
        raise RuntimeError("secret sentinel")

    github.list_open_repairs = fail
    result = await service.command(task.id, "/repair", "key")
    assert result.code == "github_unavailable"
    assert "secret" not in result.detail


async def test_stop_close_resume_and_unknown_command(database):
    service, task, *_ = await environment(database)
    result = await service.command(task.id, "/repair", "start")
    assert (
        await service.command(task.id, "/repair close", "close-active")
    ).code == "stop_first"
    stopped = await service.stop(result.repair_id, "stop")
    assert stopped.status == "stopped"
    assert (
        await service.command(task.id, "/repair resume", "resume")
    ).status == "resumed"
    await service.stop(result.repair_id, "stop-again")
    assert (await service.command(task.id, "/repair close", "close")).status == "closed"
    assert (
        await service.command(task.id, "/repair resume", "resume-closed")
    ).status == "blocked"
    with pytest.raises(ValueError):
        await service.command(task.id, "/repair nonsense", "unknown")


async def test_open_pr_and_runtime_identity_block(database):
    from crucible.repair.ports import PRIdentity

    service, task, _, git, github, _ = await environment(database)
    github.prs = (
        PRIdentity(42, "https://github.com/example/repo/pull/42", "open", "head"),
    )
    result = await service.command(task.id, "/repair", "pr")
    assert result.code == "open_repair_pr" and result.pr_urls == (github.prs[0].url,)
    github.prs = ()

    async def changed():
        return replace(IDENTITY, source_digest="drift")

    git.main_identity = changed
    assert (
        await service.command(task.id, "/repair", "drift")
    ).code == "runtime_mismatch"


async def test_worktree_failure_retains_intent_for_explicit_resume(database):
    service, task, factory, git, _, _ = await environment(database)
    original = git.create_worktree

    async def fail(*args):
        raise RuntimeError("secret-path")

    git.create_worktree = fail
    result = await service.command(task.id, "/repair", "start")
    run = await service.get(result.repair_id)
    assert run.status == "retryable_error" and "secret" not in run.outcome_detail
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
        assert (await uow.repairs.list_checkpoints(run.id))[0].result_refs_json == {
            "worktree_provision": "pending"
        }
    git.create_worktree = original
    assert (
        await service.command(task.id, "/repair resume", "resume")
    ).status == "resumed"


async def test_authenticated_repair_endpoints_intercept_commands(database):
    from httpx import ASGITransport, AsyncClient

    from crucible.api.app import create_app
    from crucible.application.auth_service import AuthService
    from crucible.application.message_service import MessageService
    from tests.contract.api.test_messages import RecordingSupervisor

    service, task, factory, *_ = await environment(database)
    supervisor = RecordingSupervisor()
    messages = MessageService(factory, CLOCK, supervisor, repairs=service)
    auth = AuthService(factory, CLOCK, "bootstrap")
    app = create_app(
        message_service=messages, repair_service=service, auth_service=auth
    )
    path = f"/api/tasks/{task.id}/messages"
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        assert (await client.get(f"/api/tasks/{task.id}/repairs")).status_code == 401
        bootstrap = await client.post(
            "/api/auth/bootstrap", json={"secret": "bootstrap"}
        )
        headers = {
            "Origin": "http://127.0.0.1:8000",
            "X-CSRF-Token": bootstrap.json()["csrfToken"],
            "Idempotency-Key": "start",
        }
        assert (await client.post(path, json={"text": "/repair"})).status_code == 403
        response = await client.post(path, json={"text": " /repair "}, headers=headers)
        assert (
            response.status_code == 202 and response.json()["kind"] == "repair_command"
        )
        repair_id = response.json()["repairId"]
        assert "runId" not in response.json()
        headers["Idempotency-Key"] = "unknown"
        invalid = await client.post(
            path, json={"text": "/repair unknown"}, headers=headers
        )
        assert (
            invalid.status_code == 422
            and invalid.json()["code"] == "invalid_repair_command"
        )
        listing = await client.get(f"/api/tasks/{task.id}/repairs")
        assert listing.json()[0]["repairId"] == repair_id
        assert "worktree" not in listing.text and "initialIdentity" not in listing.text
        stop_path = f"/api/repairs/{repair_id}/stop"
        assert (await client.post(stop_path)).status_code == 403
        headers["Idempotency-Key"] = "stop"
        assert (await client.post(stop_path, headers=headers)).json()[
            "status"
        ] == "stopped"
        assert (
            await client.post(
                stop_path,
                headers={k: v for k, v in headers.items() if k != "Idempotency-Key"},
            )
        ).status_code == 400
        headers["Idempotency-Key"] = "normal"
        assert (
            await client.post(path, json={"text": "ordinary message"}, headers=headers)
        ).json()["kind"] == "new_run"
    assert len(supervisor.submissions) == 1


async def test_stop_during_provision_keeps_owner_until_effect_finishes(database):
    service, task, factory, git, _, scheduler = await environment(database)
    entered, release = asyncio.Event(), asyncio.Event()
    original = git.create_worktree

    async def provision(*args):
        entered.set()
        await release.wait()
        return await original(*args)

    git.create_worktree = provision
    starting = asyncio.create_task(service.command(task.id, "/repair", "start"))
    await entered.wait()
    async with factory() as uow:
        repair_id = (await uow.repairs.owner()).repair_id
    stopped = await service.stop(repair_id, "stop")
    assert stopped.status == "stopped"
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id == repair_id
    release.set()
    await starting
    assert scheduler.calls == 0
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
    assert (await service.get(repair_id)).worktree_path is not None
    await service.command(task.id, "/repair resume", "resume")
    assert (await service.stop(repair_id, "stop")) == stopped
    assert (await service.get(repair_id)).status == "queued"


async def test_local_git_adapter_fetches_without_updating_main_and_reconciles_worktree(
    tmp_path,
):
    import subprocess

    from crucible.repair.admission import LocalRepairAdmissionGit

    remote, root = tmp_path / "origin.git", tmp_path / "harness"

    def git(*args, cwd=None):
        return subprocess.run(
            ["git", *map(str, args)],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "--bare", "-q", remote)
    git("init", "-b", "main", "-q", root)
    git("config", "user.name", "Test", cwd=root)
    git("config", "user.email", "test@example.test", cwd=root)
    (root / "source.txt").write_text("source")
    git("add", "source.txt", cwd=root)
    git("commit", "-qm", "baseline", cwd=root)
    git("remote", "add", "origin", remote, cwd=root)
    git("push", "-q", "origin", "main", cwd=root)
    adapter = LocalRepairAdmissionGit(root, tmp_path / "data")
    assert (await adapter.check_fresh_main()).ok
    identity = await adapter.main_identity()
    repair_id = uuid4()
    worktree = await adapter.create_worktree(repair_id, identity.commit)
    assert await adapter.create_worktree(repair_id, identity.commit) == worktree
    assert git("symbolic-ref", "--short", "HEAD", cwd=root) == "main"
    assert git("status", "--porcelain", cwd=root) == ""
    from pathlib import Path

    (Path(worktree.path) / "unexpected").write_text("user edit")
    with pytest.raises(ValueError, match="unexpected"):
        await adapter.create_worktree(repair_id, identity.commit)
    (root / "dirty").write_text("sentinel")
    assert (await adapter.check_fresh_main()).code == "dirty_main"
    assert (root / "dirty").read_text() == "sentinel"
    (root / "dirty").unlink()
    git("switch", "-qc", "feature", cwd=root)
    assert (await adapter.check_fresh_main()).code == "not_main"
    git("switch", "-q", "main", cwd=root)
    upstream = tmp_path / "upstream"
    git("clone", "-q", "--branch", "main", remote, upstream)
    git("config", "user.name", "Test", cwd=upstream)
    git("config", "user.email", "test@example.test", cwd=upstream)
    (upstream / "source.txt").write_text("remote update")
    git("add", "source.txt", cwd=upstream)
    git("commit", "-qm", "advance remote main", cwd=upstream)
    git("push", "-q", "origin", "main", cwd=upstream)
    assert (await adapter.check_fresh_main()).code == "main_behind"
    assert git("rev-parse", "HEAD", cwd=root) == identity.commit
    assert (root / "source.txt").read_text() == "source"


async def test_github_adapter_uses_configured_origin_and_catches_unlabelled_repairs(
    tmp_path,
):
    import json

    from crucible.repair.admission import LocalRepairAdmissionGitHub

    class Process:
        calls = []

        async def run(self, root, *argv):
            self.calls.append(argv)
            if argv[0] == "git":
                return "git@github.com:example/crucible.git"
            return json.dumps(
                [
                    [
                        {
                            "number": 1,
                            "html_url": "https://github.com/example/crucible/pull/1",
                            "state": "open",
                            "head": {"ref": "repair/abc", "sha": "a"},
                            "labels": [],
                        }
                    ],
                    [
                        {
                            "number": 2,
                            "html_url": "https://github.com/example/crucible/pull/2",
                            "state": "open",
                            "head": {"ref": "other", "sha": "b"},
                            "labels": [{"name": "crucible-repair"}],
                        }
                    ],
                ]
            )

    process = Process()
    prs = await LocalRepairAdmissionGitHub(tmp_path, process).list_open_repairs()
    assert [pr.number for pr in prs] == [1, 2]
    assert process.calls[-1] == (
        "gh",
        "api",
        "--paginate",
        "--slurp",
        "repos/example/crucible/pulls?state=open&per_page=100",
    )


async def test_stop_can_journal_on_another_connection_and_retry_failure(database):
    from crucible.application.repair_service import RepairStopFailed

    service, task, factory, _, _, scheduler = await environment(database)
    result = await service.command(task.id, "/repair", "key")
    fail = True

    async def stop(repair_id):
        async with factory() as uow:
            await uow.repairs.lock()
            assert (await uow.repairs.owner()).repair_id == repair_id
            await uow.commit()
        if fail:
            raise RuntimeError("private cancellation trace")

    scheduler.stop = stop
    with pytest.raises(RepairStopFailed):
        await service.stop(result.repair_id, "stop")
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id == result.repair_id
    assert (
        await service.command(task.id, "/repair resume", "resume")
    ).code == "repair_busy"
    fail = False
    assert (await service.stop(result.repair_id, "stop")).status == "stopped"


async def test_cancelled_provision_is_explicitly_resumable(database):
    service, task, factory, git, _, _ = await environment(database)
    entered = asyncio.Event()
    original = git.create_worktree

    async def provision(*args):
        entered.set()
        await asyncio.Future()

    git.create_worktree = provision
    request = asyncio.create_task(service.command(task.id, "/repair", "start"))
    await entered.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    async with factory() as uow:
        assert (await uow.repairs.owner()).repair_id is None
        run = (await uow.repairs.list_for_task(task.id))[0]
        assert run.status == "retryable_error"
    git.create_worktree = original
    assert (
        await service.command(task.id, "/repair resume", "resume")
    ).status == "resumed"
