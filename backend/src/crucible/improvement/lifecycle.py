"""Explicit local lifecycle; recovery reconciles effects and never executes trials."""

import asyncio
import hashlib
import json
import os
import re
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID, uuid4

import uvicorn

from crucible.benchmarks.evidence import atomic_json
from crucible.benchmarks.swebench import SwebenchGrader
from crucible.domain.clock import SystemClock
from crucible.improvement.broker import ImprovementBroker, broker_app
from crucible.improvement.checks import DockerProbes, check_candidate, suite_digest
from crucible.improvement.contracts import (
    EDITABLE_PATHS,
    Contract,
    ExperimentManifest,
    ExperimentPaths,
    HarnessSnapshot,
)
from crucible.improvement.manifest import ManifestDrift, freeze_manifest
from crucible.improvement.source import (
    _digest,
    git_bytes,
    inspect_source,
    safe_path,
    stage_harness,
)
from crucible.improvement.store import (
    ExperimentRecord,
    ExperimentStore,
    RevisionConflict,
)
from crucible.improvement.trials import IsolatedTrialRunner
from crucible.improvement.worker_supervisor import (
    DockerCommands,
    DockerPort,
    DockerRuntime,
    WorkerSupervisor,
    read_regular_artifact,
)
from crucible.improvement.worktree import ExperimentWorktree, OwnedWorktree
from crucible.models.responses_gateway import LiteLLMResponsesGateway
from crucible.repair.admission import LocalRepairAdmissionGitHub
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.identity import canonical_json


class RecoveryRequired(RuntimeError):
    pass


class StartConfig(Contract):
    schema_version: Literal[1]
    manifest_file: Path
    repository: Path
    registry_root: Path
    protected_suite_root: Path


class AdmissionPort(Protocol):
    async def check(
        self, config: StartConfig, manifest: ExperimentManifest
    ) -> None: ...


async def bounded_process(root: Path | None, *argv: str, limit: int = 65536) -> str:
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(30):
            assert process.stdout is not None
            chunks = []
            total = 0
            while chunk := await process.stdout.read(65536):
                total += len(chunk)
                if total > limit:
                    raise RecoveryRequired("admission response too large")
                chunks.append(chunk)
            await process.wait()
            if process.returncode:
                raise RecoveryRequired("admission command failed")
            return b"".join(chunks).decode().strip()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


class LocalAdmission:
    async def check(self, config: StartConfig, manifest: ExperimentManifest) -> None:
        local = (
            (await asyncio.to_thread(git_bytes, config.repository, "rev-parse", "HEAD"))
            .decode()
            .strip()
        )
        branch = (
            (
                await asyncio.to_thread(
                    git_bytes, config.repository, "symbolic-ref", "--short", "HEAD"
                )
            )
            .decode()
            .strip()
        )
        dirty = await asyncio.to_thread(
            git_bytes,
            config.repository,
            "status",
            "--porcelain",
            "--untracked-files=all",
        )
        if branch != "main" or dirty or local != manifest.initial_commit:
            raise ManifestDrift("clean frozen main required")
        runtime = Path(__file__).parents[4]
        actual = await asyncio.to_thread(
            inspect_source, runtime, manifest.initial_commit
        )
        controller = _digest(
            {n: v for n, v in actual.files.items() if n not in EDITABLE_PATHS}
        )
        if (
            actual.source_digest != manifest.initial_source_digest
            or controller != manifest.controller_digest
        ):
            raise ManifestDrift("loaded controller source identity differs")
        # Read-only remote admission: never fetch/pull, push or edit a PR.
        remote = await bounded_process(
            config.repository,
            "git",
            "ls-remote",
            "--exit-code",
            "origin",
            "refs/heads/main",
        )
        if not remote or remote.split()[0] != local:
            raise RecoveryRequired("remote main freshness unavailable or different")
        github = LocalRepairAdmissionGitHub(config.repository)
        origin = (
            (
                await asyncio.to_thread(
                    git_bytes, config.repository, "remote", "get-url", "origin"
                )
            )
            .decode()
            .strip()
        )
        repo = github._repository(origin)
        github_raw = await bounded_process(
            config.repository,
            "gh",
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repo}/pulls?state=open&per_page=100",
            limit=8 * 1024 * 1024,
        )
        for page in json.loads(github_raw):
            for pr in page:
                names = {label["name"] for label in pr["labels"]}
                if pr["head"]["ref"].startswith(
                    ("repair/", "codex/improve/")
                ) or names & {"crucible-repair", "crucible-improvement"}:
                    raise RevisionConflict(
                        "open repair/improvement PR blocks admission"
                    )


class ExperimentService:
    def __init__(
        self,
        store: ExperimentStore,
        root: Path,
        *,
        admission: AdmissionPort | None = None,
        docker: DockerPort | None = None,
    ) -> None:
        safe_path(root)
        self.store, self.root = store, root
        self.admission = admission or LocalAdmission()
        self.docker = docker or DockerRuntime()

    def _config_path(self, id: UUID) -> Path:
        return self.root / "runs" / str(id) / "config.json"

    def _load_config(self, id: UUID) -> StartConfig:
        return StartConfig.model_validate_json(
            read_regular_artifact(self._config_path(id), 1_048_576)
        )

    async def _validate(self, record: ExperimentRecord) -> StartConfig:
        config = self._load_config(record.id)
        frozen = freeze_manifest(
            config.manifest_file,
            config.registry_root,
            authorize_disposable_commands=record.manifest.disposable_commands_authorized,
        )
        from crucible.improvement.manifest import manifest_digest

        if manifest_digest(frozen) != record.manifest_hash:
            raise ManifestDrift("frozen inputs changed; new experiment required")
        inspection = await asyncio.to_thread(
            inspect_source, config.repository, record.initial_commit
        )
        if inspection.source_digest != record.manifest.initial_source_digest:
            raise ManifestDrift("initial source drift")
        if (
            hashlib.sha256(
                read_regular_artifact(
                    config.repository / "backend/uv.lock", 2 * 1024 * 1024
                )
            ).hexdigest()
            != frozen.dependency_lock_digest
        ):
            raise ManifestDrift("dependency lock drift")
        if suite_digest(config.protected_suite_root) != frozen.boundary_suite_digest:
            raise ManifestDrift("protected suite drift")
        await self.admission.check(config, frozen)
        return config

    async def _repository_lock(
        self, config: StartConfig, id: UUID, *, release: bool = False
    ) -> None:
        common = (
            (
                await asyncio.to_thread(
                    git_bytes,
                    config.repository,
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-common-dir",
                )
            )
            .decode()
            .strip()
        )
        path = Path(common) / "crucible-improvement-owner.json"
        safe_path(path)
        value = {"experiment_id": str(id), "state_root": str(self.root)}
        if path.exists():
            if json.loads(read_regular_artifact(path, 65536)) != value:
                if release:
                    return  # This run never acquired the foreign lock.
                raise RevisionConflict(
                    "repository owned by another state root or experiment"
                )
            if release:
                path.unlink()
            return
        if release:
            return
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(canonical_json(value))
            output.flush()
            os.fsync(output.fileno())

    async def _worktree(
        self, config: StartConfig, record: ExperimentRecord
    ) -> OwnedWorktree:
        return await ExperimentWorktree(self.root).create(
            config.repository, record.id, record.initial_commit
        )

    async def _save_snapshot(
        self, id: UUID, snapshot: HarnessSnapshot, path: Path
    ) -> UUID:
        for row in await self.store.snapshots(id):
            if (row["source_digest"], row["export_digest"]) == (
                snapshot.source_digest,
                snapshot.export_digest,
            ):
                old = UUID(str(row["id"]))
                if row["status"] == "interrupted":
                    snapshot = snapshot.model_copy(update={"snapshot_id": old})
                    atomic_json(path / "snapshot.json", snapshot)
                    await self.store.reactivate_snapshot(
                        id, old, str((path / "snapshot.json").relative_to(self.root))
                    )
                return old
        await self.store.register_snapshot(
            id, snapshot, str((path / "snapshot.json").relative_to(self.root))
        )
        return snapshot.snapshot_id

    async def start(
        self, config: Path, idempotency_key: str, authorize_commands: bool = False
    ) -> ExperimentRecord:
        settings = StartConfig.model_validate_json(
            read_regular_artifact(config, 1_048_576)
        )
        for path in (
            settings.repository,
            settings.registry_root,
            settings.protected_suite_root,
            settings.manifest_file,
        ):
            safe_path(path)
        raw = json.loads(read_regular_artifact(settings.manifest_file, 1_048_576))
        if (
            raw["command_policy"]["mode"] == "preauthorized-disposable"
            and not authorize_commands
        ):
            raise ValueError("--authorize-disposable-commands required")
        manifest = freeze_manifest(
            settings.manifest_file,
            settings.registry_root,
            authorize_disposable_commands=authorize_commands,
        )
        await self.admission.check(settings, manifest)
        if (
            suite_digest(settings.protected_suite_root)
            != manifest.boundary_suite_digest
        ):
            raise ManifestDrift("suite digest mismatch")
        inspection = await asyncio.to_thread(
            inspect_source, settings.repository, manifest.initial_commit
        )
        if inspection.source_digest != manifest.initial_source_digest:
            raise ManifestDrift("source digest mismatch")
        if (
            hashlib.sha256(
                read_regular_artifact(
                    settings.repository / "backend/uv.lock", 2 * 1024 * 1024
                )
            ).hexdigest()
            != manifest.dependency_lock_digest
        ):
            raise ManifestDrift("dependency lock mismatch")
        config_path = self._config_path(manifest.experiment_id)
        config_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if (
            config_path.exists()
            and StartConfig.model_validate_json(
                read_regular_artifact(config_path, 1_048_576)
            )
            != settings
        ):
            raise ManifestDrift("runtime paths changed")
        if not config_path.exists():
            atomic_json(config_path, json.loads(settings.model_dump_json()))
        record = await self.store.create(
            manifest, str(settings.repository.resolve()), idempotency_key
        )
        if record.status == "closed":
            return record
        owner = await self.store.claim_execution(
            record.id,
            "admission",
            process_id=os.getpid(),
            process_start=await self._process_start(os.getpid()),
        )
        try:
            await self._repository_lock(settings, record.id)
            if await self.store.snapshots(record.id):
                return record
            effect = await self.store.begin_effect(
                record.id,
                None,
                "worktree",
                {"name": str(self.root / "worktrees" / str(record.id))},
                {"experiment": str(record.id)},
            )
            worktree = await self._worktree(settings, record)
            await self.store.effect_state(effect, "active", str(worktree.path))
            output = self.root / "snapshots" / str(uuid4())
            snapshot = await asyncio.to_thread(
                stage_harness,
                worktree.path,
                record.initial_commit,
                output,
                EDITABLE_PATHS,
            )
            await self._save_snapshot(record.id, snapshot, output)
            await self.store.effect_state(effect, "stopped")
        except BaseException:
            await self.store.fail_run(record.id, "start_interrupted")
            raise
        finally:
            await self.store.release_execution(record.id, owner, "admission")
        return await self.store.get(record.id)

    async def status(self, id: UUID) -> dict[str, object]:
        record = await self.store.get(id)
        trials = await self.store.trials(id)
        return {
            "experiment_id": str(id),
            "status": record.status,
            "stage": record.stage,
            "revision": record.revision,
            "manifest_digest": record.manifest_hash,
            "stop_requested": record.stop_requested,
            "selection_trials_scheduled": record.manifest.baseline_trial_count,
            "planned_trial_upper_bound": record.manifest.planned_trial_upper_bound,
            "completed_trials": sum(r["status"] == "complete" for r in trials),
            "uncertain_usage": await self.store.unsettled_usage(id),
        }

    @asynccontextmanager
    async def _runtime(
        self, manifest: ExperimentManifest
    ) -> AsyncIterator[WorkerSupervisor]:
        gateway = ScopedModelGateway(
            LiteLLMResponsesGateway(),
            clock=SystemClock(),
            endpoint="http://broker/internal/improvement/model",
        )
        broker = ImprovementBroker(
            self.store,
            gateway,
            DockerCommands(self.store, self.docker),
            approval=self._approve,
        )
        listener = socket.socket()
        listener.bind(("0.0.0.0", 0))
        listener.listen(128)
        listener.setblocking(False)
        port = listener.getsockname()[1]

        class BrokerServer(uvicorn.Server):
            @contextmanager
            def capture_signals(self):  # type: ignore[no-untyped-def]
                yield

        server = BrokerServer(
            uvicorn.Config(
                broker_app(broker),
                log_level="critical",
                access_log=False,
                lifespan="off",
            )
        )
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            for _ in range(100):
                if server.started:
                    break
                if task.done():
                    await task
                await asyncio.sleep(0.01)
            if not server.started:
                raise RecoveryRequired("local broker did not start")
            yield WorkerSupervisor(
                self.store,
                broker,
                self.docker,
                self.root,
                manifest,
                f"http://host.docker.internal:{port}",
            )
        finally:
            await asyncio.gather(
                *(broker.stop(job_id) for job_id in tuple(broker.sessions)),
                return_exceptions=True,
            )
            server.should_exit = True
            try:
                await asyncio.wait_for(task, 10)
            except TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            listener.close()

    async def _approve(self, job: object, spec: object) -> bool:
        if not os.isatty(0):
            return False
        # Serialize stdin reads; the broker pauses the worker before this callback.
        print(
            "Disposable task command approval:",
            canonical_json(spec).decode(),
            flush=True,
        )
        print("Approve this command? [y/N] ", end="", flush=True)
        loop = asyncio.get_running_loop()
        answer: asyncio.Future[bytes] = loop.create_future()

        def readable() -> None:
            if not answer.done():
                try:
                    answer.set_result(os.read(0, 4096))
                except OSError as error:
                    answer.set_exception(error)

        loop.add_reader(0, readable)
        try:
            return (await answer).strip().lower() == b"y"
        finally:
            loop.remove_reader(0)

    async def baseline(self, id: UUID) -> dict[str, object]:
        return await self._evaluate(id, False)

    async def evaluate_candidate(self, id: UUID) -> dict[str, object]:
        return await self._evaluate(id, True)

    async def _evaluate(self, id: UUID, candidate: bool) -> dict[str, object]:
        record = await self.store.get(id)
        config = await self._validate(record)
        owner = await self.store.claim_execution(
            id,
            "candidate_checks" if candidate else "baseline",
            process_id=os.getpid(),
            process_start=await self._process_start(os.getpid()),
        )
        response: dict[str, object] = {}

        async def heartbeat() -> None:
            while True:
                await self.store.heartbeat(id, owner)
                await asyncio.sleep(2)

        pulse = asyncio.create_task(heartbeat())
        try:
            await self._repository_lock(config, id)
            worktree = await self._worktree(config, record)
            if candidate:
                output = self.root / "snapshots" / str(uuid4())
                snapshot = await asyncio.to_thread(
                    stage_harness,
                    worktree.path,
                    record.initial_commit,
                    output,
                    EDITABLE_PATHS,
                )
                if snapshot.kind != "candidate":
                    raise ValueError("no-effect candidate")
                await worktree.checkpoint_owned_edits()
                snapshot_id = await self._save_snapshot(id, snapshot, output)
                snapshot = snapshot.model_copy(update={"snapshot_id": snapshot_id})
                checks_root = self.root / "checks" / str(uuid4())
                result = await check_candidate(
                    snapshot,
                    record.manifest,
                    config.protected_suite_root,
                    checks_root,
                    harness_export=output / "harness",
                    probes=DockerProbes(self.store, self.docker, record.manifest),
                )
                result_file = checks_root / "result.json"
                await self.store.record_event(
                    id,
                    "candidate_checks",
                    {
                        "path": str(result_file.relative_to(self.root)),
                        "sha256": hashlib.sha256(result_file.read_bytes()).hexdigest(),
                    },
                )
                if result.verdict != "pass":
                    if result.verdict == "error":
                        await self.store.fail_run(
                            id,
                            "operator_stopped"
                            if result.reason == "operator_stopped"
                            else "candidate_check_infrastructure",
                            interrupted=result.reason == "operator_stopped",
                        )
                    response["candidate_checks"] = result.verdict
                    return response
            else:
                snapshots = await self.store.snapshots(id)
                snapshot_id = UUID(
                    str(next(s for s in snapshots if s["kind"] == "baseline")["id"])
                )
            async with self._runtime(record.manifest) as supervisor:
                paths = ExperimentPaths(
                    root=self.root,
                    registry_root=config.registry_root,
                    repository=config.repository,
                )
                runner = IsolatedTrialRunner(
                    self.store,
                    paths,
                    supervisor,
                    SwebenchGrader(record.manifest.grader),
                    config.protected_suite_root,
                )
                results = await runner.run_selection_baseline(id, snapshot_id)
                if any(r.status != "complete" for r in results):
                    await self.store.fail_run(id, "trial_infrastructure")
            return response
        except asyncio.CancelledError:
            await self.store.fail_run(id, "operator_interrupted", interrupted=True)
            raise
        except BaseException:
            await self.store.fail_run(id, "evaluation_error")
            raise
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)
            await self.store.release_execution(id, owner, "awaiting_search")
            response.update(await self.status(id))

    async def _process_start(self, pid: int) -> str | None:
        process = await asyncio.create_subprocess_exec(
            "ps",
            "-p",
            str(pid),
            "-o",
            "lstart=",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            raw, _ = await asyncio.wait_for(process.communicate(), 5)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
        if process.returncode or not raw.strip():
            return None
        if len(raw) > 256:
            raise RecoveryRequired("process identity unavailable")
        return raw.decode().strip()

    async def _wait_owner(self, id: UUID) -> None:
        for _ in range(30):
            owner = await self.store.execution_owner(id)
            if owner is None:
                return
            events = await self.store.events(id, "execution_claimed")
            event = next((e for e in reversed(events) if e["owner"] == owner), None)
            if (
                not event
                or not isinstance(event.get("pid"), int)
                or not event.get("process_start")
            ):
                raise RecoveryRequired("controller process ownership uncertain")
            pid = event.get("pid")
            assert isinstance(pid, int)
            actual = await self._process_start(pid)
            if actual != event["process_start"]:
                return
            await asyncio.sleep(0.1)
        raise RecoveryRequired("controller is still active; stop remains requested")

    async def _reconcile(self, id: UUID) -> None:
        failures = []
        for effect in reversed(await self.store.effects(id)):
            if effect["status"] == "stopped":
                continue
            eid = UUID(str(effect["id"]))
            kind = str(effect["kind"])
            intent = json.loads(str(effect["intent_json"]))
            expected = json.loads(str(effect["ownership_labels_json"]))
            try:
                if kind == "worktree":
                    pass  # Retained checkout is reconciled separately, never deleted.
                elif kind == "grading":
                    args = ["ps", "-aq"]
                    for key, value in expected.items():
                        args += ["--filter", f"label={key}={value}"]
                    identities = (await self.docker.invoke(*args)).split()
                    for identity in identities:
                        info = json.loads(
                            await self.docker.invoke("inspect", identity)
                        )[0]
                        if any(
                            info["Config"]["Labels"].get(k) != v
                            for k, v in expected.items()
                        ):
                            raise RecoveryRequired("grader ownership ambiguous")
                        if info["State"]["Running"]:
                            await self.docker.invoke("kill", identity)
                        info = json.loads(
                            await self.docker.invoke("inspect", identity)
                        )[0]
                        if info["State"]["Running"]:
                            raise RecoveryRequired("grader remains running")
                else:
                    name = intent["name"]
                    if kind == "network":
                        matches = await self.docker.invoke(
                            "network",
                            "ls",
                            "--filter",
                            "name=^" + re.escape(name) + "$",
                            "--format",
                            "{{.ID}}",
                        )
                        if matches:
                            info = json.loads(
                                await self.docker.invoke("network", "inspect", name)
                            )[0]
                            if any(
                                info["Labels"].get(k) != v for k, v in expected.items()
                            ):
                                raise RecoveryRequired("network ownership ambiguous")
                    else:
                        matches = await self.docker.invoke(
                            "ps", "-aq", "--filter", "name=^/" + re.escape(name) + "$"
                        )
                        if matches:
                            info = json.loads(
                                await self.docker.invoke("inspect", name)
                            )[0]
                            if any(
                                info["Config"]["Labels"].get(k) != v
                                for k, v in expected.items()
                            ):
                                raise RecoveryRequired("container ownership ambiguous")
                            if info["State"]["Running"]:
                                await self.docker.invoke("kill", name)
                            if json.loads(await self.docker.invoke("inspect", name))[0][
                                "State"
                            ]["Running"]:
                                raise RecoveryRequired("container remains running")
                await self.store.effect_state(eid, "stopped")
            except Exception as error:
                await self.store.effect_state(eid, "uncertain")
                failures.append(error)
        if failures:
            raise RecoveryRequired("owned effects require operator recovery")

    async def _claim_recovery(self, id: UUID) -> UUID:
        await self.store.request_stop(id)
        expected = await self.store.execution_owner(id)
        await self._wait_owner(id)
        current = await self.store.execution_owner(id)
        if current is not None and current != expected:
            raise RevisionConflict("controller ownership changed during recovery")
        record = await self.store.get(id)
        return await self.store.claim_recovery(
            id,
            record.revision,
            current,
            os.getpid(),
            await self._process_start(os.getpid()),
        )

    async def stop(self, id: UUID) -> dict[str, object]:
        if (await self.store.get(id)).status == "closed":
            return await self.status(id)
        owner = await self._claim_recovery(id)
        try:
            await self._reconcile(id)
            await self.store.fail_run(id, "operator_stopped", interrupted=True)
        finally:
            await self.store.release_execution(
                id, owner, (await self.store.get(id)).stage
            )
        return await self.status(id)

    async def resume(self, id: UUID) -> dict[str, object]:
        if await self.store.unsettled_usage(id):
            raise RecoveryRequired(
                "uncertain or unfinished usage blocks resume; close retains spending"
            )
        record = await self.store.get(id)
        if record.status not in ("running", "error", "interrupted", "stopping"):
            raise RevisionConflict("explicit resume requires interrupted/error state")
        config = await self._validate(record)
        owner = await self._claim_recovery(id)
        try:
            await self._repository_lock(config, id)
            await self._reconcile(id)
            worktree = await self._worktree(config, record)
            await worktree.reconcile_proposal_transfer()
            inspection = await worktree.inspect()
            if inspection.changed_paths:
                await worktree.archive_interrupted(uuid4())
                await worktree.restore_last_accepted(
                    record.manifest.initial_source_digest
                )
            await self.store.invalidate_interrupted(id)
            await self.store.recover_ready(id, owner)
        finally:
            if await self.store.execution_owner(id) == str(owner):
                await self.store.release_execution(id, owner, record.stage)
        return await self.status(id)

    async def close(self, id: UUID) -> dict[str, object]:
        if (await self.store.get(id)).status == "closed":
            await self._repository_lock(self._load_config(id), id, release=True)
            return await self.status(id)
        owner = await self._claim_recovery(id)
        try:
            await self._reconcile(id)
            await self.store.finish_close(id, owner)
            await self._repository_lock(self._load_config(id), id, release=True)
        finally:
            if await self.store.execution_owner(id) == str(owner):
                await self.store.release_execution(
                    id, owner, (await self.store.get(id)).stage
                )
        return await self.status(id)
