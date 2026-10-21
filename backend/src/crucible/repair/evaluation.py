"""Controller adapter for protected custom, fixed and private incident trials."""

import asyncio
import json
from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.evals import EvalBudgets
from crucible.domain.repair import RepairRun
from crucible.evals.manifests import EvalPartition, EvalSuiteDefinition, load_case
from crucible.models.token_counting import LocalTokenEstimator
from crucible.repair.cases import ApprovedCase, relative_path
from crucible.repair.controller import RepairInfrastructureError, TrialBatch
from crucible.repair.fingerprints import (
    EvaluationIdentity,
    capture_source_identity,
    fingerprint,
)
from crucible.repair.gateway import ScopedModelGateway
from crucible.repair.identity import RuntimeIdentity, digest
from crucible.repair.incident_suite import IncidentSuite
from crucible.repair.trials import TrialService, fixture_manifest
from crucible.repair.verification import VERIFIER_VERSION


class RepairEvaluation:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        artifacts: ArtifactService,
        *,
        main_root: Path,
        data_dir: Path,
        runtime_identity: RuntimeIdentity,
        model: str,
        worker_factory: Callable[[], Any],
        gateway: ScopedModelGateway,
        fixtures: Any,
        evaluators: Any,
        image: str,
        instruction_digest: str,
        tool_schema_digest: str,
        prompt_digest: str,
        budgets: EvalBudgets = EvalBudgets(),
    ) -> None:
        self.factory, self.clock, self.artifacts = unit_of_work, clock, artifacts
        self.main_root, self.data_dir, self.runtime, self.model = (
            main_root,
            data_dir,
            runtime_identity,
            model,
        )
        (
            self.worker_factory,
            self.gateway,
            self.fixtures,
            self.evaluators,
            self.image,
        ) = worker_factory, gateway, fixtures, evaluators, image
        (
            self.instruction_digest,
            self.tool_schema_digest,
            self.prompt_digest,
            self.budgets,
        ) = instruction_digest, tool_schema_digest, prompt_digest, budgets
        self.active: dict[UUID, asyncio.Task[Any]] = {}
        self.jobs: dict[UUID, set[UUID]] = {}

    async def identity(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
    ) -> str:
        source = await capture_source_identity(Path(repair.worktree_path or ""))
        fixed = [
            self._fixed(partition).suite_digest
            for partition in (EvalPartition.DEVELOPMENT, EvalPartition.HELD_OUT)
        ]
        library = IncidentSuite(
            self.data_dir / "repair-incidents",
            self.factory,
            self.artifacts,
            self.clock,
            fixtures={},
        )
        incidents = await library.list_accepted()
        return digest(
            {
                "custom": self._fingerprint(
                    source,
                    case.case.fixture_manifest_digest,
                    case.case_digest,
                    3 if case.case.replay_mode == "fresh_call" else 1,
                ),
                "fixed": fixed,
                "incidents": [record.case_digest for record in incidents],
            }
        )

    async def baseline(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
    ) -> TrialBatch:
        return await self._run(repair, case, fixture_files, "baseline", 0)

    async def candidate(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        fixture_files: dict[str, str | None],
        generation: int,
    ) -> TrialBatch:
        return await self._run(repair, case, fixture_files, "candidate", generation)

    async def stop(self, repair_id: UUID) -> None:
        for job in self.jobs.get(repair_id, ()):
            self.gateway.revoke(job)
        task = self.active.get(repair_id)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _fixture(self, case: ApprovedCase, files: dict[str, str | None]) -> Path:
        root = self.data_dir / "repair-fixtures" / case.case.fixture_manifest_digest
        if root.is_symlink() or any(p.is_symlink() for p in root.parents):
            raise RepairInfrastructureError("Unsafe fixture directory")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name, content in files.items():
            relative_path(name)
            if content is not None:
                path = root / name
                if path.is_symlink() or any(
                    p.is_symlink()
                    for p in path.parents
                    if p != root and p.is_relative_to(root)
                ):
                    raise RepairInfrastructureError("Unsafe fixture path")
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                if path.exists() and path.read_text() != content:
                    raise RepairInfrastructureError("Fixture contents changed")
                if not path.exists():
                    path.write_text(content)
        if digest(fixture_manifest(root)) != case.case.fixture_manifest_digest:
            raise RepairInfrastructureError("Fixture manifest differs")
        return root

    def _fixed(self, partition: EvalPartition) -> EvalSuiteDefinition:
        root = self.main_root / "evals" / partition.value
        names = sorted(p.stem for p in (root / "cases").glob("*.toml"))
        if not names:
            raise RepairInfrastructureError("Fixed evaluation suite is unavailable")
        cases = tuple(
            replace(
                load_case(partition, name, root=root),
                model=self.model,
                budgets=self.budgets,
            )
            for name in names
        )
        return EvalSuiteDefinition(
            "repair-fixed",
            cases,
            digest([case.case_digest for case in cases]),
            partition,
        )

    def _fingerprint(
        self, source: Any, fixture_digest: str, eval_digest: str, repeats: int
    ) -> str:
        return fingerprint(
            EvaluationIdentity(
                source=source,
                model=self.model,
                parameters={},
                fixture_digest=fixture_digest,
                eval_digest=eval_digest,
                instruction_digest=self.instruction_digest,
                tool_schema_digest=self.tool_schema_digest,
                prompt_digest=self.prompt_digest,
                lock_digest=self.runtime.lock_digest,
                image_digest=self.image,
                verifier_version=VERIFIER_VERSION,
                estimator_version=LocalTokenEstimator.version,
                adapter_version="repair-worker-v1",
                resource_policy={
                    "network": "scoped-model-relay",
                    "cpus": 1,
                    "memory_bytes": 1073741824,
                    "pids": 256,
                },
                budgets=asdict(self.budgets),
                repeats=repeats,
            )
        )

    async def _run(
        self,
        repair: RepairRun,
        case: ApprovedCase,
        files: dict[str, str | None],
        side: str,
        generation: int,
    ) -> TrialBatch:
        task = asyncio.current_task()
        if task is not None:
            self.active[repair.id] = task
        try:
            if not self.image or "@sha256:" not in self.image:
                raise RepairInfrastructureError(
                    "A digest-pinned repair worker image is required"
                )
            worker = self.worker_factory()
            root = Path(repair.worktree_path or "")
            source = await capture_source_identity(root)
            fixture_map = {
                case.case.fixture_manifest_digest: self._fixture(case, files)
            }

            async def credential(job: Any) -> Any:
                self.jobs.setdefault(repair.id, set()).add(job.job_id)
                return self.gateway.issue(
                    job.job_id,
                    self.model,
                    {
                        "max_requests": self.budgets.max_steps,
                        "max_tokens": self.budgets.max_model_tokens,
                    },
                    timedelta(seconds=600),
                )

            trials = TrialService(
                self.factory,
                worker,
                source_root=root,
                fixtures=fixture_map,
                artifacts=self.artifacts,
                clock=self.clock,
                invocation_id=repair.id,
                candidate_generation=generation,
                credential_factory=credential,
                repair_id=repair.id,
            )
            repeats = 3 if case.case.replay_mode == "fresh_call" else 1
            custom_fingerprint = self._fingerprint(
                source, case.case.fixture_manifest_digest, case.case_digest, repeats
            )
            custom = {
                case.case.case_id: tuple(
                    [
                        await trials.run(case, source, side, repeat, custom_fingerprint)
                        for repeat in range(repeats)
                    ]
                )
            }
            # Reproduction is a hard gate. Do not run paid fixed suites when the
            # custom baseline has not verified the intended historical failure.
            if side == "baseline" and not any(
                row.verdict == "pass" for row in custom[case.case.case_id]
            ):
                return TrialBatch(custom, {}, {})
            fixed = []
            for partition in (EvalPartition.DEVELOPMENT, EvalPartition.HELD_OUT):
                suite = self._fixed(partition)
                fingerprints = {
                    item.case_id: self._fingerprint(
                        source,
                        item.case_digest,
                        digest(
                            {
                                "suite": suite.suite_digest,
                                "case": item.case_digest,
                                "settings": asdict(item.repository_settings),
                                "budgets": asdict(item.budgets),
                            }
                        ),
                        3,
                    )
                    for item in suite.cases
                }
                fixed.append(
                    await trials.run_repository_suite(
                        suite,
                        source,
                        side,
                        fingerprints,
                        fixtures=self.fixtures,
                        evaluators=self.evaluators(partition)
                        if callable(self.evaluators)
                        else self.evaluators,
                        task_id=repair.source_task_id,
                        run_id=case.case.incident_ids["run"],
                    )
                )
            incidents, approved = {}, {}
            library = IncidentSuite(
                self.data_dir / "repair-incidents",
                self.factory,
                self.artifacts,
                self.clock,
                fixtures=fixture_map,
            )
            for record in await library.list_accepted():
                _, raw = await self.artifacts.read(record.definition_artifact_id)
                prior = ApprovedCase.model_validate_json(raw)
                _, manifest_raw = await self.artifacts.read(
                    record.fixture_manifest_artifact_id
                )
                manifest = json.loads(manifest_raw)
                fixture_map[prior.case.fixture_manifest_digest] = self._fixture(
                    prior, manifest["files"]
                )
                name = f"incident-{prior.case_digest}"
                count = 3 if prior.case.replay_mode == "fresh_call" else 1
                identity = self._fingerprint(
                    source, prior.case.fixture_manifest_digest, prior.case_digest, count
                )
                observations = []
                for repeat in range(count):
                    row = await trials.run(
                        prior, source, "regression", repeat, identity
                    )
                    observations.append(replace(row, side=side, case_id=name))
                incidents[name], approved[name] = tuple(observations), prior
            if await capture_source_identity(root) != source:
                raise RepairInfrastructureError("Source changed during validation")
            return TrialBatch(custom, fixed[0], fixed[1], incidents, approved)
        finally:
            self.active.pop(repair.id, None)
