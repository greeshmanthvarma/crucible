"""Independent measurement after an immutable search seal; never selection feedback."""

from dataclasses import asdict
from typing import Annotated, Literal, Protocol
from uuid import UUID

from pydantic import Field

from crucible.benchmarks.contracts import Commit, Sha256, TrialResult
from crucible.improvement.contracts import Contract, ExperimentPaths
from crucible.improvement.selection import EvaluationSet, ScoredTrial
from crucible.improvement.store import ExperimentStore, RevisionConflict
from crucible.improvement.trials import build_evaluation_set
from crucible.repair.identity import digest


class FinalTrialPort(Protocol):
    async def run_final(
        self,
        experiment_id: UUID,
        snapshot_id: UUID,
        case_id: Annotated[str, Field(min_length=1, max_length=200)],
        repeat_index: int,
        seal_digest: str,
    ) -> TrialResult: ...


class MeasuredCase(Contract):
    case_id: Annotated[str, Field(min_length=1, max_length=200)]
    split: Literal["development", "selection-validation", "final-test"]
    initial_passes: Annotated[int, Field(ge=0, le=3)] | None
    final_passes: Annotated[int, Field(ge=0, le=3)] | None
    initial_trials: Annotated[tuple[ScoredTrial, ...], Field(max_length=3)]
    final_trials: Annotated[tuple[ScoredTrial, ...], Field(max_length=3)]


class SplitMeasurement(Contract):
    split: Literal["development", "selection-validation", "final-test"]
    expected_tasks: Annotated[int, Field(ge=0, le=4096)]
    expected_trials_per_version: Annotated[int, Field(ge=0, le=12288)]
    initial_passed_trials: Annotated[int, Field(ge=0, le=12288)] | None
    final_passed_trials: Annotated[int, Field(ge=0, le=12288)] | None
    initial_successful_tasks: Annotated[int, Field(ge=0, le=4096)] | None
    final_successful_tasks: Annotated[int, Field(ge=0, le=4096)] | None


class MeasurementError(Contract):
    snapshot_id: UUID
    case_id: Annotated[str, Field(min_length=1, max_length=200)]
    repeat_index: Annotated[int, Field(ge=0, le=2)]
    reason: Literal["missing", "error", "interrupted"]
    trial_id: UUID | None


class FinalMeasurement(Contract):
    experiment_id: UUID
    seal_digest: Sha256
    initial_snapshot_id: UUID
    final_snapshot_id: UUID
    selected_final_commit: Commit
    cases: Annotated[tuple[MeasuredCase, ...], Field(max_length=4096)]
    splits: Annotated[tuple[SplitMeasurement, ...], Field(min_length=3, max_length=3)]
    errors: Annotated[tuple[MeasurementError, ...], Field(max_length=24576)]
    complete: bool


def _passes(trials: tuple[ScoredTrial, ...]) -> int | None:
    if (
        len(trials) != 3
        or {t.repeat_index for t in trials} != {0, 1, 2}
        or any(t.verdict not in ("pass", "fail") for t in trials)
    ):
        return None
    return sum(t.verdict == "pass" for t in trials)


class FinalEvaluator:
    def __init__(
        self,
        store: ExperimentStore,
        paths: ExperimentPaths | None,
        runner: FinalTrialPort | None,
    ) -> None:
        self.store, self.paths, self.runner = store, paths, runner

    async def measurement(self, id: UUID) -> FinalMeasurement:
        seal = await self.store.search_seal(id)
        if seal is None:
            raise RevisionConflict("final measurement requires immutable search seal")
        if self.paths is None:
            raise ValueError("measurement paths required")
        initial: list[EvaluationSet] = []
        final: list[EvaluationSet] = []
        for sid, output in (
            (seal.initial_snapshot_id, initial),
            (seal.selected_final_snapshot_id, final),
        ):
            for hidden in (False, True):
                output.append(
                    await build_evaluation_set(
                        self.store, self.paths, id, sid, final=hidden
                    )
                )
        cases, errors = [], []
        manifest = (await self.store.get(id)).manifest
        for case in sorted(manifest.cases, key=lambda c: (c.split, c.case_id)):
            pairs = []
            for sid, evaluations in (
                (seal.initial_snapshot_id, initial),
                (seal.selected_final_snapshot_id, final),
            ):
                trials = tuple(
                    t
                    for e in evaluations
                    for t in e.trials
                    if t.case_id == case.case_id
                )
                pairs.append(trials)
                by_repeat = {t.repeat_index: t for t in trials}
                for repeat in range(3):
                    trial = by_repeat.get(repeat)
                    if trial is None or trial.verdict in ("error", "interrupted"):
                        errors.append(
                            MeasurementError(
                                snapshot_id=sid,
                                case_id=case.case_id,
                                repeat_index=repeat,
                                reason="missing"
                                if trial is None
                                else "interrupted"
                                if trial.verdict == "interrupted"
                                else "error",
                                trial_id=trial.original_trial_id if trial else None,
                            )
                        )
            before, after = pairs
            cases.append(
                MeasuredCase(
                    case_id=case.case_id,
                    split=case.split,
                    initial_passes=_passes(before),
                    final_passes=_passes(after),
                    initial_trials=before,
                    final_trials=after,
                )
            )
        splits = []
        for split in ("development", "selection-validation", "final-test"):
            rows = [c for c in cases if c.split == split]
            complete = all(
                c.initial_passes is not None and c.final_passes is not None
                for c in rows
            )
            splits.append(
                SplitMeasurement.model_validate(
                    dict(
                        split=split,
                        expected_tasks=len(rows),
                        expected_trials_per_version=3 * len(rows),
                        initial_passed_trials=sum(c.initial_passes or 0 for c in rows)
                        if complete
                        else None,
                        final_passed_trials=sum(c.final_passes or 0 for c in rows)
                        if complete
                        else None,
                        initial_successful_tasks=sum(
                            (c.initial_passes or 0) >= 2 for c in rows
                        )
                        if complete
                        else None,
                        final_successful_tasks=sum(
                            (c.final_passes or 0) >= 2 for c in rows
                        )
                        if complete
                        else None,
                    )
                )
            )
        return FinalMeasurement(
            experiment_id=id,
            seal_digest=digest(asdict(seal)),
            initial_snapshot_id=seal.initial_snapshot_id,
            final_snapshot_id=seal.selected_final_snapshot_id,
            selected_final_commit=seal.selected_final_commit,
            cases=tuple(cases),
            splits=tuple(splits),
            errors=tuple(errors),
            complete=not errors,
        )

    async def evaluate_final(self, id: UUID) -> FinalMeasurement:
        seal = await self.store.search_seal(id)
        if seal is None:
            raise RevisionConflict("final measurement requires immutable search seal")
        if self.runner is None or self.paths is None:
            raise ValueError("final execution ports required")
        sha = digest(asdict(seal))
        manifest = (await self.store.get(id)).manifest
        stopped = False
        for sid in dict.fromkeys(
            (seal.initial_snapshot_id, seal.selected_final_snapshot_id)
        ):
            before = await build_evaluation_set(
                self.store, self.paths, id, sid, final=True
            )
            completed = {
                (t.case_id, t.repeat_index)
                for t in before.trials
                if t.verdict in ("pass", "fail")
            }
            for case in manifest.cases:
                if case.split != "final-test":
                    continue
                for repeat in range(3):
                    if (case.case_id, repeat) in completed:
                        continue
                    result = await self.runner.run_final(
                        id, sid, case.case_id, repeat, sha
                    )
                    if result.status != "complete":
                        stopped = True
                        break
                if stopped:
                    break
            if stopped:
                break
        if seal.initial_snapshot_id == seal.selected_final_snapshot_id:
            await self.store.record_event(
                id,
                "final_same_source",
                {"seal_digest": sha, "snapshot_id": str(seal.initial_snapshot_id)},
            )
        measurement = await self.measurement(id)
        await self.store.record_event(
            id,
            "final_measurement",
            {
                "seal_digest": sha,
                "measurement_digest": digest(measurement),
                "complete": measurement.complete,
            },
        )
        if stopped:
            await self.store.fail_run(id, "final_infrastructure")
        return measurement
