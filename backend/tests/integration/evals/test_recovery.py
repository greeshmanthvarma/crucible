from datetime import UTC, datetime

from crucible.domain.evals import EvalCase, EvalSuite, EvalTrial, ResultVerdict
from crucible.domain.ids import new_id
from crucible.evals.cli import reconcile_incomplete_trials
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork


async def test_restart_marks_uncertain_trial_interrupted_without_pass(
    database: Database,
) -> None:
    now = datetime.now(UTC)
    suite_id, case_id, invocation_id, trial_id = (new_id() for _ in range(4))
    async with SqlAlchemyUnitOfWork(database) as uow:
        await uow.evals.add_suite(
            EvalSuite(suite_id, "development", "smoke", "suite", {}, now)
        )
        await uow.evals.add_case(EvalCase(case_id, suite_id, "tiny", "case", {}, now))
        await uow.evals.add_trial(
            EvalTrial.queued(
                trial_id,
                suite_id,
                case_id,
                invocation_id,
                1,
                "development",
                "case",
                "config",
                now,
            ).prepare(now)
        )
        await uow.commit()
    await reconcile_incomplete_trials(lambda: SqlAlchemyUnitOfWork(database))
    async with SqlAlchemyUnitOfWork(database) as uow:
        trial = await uow.evals.get_trial(trial_id)
        result = await uow.evals.get_result(trial_id)
    assert trial is not None and trial.status == "interrupted"
    assert result is not None and result.verdict is ResultVerdict.ERROR
