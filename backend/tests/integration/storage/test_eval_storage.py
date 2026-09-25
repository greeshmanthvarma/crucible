from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from crucible.domain.evals import (
    EvalCase,
    EvalResult,
    EvalSuite,
    EvalTrial,
    ResultVerdict,
)
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork


async def test_eval_trial_identity_and_result_are_unique(database: Database) -> None:
    suite, case, invocation, trial = [str(uuid4()) for _ in range(4)]
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO eval_suites VALUES "
                "(:id,'development','smoke','digest','{}',CURRENT_TIMESTAMP)"
            ),
            {"id": suite},
        )
        await connection.execute(
            text(
                "INSERT INTO eval_cases VALUES "
                "(:id,:suite,'tiny','digest','{}',CURRENT_TIMESTAMP)"
            ),
            {"id": case, "suite": suite},
        )
        await connection.execute(
            text(
                "INSERT INTO eval_trials "
                "(id,suite_id,case_id,invocation_id,repeat_index,partition,"
                "case_digest,configuration_digest,status,created_at,updated_at) "
                "VALUES (:id,:suite,:case,:invocation,1,'development','case',"
                "'config','queued',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
            ),
            {"id": trial, "suite": suite, "case": case, "invocation": invocation},
        )
    with pytest.raises(IntegrityError):
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO eval_trials "
                    "(id,suite_id,case_id,invocation_id,repeat_index,partition,"
                    "case_digest,configuration_digest,status,created_at,updated_at) "
                    "VALUES (:id,:suite,:case,:invocation,1,'development','case',"
                    "'config','queued',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
                ),
                {
                    "id": str(uuid4()),
                    "suite": suite,
                    "case": case,
                    "invocation": invocation,
                },
            )
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO eval_results "
                "(trial_id,verdict,evaluator_results_json,created_at) "
                "VALUES (:trial,'failed','[]',CURRENT_TIMESTAMP)"
            ),
            {"trial": trial},
        )
    with pytest.raises(IntegrityError):
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO eval_results "
                    "(trial_id,verdict,evaluator_results_json,created_at) "
                    "VALUES (:trial,'passed','[]',CURRENT_TIMESTAMP)"
                ),
                {"trial": trial},
            )


async def test_eval_schema_is_migrated(database: Database) -> None:
    async with database.engine.connect() as connection:
        names = (
            (
                await connection.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
            )
            .scalars()
            .all()
        )
    assert {
        "eval_suites",
        "eval_cases",
        "eval_trials",
        "eval_results",
        "step_usage",
        "run_summaries",
    } <= set(names)


async def test_store_preserves_trial_and_result_across_reopen(
    database: Database,
) -> None:
    now = datetime.now(UTC)
    suite_id, case_id, invocation_id, trial_id = (uuid4() for _ in range(4))
    suite = EvalSuite(
        suite_id, "development", "smoke", "suite-digest", {"version": 1}, now
    )
    case = EvalCase(case_id, suite_id, "tiny", "case-digest", {"prompt": "tiny"}, now)
    trial = EvalTrial.queued(
        trial_id,
        suite_id,
        case_id,
        invocation_id,
        1,
        "development",
        "case-digest",
        "config-digest",
        now,
    )
    async with SqlAlchemyUnitOfWork(database) as uow:
        await uow.evals.add_suite(suite)
        await uow.evals.add_case(case)
        await uow.evals.add_trial(trial)
        await uow.evals.update_trial(trial.prepare(now).fail("fixture_error", now))
        await uow.evals.add_result(
            EvalResult(trial_id, ResultVerdict.ERROR, (), None, now)
        )
        await uow.commit()
    async with SqlAlchemyUnitOfWork(database) as uow:
        restored = await uow.evals.get_trial(trial_id)
        result = await uow.evals.get_result(trial_id)
        assert restored is not None and restored.failure_code == "fixture_error"
        assert result is not None and result.verdict is ResultVerdict.ERROR
        assert await uow.evals.list_trials(invocation_id) == (restored,)
        with pytest.raises(ValueError, match="terminal"):
            await uow.evals.update_trial(trial)
