from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import IntegrityError

from crucible.domain.compaction import Compaction
from crucible.domain.ids import new_id
from crucible.domain.results import Integration, ResultRevision
from crucible.domain.validation import ValidationAttempt, ValidationStatus
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork
from tests.integration.storage.test_tool_loop_storage import seed_exchange

NOW = datetime(2026, 9, 23, tzinfo=UTC)


async def test_interactive_records_round_trip(database: Database) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, _step, _message = await seed_exchange(uow, "interactive")
        artifact_id = new_id()
        from crucible.domain.artifacts import Artifact

        await uow.artifacts.add(
            Artifact(
                artifact_id,
                task.id,
                "a" * 64,
                "text/plain",
                1,
                "aa/a",
                "private",
                {},
                NOW,
            )
        )
        compaction = Compaction(
            new_id(),
            task.id,
            1,
            2,
            3,
            artifact_id,
            "summary",
            None,
            "model",
            {},
            "v1",
            10,
            5,
            20,
            NOW,
        )
        validation = ValidationAttempt(
            new_id(), run.id, 1, ValidationStatus.PASSED, NOW, NOW
        )
        result = ResultRevision(
            new_id(),
            task.id,
            "b" * 40,
            task.base_revision or "",
            None,
            artifact_id,
            {"status": "passed"},
            "done",
            "user",
            NOW,
        )
        integration = Integration.pending(
            new_id(),
            result.id,
            task.repository_id,
            "main",
            task.base_revision or "",
            "key",
            NOW,
        )
        await uow.compactions.add(compaction)
        await uow.validation_attempts.add(validation)
        await uow.result_revisions.add(result)
        await uow.integrations.add(integration)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(database) as uow:
        assert await uow.compactions.get(compaction.id) == compaction
        assert await uow.validation_attempts.get(validation.id) == validation
        assert await uow.result_revisions.get(result.id) == result
        assert await uow.integrations.get(integration.id) == integration


async def test_validation_attempt_number_and_integration_key_are_unique(
    database: Database,
) -> None:
    async with SqlAlchemyUnitOfWork(database) as uow:
        task, run, _step, _message = await seed_exchange(uow, "interactive-unique")
        first = ValidationAttempt(
            new_id(), run.id, 1, ValidationStatus.PENDING, NOW, None
        )
        await uow.validation_attempts.add(first)
        with pytest.raises(IntegrityError):
            await uow.validation_attempts.add(
                ValidationAttempt(
                    new_id(), run.id, 1, ValidationStatus.PENDING, NOW, None
                )
            )
            await uow.commit()
