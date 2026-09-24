from datetime import UTC, datetime

import pytest

from crucible.domain.compaction import Compaction
from crucible.domain.ids import new_id
from crucible.domain.results import Integration, IntegrationStatus, ResultRevision
from crucible.domain.validation import ValidationAttempt, ValidationStatus

NOW = datetime(2026, 9, 23, tzinfo=UTC)


def test_compaction_requires_an_ordered_source_range_and_retained_tail() -> None:
    with pytest.raises(ValueError, match="source range"):
        Compaction(
            new_id(),
            new_id(),
            4,
            3,
            5,
            new_id(),
            "summary",
            None,
            "model",
            {},
            "v1",
            1,
            1,
            10,
            NOW,
        )
    with pytest.raises(ValueError, match="retained tail"):
        Compaction(
            new_id(),
            new_id(),
            1,
            4,
            4,
            new_id(),
            "summary",
            None,
            "model",
            {},
            "v1",
            1,
            1,
            10,
            NOW,
        )


def test_validation_and_result_records_reject_invalid_immutable_identifiers() -> None:
    with pytest.raises(ValueError, match="attempt number"):
        ValidationAttempt(new_id(), new_id(), 0, ValidationStatus.PENDING, NOW, None)
    with pytest.raises(ValueError, match="commit SHA"):
        ResultRevision(
            new_id(),
            new_id(),
            "mutable",
            "a" * 40,
            None,
            new_id(),
            {},
            "done",
            "user",
            NOW,
        )


def test_integration_state_machine_records_success_once() -> None:
    integration = Integration.pending(
        new_id(), new_id(), new_id(), "main", "a" * 40, "key", NOW
    )
    completed = integration.succeed("a" * 40, "b" * 40, NOW)
    assert completed.status is IntegrationStatus.COMPLETED
    assert completed.succeed("a" * 40, "b" * 40, NOW) == completed
