"""Package export/verification at the positive immutable input seam."""

import pytest

from crucible.improvement.trajectory_contracts import TrialSelection


def test_selection_ids_cannot_repeat():
    from uuid import UUID

    with pytest.raises(ValueError):
        TrialSelection(mode="explicit-three-repeat", trial_ids=(UUID(int=1),) * 3)
