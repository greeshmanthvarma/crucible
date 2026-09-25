"""retain prepared fixture content and root identity

Revision ID: 0012_eval_fixture_provenance
Revises: 0011_eval_runner
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_eval_fixture_provenance"
down_revision: str | None = "0011_eval_runner"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("eval_trials", sa.Column("configuration_snapshot_json", sa.JSON()))
    op.add_column("eval_trials", sa.Column("fixture_content_digest", sa.String(64)))
    op.add_column("eval_trials", sa.Column("fixture_root", sa.String()))


def downgrade() -> None:
    op.drop_column("eval_trials", "fixture_root")
    op.drop_column("eval_trials", "fixture_content_digest")
    op.drop_column("eval_trials", "configuration_snapshot_json")
