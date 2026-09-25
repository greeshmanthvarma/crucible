"""completion proposals for authoritative validation

Revision ID: 0007_validation_proposals
Revises: 0006_compaction_manifests
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_validation_proposals"
down_revision: str | None = "0006_compaction_manifests"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.drop_constraint("ck_runs_status", type_="check")
        batch.create_check_constraint(
            "ck_runs_status",
            "status IN ('queued','running','validating','completed','failed','interrupted','cancelled')",
        )
    op.create_table(
        "completion_proposals",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False, unique=True),
        sa.Column("assistant_message_id", sa.String(36), sa.ForeignKey("messages.id"), nullable=False),
        sa.Column("summary", sa.String(), nullable=False),
        sa.Column("claimed_files_json", sa.JSON(), nullable=False),
        sa.Column("notes", sa.String()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("completion_proposals")
    with op.batch_alter_table("runs") as batch:
        batch.drop_constraint("ck_runs_status", type_="check")
        batch.create_check_constraint(
            "ck_runs_status",
            "status IN ('queued','running','completed','failed','interrupted','cancelled')",
        )
