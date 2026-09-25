"""accepted Task lifecycle state

Revision ID: 0008_task_acceptance
Revises: 0007_validation_proposals
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0008_task_acceptance"
down_revision: str | None = "0007_validation_proposals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','accepted','provisioning_failed')",
        )


def downgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','provisioning_failed')",
        )
