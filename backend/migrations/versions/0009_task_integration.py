"""integrated Task lifecycle state

Revision ID: 0009_task_integration
Revises: 0008_task_acceptance
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0009_task_integration"
down_revision: str | None = "0008_task_acceptance"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','accepted','integrated','provisioning_failed')",
        )


def downgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','accepted','provisioning_failed')",
        )
