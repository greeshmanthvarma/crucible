"""retain workspace generations when an integrated chat continues

Revision ID: 0013_task_workspace_generations
Revises: 0012_eval_fixture_provenance
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013_task_workspace_generations"
down_revision: str | None = "0012_eval_fixture_provenance"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.add_column(
            sa.Column(
                "workspace_generation", sa.Integer(), nullable=False, server_default="0"
            )
        )
        batch.add_column(sa.Column("workspace_base_revision", sa.String()))
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','accepted','integrated',"
            "'continuing','provisioning_failed')",
        )


def downgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint(
            "ck_tasks_status",
            "status IN ('provisioning','active','accepted','integrated',"
            "'provisioning_failed')",
        )
        batch.drop_column("workspace_base_revision")
        batch.drop_column("workspace_generation")
