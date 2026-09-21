import sqlalchemy as sa
from alembic import op

revision = "0002_failed_task_without_revision"
down_revision = "0001_run_spine"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table(
        "tasks",
        table_args=(
            sa.CheckConstraint(
                "status IN ('provisioning','active','provisioning_failed')",
                name="ck_tasks_status",
            ),
            sa.CheckConstraint("next_task_sequence > 0", name="ck_tasks_task_sequence"),
            sa.CheckConstraint(
                "next_conversation_sequence > 0",
                name="ck_tasks_conversation_sequence",
            ),
            sa.CheckConstraint(
                "base_revision IS NOT NULL OR "
                "(status = 'provisioning_failed' "
                "AND failure_code IS NOT NULL "
                "AND failure_code = 'revision_not_found')",
                name="ck_tasks_failed_revision",
            ),
        ),
    ) as batch_op:
        batch_op.alter_column(
            "base_revision",
            existing_type=sa.String(),
            nullable=True,
        )


def downgrade() -> None:
    null_revisions = op.get_bind().scalar(
        sa.text("SELECT count(*) FROM tasks WHERE base_revision IS NULL")
    )
    if null_revisions:
        raise RuntimeError(
            "Cannot downgrade while revision_not_found Task evidence exists"
        )
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.drop_constraint("ck_tasks_failed_revision", type_="check")
        batch_op.alter_column(
            "base_revision",
            existing_type=sa.String(),
            nullable=False,
        )
