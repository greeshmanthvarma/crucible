"""Internal role Tasks share one repair worktree without public task binding."""

from alembic import op
import sqlalchemy as sa

revision = "0017_internal_repair_tasks"
down_revision = "0016_repair_runs"
branch_labels = None
depends_on = None


def _checks():
    return (
        sa.CheckConstraint("next_task_sequence > 0"),
        sa.CheckConstraint("next_conversation_sequence > 0"),
        sa.CheckConstraint(
            "base_revision IS NOT NULL OR (status = 'provisioning_failed' AND failure_code IS NOT NULL AND failure_code = 'revision_not_found')"
        ),
    )


def upgrade() -> None:
    with op.batch_alter_table(
        "tasks",
        naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"},
        table_args=_checks(),
    ) as batch:
        batch.add_column(sa.Column("internal_repair_id", sa.String(36)))
        batch.create_foreign_key(
            "fk_task_internal_repair", "repair_runs", ["internal_repair_id"], ["id"]
        )
        batch.drop_constraint("uq_tasks_workspace_path", type_="unique")
    op.create_index(
        "uq_public_task_workspace",
        "tasks",
        ["workspace_path"],
        unique=True,
        sqlite_where=sa.text("internal_repair_id IS NULL"),
    )


def downgrade() -> None:
    # Internal conversations are durable evidence. A downgrade cannot silently
    # discard them or invent unique worktree bindings.
    if op.get_bind().scalar(
        sa.text("SELECT count(*) FROM tasks WHERE internal_repair_id IS NOT NULL")
    ):
        raise RuntimeError("Cannot downgrade while internal repair Tasks exist")
    op.drop_index("uq_public_task_workspace", table_name="tasks")
    with op.batch_alter_table("tasks", table_args=_checks()) as batch:
        batch.drop_constraint("fk_task_internal_repair", type_="foreignkey")
        batch.drop_column("internal_repair_id")
        batch.create_unique_constraint("uq_tasks_workspace_path", ["workspace_path"])
