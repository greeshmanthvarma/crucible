import sqlalchemy as sa
from alembic import op

revision = "0003_tool_loop"
down_revision = "0002_failed_task_without_revision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "steps",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("step_sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime()),
        sa.Column("completed_at", sa.DateTime()),
        sa.UniqueConstraint("run_id", "step_sequence"),
        sa.CheckConstraint("step_sequence > 0"),
    )
    with op.batch_alter_table(
        "messages",
        table_args=(
            sa.CheckConstraint(
                "role IN ('user','assistant','system','tool')",
                name="ck_messages_role",
            ),
        ),
    ) as batch:
        batch.add_column(sa.Column("step_id", sa.String(36)))
        batch.create_foreign_key("fk_messages_step", "steps", ["step_id"], ["id"])
    with op.batch_alter_table(
        "message_parts",
        table_args=(
            sa.CheckConstraint(
                "kind IN ('text','reasoning','tool_call','tool_result')",
                name="ck_message_parts_kind",
            ),
        ),
    ) as batch:
        batch.alter_column("text_content", existing_type=sa.String(), nullable=True)
        batch.add_column(sa.Column("reasoning_content", sa.String()))
        batch.add_column(sa.Column("tool_call_id", sa.String(36)))
        batch.add_column(sa.Column("tool_result_id", sa.String(36)))
    op.create_table(
        "context_manifests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column(
            "step_id",
            sa.String(36),
            sa.ForeignKey("steps.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("parameters_json", sa.JSON(), nullable=False),
        sa.Column("input_limit", sa.Integer(), nullable=False),
        sa.Column("output_reserve", sa.Integer(), nullable=False),
        sa.Column("threshold", sa.String(), nullable=False),
        sa.Column("estimated_tokens", sa.Integer(), nullable=False),
        sa.Column("message_ids_json", sa.JSON(), nullable=False),
        sa.Column("part_ids_json", sa.JSON(), nullable=False),
        sa.Column("instruction_digests_json", sa.JSON(), nullable=False),
        sa.Column("tool_schema_digest", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "tool_calls",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("step_id", sa.String(36), sa.ForeignKey("steps.id"), nullable=False),
        sa.Column(
            "assistant_message_id",
            sa.String(36),
            sa.ForeignKey("messages.id"),
            nullable=False,
        ),
        sa.Column("call_sequence", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("arguments_json", sa.JSON(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("provider_correlation_id", sa.String()),
        sa.Column("execution_mode", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("step_id", "call_sequence"),
    )
    op.create_table(
        "tool_results",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("step_id", sa.String(36), sa.ForeignKey("steps.id"), nullable=False),
        sa.Column(
            "tool_call_id",
            sa.String(36),
            sa.ForeignKey("tool_calls.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("display_text", sa.String(), nullable=False),
        sa.Column("error_code", sa.String()),
        sa.Column("completion_sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("step_id", "completion_sequence"),
    )


def downgrade() -> None:
    op.drop_table("tool_results")
    op.drop_table("tool_calls")
    op.drop_table("context_manifests")
    with op.batch_alter_table("message_parts") as batch:
        batch.drop_column("tool_result_id")
        batch.drop_column("tool_call_id")
        batch.drop_column("reasoning_content")
        batch.alter_column("text_content", existing_type=sa.String(), nullable=False)
    with op.batch_alter_table("messages") as batch:
        batch.drop_constraint("fk_messages_step", type_="foreignkey")
        batch.drop_column("step_id")
    op.drop_table("steps")
