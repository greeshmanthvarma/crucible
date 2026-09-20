import sqlalchemy as sa
from alembic import op

revision = "0001_run_spine"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "repositories",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("root_path", sa.String(), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "repository_id",
            sa.String(36),
            sa.ForeignKey("repositories.id"),
            nullable=False,
        ),
        sa.Column("source_ref", sa.String(), nullable=False),
        sa.Column("base_revision", sa.String(), nullable=False),
        sa.Column("workspace_path", sa.String(), nullable=False, unique=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("failure_code", sa.String()),
        sa.Column("failure_detail", sa.String()),
        sa.Column(
            "next_task_sequence", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column(
            "next_conversation_sequence",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("status IN ('provisioning','active','provisioning_failed')"),
        sa.CheckConstraint("next_task_sequence > 0"),
        sa.CheckConstraint("next_conversation_sequence > 0"),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("triggering_message_id", sa.String(36), sa.ForeignKey("messages.id")),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column(
            "next_run_sequence", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column("execution_id", sa.String(36)),
        sa.Column("lease_expires_at", sa.DateTime()),
        sa.Column("heartbeat_at", sa.DateTime()),
        sa.Column("outcome_code", sa.String()),
        sa.Column("outcome_detail", sa.String()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime()),
        sa.Column("completed_at", sa.DateTime()),
        sa.CheckConstraint(
            "status IN ('queued','running','completed','failed','interrupted')"
        ),
        sa.CheckConstraint("next_run_sequence > 0"),
    )
    op.create_table(
        "messages",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id")),
        sa.Column("conversation_sequence", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("task_id", "conversation_sequence"),
        sa.CheckConstraint("role IN ('user','assistant')"),
        sa.CheckConstraint("status IN ('completed','interrupted')"),
    )
    op.create_table(
        "message_parts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "message_id",
            sa.String(36),
            sa.ForeignKey("messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("part_sequence", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("text_content", sa.String(), nullable=False),
        sa.UniqueConstraint("message_id", "part_sequence"),
        sa.CheckConstraint("part_sequence > 0"),
        sa.CheckConstraint("kind = 'text'"),
    )
    op.create_table(
        "task_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id")),
        sa.Column("task_sequence", sa.Integer(), nullable=False),
        sa.Column("run_sequence", sa.Integer()),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("task_id", "task_sequence"),
        sa.UniqueConstraint("run_id", "run_sequence"),
        sa.CheckConstraint("task_sequence > 0"),
        sa.CheckConstraint("run_sequence > 0"),
        sa.CheckConstraint(
            "(run_id IS NULL AND run_sequence IS NULL) OR "
            "(run_id IS NOT NULL AND run_sequence IS NOT NULL)"
        ),
        sa.CheckConstraint("schema_version > 0"),
    )
    op.create_table(
        "idempotency_records",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("scope", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("request_hash", sa.String(), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=False),
        sa.Column("response_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope", "key"),
    )


def downgrade() -> None:
    for table_name in (
        "task_events",
        "idempotency_records",
        "message_parts",
        "messages",
        "runs",
        "tasks",
        "repositories",
    ):
        op.drop_table(table_name)
