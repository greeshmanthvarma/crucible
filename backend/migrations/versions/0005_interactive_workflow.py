"""interactive workflow records

Revision ID: 0005_interactive_workflow
Revises: 0004_authorized_commands
"""

from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "0005_interactive_workflow"
down_revision: str | None = "0004_authorized_commands"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("repositories") as batch:
        batch.add_column(
            sa.Column("settings_json", sa.JSON(), nullable=False, server_default="{}")
        )
    with op.batch_alter_table("runs") as batch:
        batch.add_column(
            sa.Column(
                "settings_snapshot_json", sa.JSON(), nullable=False, server_default="{}"
            )
        )
    op.create_table(
        "compactions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("source_start_sequence", sa.Integer(), nullable=False),
        sa.Column("source_end_sequence", sa.Integer(), nullable=False),
        sa.Column("retained_tail_start_sequence", sa.Integer(), nullable=False),
        sa.Column(
            "summary_artifact_id",
            sa.String(36),
            sa.ForeignKey("artifacts.id"),
            nullable=False,
        ),
        sa.Column("rendered_summary", sa.String(), nullable=False),
        sa.Column(
            "previous_compaction_id", sa.String(36), sa.ForeignKey("compactions.id")
        ),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("parameters_json", sa.JSON(), nullable=False),
        sa.Column("prompt_version", sa.String(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("resulting_context_estimate", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("task_id", "source_end_sequence"),
        sa.CheckConstraint(
            "source_start_sequence > 0 AND source_end_sequence >= source_start_sequence"
        ),
        sa.CheckConstraint("retained_tail_start_sequence > source_end_sequence"),
    )
    op.create_table(
        "validation_attempts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime()),
        sa.UniqueConstraint("run_id", "attempt_number"),
    )
    op.create_table(
        "validation_command_results",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "validation_attempt_id",
            sa.String(36),
            sa.ForeignKey("validation_attempts.id"),
            nullable=False,
        ),
        sa.Column("command_sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("approval_id", sa.String(36), sa.ForeignKey("approvals.id")),
        sa.Column("tool_call_id", sa.String(36), sa.ForeignKey("tool_calls.id")),
        sa.Column("artifact_id", sa.String(36), sa.ForeignKey("artifacts.id")),
        sa.Column("exit_code", sa.Integer()),
        sa.Column("summary", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime()),
        sa.UniqueConstraint("validation_attempt_id", "command_sequence"),
    )
    op.create_table(
        "result_revisions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("commit_sha", sa.String(40), nullable=False, unique=True),
        sa.Column("parent_revision", sa.String(40), nullable=False),
        sa.Column(
            "previous_result_revision_id",
            sa.String(36),
            sa.ForeignKey("result_revisions.id"),
        ),
        sa.Column(
            "diff_artifact_id",
            sa.String(36),
            sa.ForeignKey("artifacts.id"),
            nullable=False,
        ),
        sa.Column("validation_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("summary", sa.String(), nullable=False),
        sa.Column("created_by", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "integrations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "result_revision_id",
            sa.String(36),
            sa.ForeignKey("result_revisions.id"),
            nullable=False,
        ),
        sa.Column(
            "repository_id",
            sa.String(36),
            sa.ForeignKey("repositories.id"),
            nullable=False,
        ),
        sa.Column("target_ref", sa.String(), nullable=False),
        sa.Column("expected_target_revision", sa.String(40), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("observed_before_revision", sa.String(40)),
        sa.Column("observed_after_revision", sa.String(40)),
        sa.Column("failure_code", sa.String()),
        sa.Column("failure_detail", sa.String()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime()),
        sa.UniqueConstraint("result_revision_id", "idempotency_key"),
    )


def downgrade() -> None:
    op.drop_table("integrations")
    op.drop_table("result_revisions")
    op.drop_table("validation_command_results")
    op.drop_table("validation_attempts")
    op.drop_table("compactions")
    with op.batch_alter_table("runs") as batch:
        batch.drop_column("settings_snapshot_json")
    with op.batch_alter_table("repositories") as batch:
        batch.drop_column("settings_json")
