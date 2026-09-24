"""authorized commands, approvals, artifacts, and external resources

Revision ID: 0004_authorized_commands
Revises: 0003_tool_loop
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_authorized_commands"
down_revision: str | None = "0003_tool_loop"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "approvals",
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
        sa.Column("spec_json", sa.JSON(), nullable=False),
        sa.Column("spec_digest", sa.String(64), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("decision_reason", sa.String()),
        sa.Column("decided_by", sa.String()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("decided_at", sa.DateTime()),
        sa.CheckConstraint(
            "status IN ('pending','approved','denied','cancelled','invalidated')"
        ),
    )
    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("media_type", sa.String(), nullable=False),
        sa.Column("byte_length", sa.Integer(), nullable=False),
        sa.Column("storage_identity", sa.String(), nullable=False),
        sa.Column("sensitivity", sa.String(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("task_id", "content_hash", "media_type", "sensitivity"),
        sa.CheckConstraint("byte_length >= 0"),
    )
    op.create_table(
        "external_resources",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id")),
        sa.Column("tool_call_id", sa.String(36), sa.ForeignKey("tool_calls.id")),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("external_identity", sa.String(), nullable=False, unique=True),
        sa.Column("mount_target", sa.String()),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("labels_json", sa.JSON(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("kind IN ('volume','container')"),
        sa.CheckConstraint(
            "status IN ('present','active','removed','missing','orphaned','error')"
        ),
    )
    with op.batch_alter_table("tool_results") as batch:
        batch.add_column(sa.Column("artifact_id", sa.String(36)))
        batch.create_foreign_key(
            "fk_tool_results_artifact_id", "artifacts", ["artifact_id"], ["id"]
        )
    with op.batch_alter_table("runs") as batch:
        batch.add_column(sa.Column("cancel_requested_at", sa.DateTime()))
        batch.add_column(sa.Column("cancel_code", sa.String()))


def downgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.drop_column("cancel_code")
        batch.drop_column("cancel_requested_at")
    with op.batch_alter_table("tool_results") as batch:
        batch.drop_constraint("fk_tool_results_artifact_id", type_="foreignkey")
        batch.drop_column("artifact_id")
    op.drop_table("external_resources")
    op.drop_table("artifacts")
    op.drop_table("approvals")
