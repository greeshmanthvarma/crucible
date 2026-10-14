"""private historical repair evidence

Revision ID: 0014_repair_evidence
Revises: 0013_task_workspace_generations
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_repair_evidence"
down_revision: str | None = "0013_task_workspace_generations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "repair_evidence",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("step_id", sa.String(36), sa.ForeignKey("steps.id")),
        sa.Column("tool_call_id", sa.String(36), sa.ForeignKey("tool_calls.id")),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("source_key", sa.String(), nullable=False),
        sa.Column("attempt_index", sa.Integer(), nullable=False),
        sa.Column("artifact_id", sa.String(36), sa.ForeignKey("artifacts.id")),
        sa.Column("source_ids_json", sa.JSON(), nullable=False),
        sa.Column("manifest_digest", sa.String(64), nullable=False),
        sa.Column("complete", sa.Boolean(), nullable=False),
        sa.Column("unsupported_code", sa.String()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "run_id",
            "kind",
            "source_key",
            "attempt_index",
            name="uq_repair_evidence_source",
        ),
        sa.CheckConstraint("attempt_index >= 0", name="ck_repair_evidence_attempt"),
        sa.CheckConstraint(
            "NOT complete OR (artifact_id IS NOT NULL AND unsupported_code IS NULL)",
            name="ck_repair_evidence_complete",
        ),
    )


def downgrade() -> None:
    op.drop_table("repair_evidence")
