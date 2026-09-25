"""durable evaluation lineage and usage projections

Revision ID: 0011_eval_runner
Revises: 0010_browser_sessions
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011_eval_runner"
down_revision: str | None = "0010_browser_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "eval_suites",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("partition", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("definition_digest", sa.String(64), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("partition", "name", "definition_digest"),
    )
    op.create_table(
        "eval_cases",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "suite_id", sa.String(36), sa.ForeignKey("eval_suites.id"), nullable=False
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("definition_digest", sa.String(64), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("suite_id", "name", "definition_digest"),
    )
    op.create_table(
        "eval_trials",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "suite_id", sa.String(36), sa.ForeignKey("eval_suites.id"), nullable=False
        ),
        sa.Column(
            "case_id", sa.String(36), sa.ForeignKey("eval_cases.id"), nullable=False
        ),
        sa.Column("invocation_id", sa.String(36), nullable=False),
        sa.Column("repeat_index", sa.Integer(), nullable=False),
        sa.Column("partition", sa.String(), nullable=False),
        sa.Column("case_digest", sa.String(64), nullable=False),
        sa.Column("configuration_digest", sa.String(64), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("fixture_commit", sa.String(40)),
        sa.Column("repository_id", sa.String(36), sa.ForeignKey("repositories.id")),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id")),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id")),
        sa.Column("failure_code", sa.String()),
        sa.UniqueConstraint("suite_id", "case_id", "repeat_index", "invocation_id"),
        sa.CheckConstraint("repeat_index > 0"),
        sa.CheckConstraint(
            "status IN ('queued','preparing','running','evaluating',"
            "'completed','failed','interrupted')"
        ),
    )
    op.create_table(
        "eval_results",
        sa.Column(
            "trial_id", sa.String(36), sa.ForeignKey("eval_trials.id"), primary_key=True
        ),
        sa.Column("verdict", sa.String(), nullable=False),
        sa.Column("evaluator_results_json", sa.JSON(), nullable=False),
        sa.Column("report_artifact_id", sa.String(36), sa.ForeignKey("artifacts.id")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("verdict IN ('passed','failed','error')"),
    )
    op.create_table(
        "step_usage",
        sa.Column(
            "step_id", sa.String(36), sa.ForeignKey("steps.id"), primary_key=True
        ),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("model_id", sa.String(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("source IN ('reported','estimated')"),
        sa.CheckConstraint("input_tokens >= 0 AND output_tokens >= 0"),
    )
    op.create_table(
        "run_summaries",
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("projection_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("schema_version > 0"),
    )


def downgrade() -> None:
    for name in (
        "run_summaries",
        "step_usage",
        "eval_results",
        "eval_trials",
        "eval_cases",
        "eval_suites",
    ):
        op.drop_table(name)
