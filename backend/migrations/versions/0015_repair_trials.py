"""Durable protected repair trials and accepted private incidents."""

import sqlalchemy as sa
from alembic import op

revision = "0015_repair_trials"
down_revision = "0014_repair_evidence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "repair_trial_jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("repair_id", sa.String(36)),
        sa.Column("invocation_id", sa.String(36), nullable=False),
        sa.Column("candidate_generation", sa.Integer, nullable=False),
        sa.Column("case_id", sa.String, nullable=False),
        sa.Column("case_digest", sa.String(64), nullable=False),
        sa.Column("source_digest", sa.String(64), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("side", sa.String, nullable=False),
        sa.Column("repeat_index", sa.Integer, nullable=False),
        sa.Column("status", sa.String, nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime()),
        sa.Column(
            "observation_artifact_id", sa.String(36), sa.ForeignKey("artifacts.id")
        ),
        sa.UniqueConstraint(
            "invocation_id",
            "candidate_generation",
            "fingerprint",
            "case_digest",
            "source_digest",
            "side",
            "repeat_index",
            name="uq_repair_trial_scope",
        ),
        sa.CheckConstraint("candidate_generation >= 0 AND repeat_index >= 0"),
        sa.CheckConstraint("side IN ('baseline','candidate','regression')"),
        sa.CheckConstraint("status IN ('running','completed','interrupted')"),
        sa.CheckConstraint("status != 'completed' OR completed_at IS NOT NULL"),
    )
    op.create_table(
        "repair_trial_results",
        sa.Column(
            "job_id",
            sa.String(36),
            sa.ForeignKey("repair_trial_jobs.id"),
            primary_key=True,
        ),
        sa.Column("verdict", sa.String, nullable=False),
        sa.Column("assertions_json", sa.JSON, nullable=False),
        sa.Column("usage_json", sa.JSON, nullable=False),
        sa.Column("termination", sa.String, nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("verdict IN ('pass','fail','error','unsupported')"),
    )
    op.create_table(
        "repair_incident_cases",
        sa.Column("case_digest", sa.String(64), primary_key=True),
        sa.Column(
            "definition_artifact_id",
            sa.String(36),
            sa.ForeignKey("artifacts.id"),
            nullable=False,
        ),
        sa.Column(
            "fixture_manifest_artifact_id",
            sa.String(36),
            sa.ForeignKey("artifacts.id"),
            nullable=False,
        ),
        sa.Column("accepted_commit", sa.String, nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("repair_incident_cases")
    op.drop_table("repair_trial_results")
    op.drop_table("repair_trial_jobs")
