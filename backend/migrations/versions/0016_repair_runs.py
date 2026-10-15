"""Durable repair admission, ownership and orchestration records."""

from alembic import op
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)

revision = "0016_repair_runs"
down_revision = "0015_repair_trials"
branch_labels = None
depends_on = None


def upgrade() -> None:
    repair_runs = op.create_table(
        "repair_runs",
        Column("id", String(36), primary_key=True),
        Column("source_task_id", String(36), ForeignKey("tasks.id"), nullable=False),
        Column("status", String, nullable=False),
        Column("stage", String, nullable=False),
        Column("initial_identity_json", JSON, nullable=False),
        Column("branch", String),
        Column("worktree_path", String),
        Column("last_accepted_commit", String, nullable=False),
        Column("current_fix_id", String(36)),
        Column("created_at", DateTime(), nullable=False),
        Column("updated_at", DateTime(), nullable=False),
        Column("outcome_code", String),
        Column("outcome_detail", String),
        Column("predecessor_repair_id", String(36), ForeignKey("repair_runs.id")),
        CheckConstraint(
            "status IN ('queued','running','stopped','interrupted','retryable_error','completed','exhausted','closed','invalidated')"
        ),
        CheckConstraint(
            "stage IN ('intake','diagnosis','eval_review','baseline_reproduction','proposal','candidate_validation','commit_acceptance','publication')"
        ),
    )
    repair_ownership = op.create_table(
        "repair_ownership",
        Column("singleton_id", Integer, primary_key=True),
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), unique=True),
        Column("owner_execution_id", String),
        Column("heartbeat_at", DateTime()),
        Column("lease_expires_at", DateTime()),
        CheckConstraint("singleton_id = 1"),
    )
    repair_fixes = op.create_table(
        "repair_fixes",
        Column("id", String(36), primary_key=True),
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), nullable=False),
        Column("ordinal", Integer, nullable=False),
        Column("diagnosis_case_digest", String, nullable=False),
        Column("approved_case_digest", String),
        Column("status", String, nullable=False),
        Column("accepted_commit", String),
        Column("attempts_used", Integer, nullable=False),
        UniqueConstraint("repair_id", "ordinal"),
        CheckConstraint("ordinal >= 0 AND attempts_used >= 0"),
    )
    repair_attempts = op.create_table(
        "repair_attempts",
        Column("id", String(36), primary_key=True),
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), nullable=False),
        Column("fix_id", String(36), ForeignKey("repair_fixes.id")),
        Column("role", String, nullable=False),
        Column("attempt_index", Integer, nullable=False),
        Column("harness_run_id", String(36), ForeignKey("runs.id")),
        Column("status", String, nullable=False),
        Column("charged", Boolean, nullable=False),
        Column("started_at", DateTime(), nullable=False),
        Column("completed_at", DateTime()),
        Column("output_artifact_id", String(36), ForeignKey("artifacts.id")),
        CheckConstraint("attempt_index >= 0"),
    )
    repair_checkpoints = op.create_table(
        "repair_checkpoints",
        Column("id", String(36), primary_key=True),
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), nullable=False),
        Column("stage", String, nullable=False),
        Column("fix_id", String(36), ForeignKey("repair_fixes.id")),
        Column("trial_job_id", String(36), ForeignKey("repair_trial_jobs.id")),
        Column("input_fingerprint", String, nullable=False),
        Column("accepted_commit", String, nullable=False),
        Column("attempts_json", JSON, nullable=False),
        Column("result_refs_json", JSON, nullable=False),
        Column("created_at", DateTime(), nullable=False),
    )
    repair_acceptances = op.create_table(
        "repair_acceptances",
        Column("id", String(36), primary_key=True),
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), nullable=False),
        Column("fix_id", String(36), ForeignKey("repair_fixes.id"), nullable=False),
        Column("expected_parent", String, nullable=False),
        Column("candidate_tree_digest", String, nullable=False),
        Column("evaluated_source_digest", String, nullable=False),
        Column("result_refs_json", JSON, nullable=False),
        Column("status", String, nullable=False),
        Column("commit_sha", String),
    )
    repair_publications = op.create_table(
        "repair_publications",
        Column("repair_id", String(36), ForeignKey("repair_runs.id"), primary_key=True),
        Column("head_branch", String, nullable=False),
        Column("base_branch", String, nullable=False),
        Column("head_sha", String, nullable=False),
        Column("status", String, nullable=False),
        Column("pr_number", Integer),
        Column("pr_url", String),
        Column("last_error", String),
    )
    op.bulk_insert(repair_ownership, [{"singleton_id": 1}])
    with op.batch_alter_table(
        "repair_trial_jobs",
        table_args=(
            CheckConstraint("candidate_generation >= 0 AND repeat_index >= 0"),
            CheckConstraint("side IN ('baseline','candidate','regression')"),
            CheckConstraint("status IN ('running','completed','interrupted')"),
            CheckConstraint("status != 'completed' OR completed_at IS NOT NULL"),
        ),
    ) as batch:
        batch.create_foreign_key(
            "fk_trial_repair", "repair_runs", ["repair_id"], ["id"]
        )


def downgrade() -> None:
    with op.batch_alter_table(
        "repair_trial_jobs",
        table_args=(
            CheckConstraint("candidate_generation >= 0 AND repeat_index >= 0"),
            CheckConstraint("side IN ('baseline','candidate','regression')"),
            CheckConstraint("status IN ('running','completed','interrupted')"),
            CheckConstraint("status != 'completed' OR completed_at IS NOT NULL"),
        ),
    ) as batch:
        batch.drop_constraint("fk_trial_repair", type_="foreignkey")
    for name in (
        "repair_publications",
        "repair_acceptances",
        "repair_checkpoints",
        "repair_attempts",
        "repair_fixes",
        "repair_ownership",
        "repair_runs",
    ):
        op.drop_table(name)
