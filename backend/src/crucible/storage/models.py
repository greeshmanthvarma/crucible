from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.types import TypeDecorator

from crucible.domain.clock import require_utc


class UTCDateTime(TypeDecorator[datetime]):
    impl = DateTime
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        require_utc(value)
        return value.replace(tzinfo=None)

    def process_result_value(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC)


metadata = MetaData()

repositories = Table(
    "repositories",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("root_path", String, nullable=False, unique=True),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("settings_json", JSON, nullable=False, server_default="{}"),
)

tasks = Table(
    "tasks",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("repository_id", ForeignKey("repositories.id"), nullable=False),
    Column("source_ref", String, nullable=False),
    Column("base_revision", String),
    Column("workspace_path", String, nullable=False, unique=True),
    Column("status", String, nullable=False),
    Column("failure_code", String),
    Column("failure_detail", String),
    Column("next_task_sequence", Integer, nullable=False, server_default="1"),
    Column("next_conversation_sequence", Integer, nullable=False, server_default="1"),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("updated_at", UTCDateTime(), nullable=False),
    CheckConstraint("status IN ('provisioning','active','provisioning_failed')"),
    CheckConstraint("next_task_sequence > 0"),
    CheckConstraint("next_conversation_sequence > 0"),
    CheckConstraint(
        "base_revision IS NOT NULL OR "
        "(status = 'provisioning_failed' AND failure_code IS NOT NULL "
        "AND failure_code = 'revision_not_found')"
    ),
)

runs = Table(
    "runs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("triggering_message_id", ForeignKey("messages.id")),
    Column("status", String, nullable=False),
    Column("next_run_sequence", Integer, nullable=False, server_default="1"),
    Column("execution_id", String(36)),
    Column("lease_expires_at", UTCDateTime()),
    Column("heartbeat_at", UTCDateTime()),
    Column("outcome_code", String),
    Column("outcome_detail", String),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("started_at", UTCDateTime()),
    Column("completed_at", UTCDateTime()),
    Column("cancel_requested_at", UTCDateTime()),
    Column("cancel_code", String),
    Column("settings_snapshot_json", JSON, nullable=False, server_default="{}"),
    CheckConstraint(
        "status IN ('queued','running','completed','failed','interrupted','cancelled')",
        name="ck_runs_status",
    ),
    CheckConstraint("next_run_sequence > 0", name="ck_runs_next_run_sequence_positive"),
)

steps = Table(
    "steps",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("step_sequence", Integer, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("started_at", UTCDateTime()),
    Column("completed_at", UTCDateTime()),
    UniqueConstraint("run_id", "step_sequence"),
    CheckConstraint("step_sequence > 0"),
    CheckConstraint(
        "status IN "
        "('preparing','model_active','tools_active','completed','failed','interrupted')"
    ),
)

messages = Table(
    "messages",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id")),
    Column("step_id", ForeignKey("steps.id")),
    Column("conversation_sequence", Integer, nullable=False),
    Column("role", String, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime(), nullable=False),
    UniqueConstraint("task_id", "conversation_sequence"),
    CheckConstraint("role IN ('user','assistant','system','tool')"),
    CheckConstraint("status IN ('completed','interrupted')"),
)

message_parts = Table(
    "message_parts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("message_id", ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
    Column("part_sequence", Integer, nullable=False),
    Column("kind", String, nullable=False),
    Column("text_content", String),
    Column("reasoning_content", String),
    Column("tool_call_id", String(36)),
    Column("tool_result_id", String(36)),
    UniqueConstraint("message_id", "part_sequence"),
    CheckConstraint("part_sequence > 0"),
    CheckConstraint("kind IN ('text','reasoning','tool_call','tool_result')"),
)

context_manifests = Table(
    "context_manifests",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("step_id", ForeignKey("steps.id"), nullable=False, unique=True),
    Column("model", String, nullable=False),
    Column("parameters_json", JSON, nullable=False),
    Column("input_limit", Integer, nullable=False),
    Column("output_reserve", Integer, nullable=False),
    Column("threshold", String, nullable=False),
    Column("estimated_tokens", Integer, nullable=False),
    Column("message_ids_json", JSON, nullable=False),
    Column("part_ids_json", JSON, nullable=False),
    Column("instruction_digests_json", JSON, nullable=False),
    Column("tool_schema_digest", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
)

tool_calls = Table(
    "tool_calls",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("step_id", ForeignKey("steps.id"), nullable=False),
    Column("assistant_message_id", ForeignKey("messages.id"), nullable=False),
    Column("call_sequence", Integer, nullable=False),
    Column("name", String, nullable=False),
    Column("arguments_json", JSON, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("provider_correlation_id", String),
    Column("execution_mode", String, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("step_id", "call_sequence"),
    CheckConstraint("call_sequence > 0"),
    CheckConstraint("schema_version > 0"),
)

tool_results = Table(
    "tool_results",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("step_id", ForeignKey("steps.id"), nullable=False),
    Column("tool_call_id", ForeignKey("tool_calls.id"), nullable=False, unique=True),
    Column("status", String, nullable=False),
    Column("result_json", JSON, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("display_text", String, nullable=False),
    Column("error_code", String),
    Column("completion_sequence", Integer, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime(), nullable=False),
    Column("artifact_id", ForeignKey("artifacts.id")),
    UniqueConstraint("step_id", "completion_sequence"),
    CheckConstraint("completion_sequence > 0"),
    CheckConstraint("schema_version > 0"),
)

approvals = Table(
    "approvals",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("step_id", ForeignKey("steps.id"), nullable=False),
    Column("tool_call_id", ForeignKey("tool_calls.id"), nullable=False, unique=True),
    Column("spec_json", JSON, nullable=False),
    Column("spec_digest", String(64), nullable=False),
    Column("status", String, nullable=False),
    Column("decision_reason", String),
    Column("decided_by", String),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("decided_at", UTCDateTime()),
    CheckConstraint(
        "status IN ('pending','approved','denied','cancelled','invalidated')"
    ),
)

artifacts = Table(
    "artifacts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("media_type", String, nullable=False),
    Column("byte_length", Integer, nullable=False),
    Column("storage_identity", String, nullable=False),
    Column("sensitivity", String, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("task_id", "content_hash", "media_type", "sensitivity"),
    CheckConstraint("byte_length >= 0"),
)

external_resources = Table(
    "external_resources",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id")),
    Column("tool_call_id", ForeignKey("tool_calls.id")),
    Column("kind", String, nullable=False),
    Column("external_identity", String, nullable=False, unique=True),
    Column("mount_target", String),
    Column("status", String, nullable=False),
    Column("labels_json", JSON, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("updated_at", UTCDateTime(), nullable=False),
    CheckConstraint("kind IN ('volume','container')"),
    CheckConstraint(
        "status IN ('present','active','removed','missing','orphaned','error')"
    ),
)

task_events = Table(
    "task_events",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id")),
    Column("task_sequence", Integer, nullable=False),
    Column("run_sequence", Integer),
    Column("type", String, nullable=False),
    Column("schema_version", Integer, nullable=False, server_default="1"),
    Column("payload_json", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("task_id", "task_sequence"),
    UniqueConstraint("run_id", "run_sequence"),
    CheckConstraint("task_sequence > 0"),
    CheckConstraint("run_sequence > 0"),
    CheckConstraint(
        "(run_id IS NULL AND run_sequence IS NULL) OR "
        "(run_id IS NOT NULL AND run_sequence IS NOT NULL)"
    ),
    CheckConstraint("schema_version > 0"),
)

idempotency_records = Table(
    "idempotency_records",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("scope", String, nullable=False),
    Column("key", String, nullable=False),
    Column("request_hash", String, nullable=False),
    Column("response_status", Integer, nullable=False),
    Column("response_json", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("scope", "key"),
)

compactions = Table(
    "compactions",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("source_start_sequence", Integer, nullable=False),
    Column("source_end_sequence", Integer, nullable=False),
    Column("retained_tail_start_sequence", Integer, nullable=False),
    Column("summary_artifact_id", ForeignKey("artifacts.id"), nullable=False),
    Column("rendered_summary", String, nullable=False),
    Column("previous_compaction_id", ForeignKey("compactions.id")),
    Column("model", String, nullable=False),
    Column("parameters_json", JSON, nullable=False),
    Column("prompt_version", String, nullable=False),
    Column("input_tokens", Integer, nullable=False),
    Column("output_tokens", Integer, nullable=False),
    Column("resulting_context_estimate", Integer, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("task_id", "source_end_sequence"),
    CheckConstraint(
        "source_start_sequence > 0 AND source_end_sequence >= source_start_sequence"
    ),
    CheckConstraint("retained_tail_start_sequence > source_end_sequence"),
)

validation_attempts = Table(
    "validation_attempts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("run_id", ForeignKey("runs.id"), nullable=False),
    Column("attempt_number", Integer, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime()),
    UniqueConstraint("run_id", "attempt_number"),
)

validation_command_results = Table(
    "validation_command_results",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "validation_attempt_id", ForeignKey("validation_attempts.id"), nullable=False
    ),
    Column("command_sequence", Integer, nullable=False),
    Column("status", String, nullable=False),
    Column("approval_id", ForeignKey("approvals.id")),
    Column("tool_call_id", ForeignKey("tool_calls.id")),
    Column("artifact_id", ForeignKey("artifacts.id")),
    Column("exit_code", Integer),
    Column("summary", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime()),
    UniqueConstraint("validation_attempt_id", "command_sequence"),
)

result_revisions = Table(
    "result_revisions",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("commit_sha", String(40), nullable=False, unique=True),
    Column("parent_revision", String(40), nullable=False),
    Column("previous_result_revision_id", ForeignKey("result_revisions.id")),
    Column("diff_artifact_id", ForeignKey("artifacts.id"), nullable=False),
    Column("validation_snapshot_json", JSON, nullable=False),
    Column("summary", String, nullable=False),
    Column("created_by", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
)

integrations = Table(
    "integrations",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("result_revision_id", ForeignKey("result_revisions.id"), nullable=False),
    Column("repository_id", ForeignKey("repositories.id"), nullable=False),
    Column("target_ref", String, nullable=False),
    Column("expected_target_revision", String(40), nullable=False),
    Column("idempotency_key", String, nullable=False),
    Column("status", String, nullable=False),
    Column("observed_before_revision", String(40)),
    Column("observed_after_revision", String(40)),
    Column("failure_code", String),
    Column("failure_detail", String),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime()),
    UniqueConstraint("result_revision_id", "idempotency_key"),
)
