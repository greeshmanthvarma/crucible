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
    CheckConstraint(
        "status IN ('queued','running','completed','failed','interrupted')"
    ),
    CheckConstraint("next_run_sequence > 0"),
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
    UniqueConstraint("step_id", "completion_sequence"),
    CheckConstraint("completion_sequence > 0"),
    CheckConstraint("schema_version > 0"),
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
