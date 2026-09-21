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

messages = Table(
    "messages",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id")),
    Column("conversation_sequence", Integer, nullable=False),
    Column("role", String, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime(), nullable=False),
    UniqueConstraint("task_id", "conversation_sequence"),
    CheckConstraint("role IN ('user','assistant')"),
    CheckConstraint("status IN ('completed','interrupted')"),
)

message_parts = Table(
    "message_parts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("message_id", ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
    Column("part_sequence", Integer, nullable=False),
    Column("kind", String, nullable=False),
    Column("text_content", String, nullable=False),
    UniqueConstraint("message_id", "part_sequence"),
    CheckConstraint("part_sequence > 0"),
    CheckConstraint("kind = 'text'"),
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
