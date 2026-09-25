"""opaque browser sessions

Revision ID: 0010_browser_sessions
Revises: 0009_task_integration
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_browser_sessions"
down_revision: str | None = "0009_task_integration"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "browser_sessions",
        sa.Column("session_hash", sa.String(64), primary_key=True),
        sa.Column("csrf_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime()),
    )


def downgrade() -> None:
    op.drop_table("browser_sessions")
