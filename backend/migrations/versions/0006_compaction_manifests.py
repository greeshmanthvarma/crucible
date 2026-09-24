"""associate context manifests with compactions

Revision ID: 0006_compaction_manifests
Revises: 0005_interactive_workflow
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_compaction_manifests"
down_revision: str | None = "0005_interactive_workflow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("context_manifests") as batch:
        batch.add_column(sa.Column("compaction_id", sa.String(36)))
        batch.create_foreign_key(
            "fk_context_manifests_compaction_id",
            "compactions",
            ["compaction_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("context_manifests") as batch:
        batch.drop_constraint("fk_context_manifests_compaction_id", type_="foreignkey")
        batch.drop_column("compaction_id")
