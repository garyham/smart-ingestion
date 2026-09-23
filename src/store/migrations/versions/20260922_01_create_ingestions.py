"""Create the ingestion state table."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260922_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS smart_files")
    op.create_table(
        "ingestions",
        sa.Column("ingestion_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=False),
        sa.Column("pipeline_version", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("current_step", sa.String(), nullable=False),
        sa.Column("source", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "steps",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "outputs",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "source_sha256 ~ '^[0-9a-f]{64}$'", name="ingestions_sha256_check"
        ),
        sa.CheckConstraint(
            "status IN ('processing', 'published', 'embedding', 'completed', 'failed')",
            name="ingestions_status_check",
        ),
        sa.PrimaryKeyConstraint("ingestion_id"),
        sa.UniqueConstraint(
            "source_sha256",
            "pipeline_version",
            name="ingestions_source_pipeline_key",
        ),
        schema="smart_files",
    )
    op.create_index(
        "ingestions_status_idx",
        "ingestions",
        ["status", "updated_at"],
        schema="smart_files",
    )


def downgrade() -> None:
    op.drop_index(
        "ingestions_status_idx", table_name="ingestions", schema="smart_files"
    )
    op.drop_table("ingestions", schema="smart_files")
