"""Record a document's pipeline status on the document, and drop the ingestions table.

`status` is where the pipeline last got to, for display only; `published` turns true once
the document is completely ingested. Each document's latest ingestion is carried across
before the table goes, so documents already built stay published.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260923_06"
down_revision: str | None = "20260922_05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "documents",
        sa.Column(
            "status",
            sa.String(),
            server_default=sa.text("'uploaded'"),
            nullable=False,
        ),
        schema="smart_files",
    )
    op.add_column(
        "documents",
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema="smart_files",
    )
    op.add_column(
        "documents",
        sa.Column(
            "published", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
        schema="smart_files",
    )
    op.create_check_constraint(
        "documents_status_check",
        "documents",
        "status IN ('uploaded', 'ingesting', 'embedding', 'ok', "
        "'needs_intervention', 'failed')",
        schema="smart_files",
    )

    # A completed ingestion recorded the bundle's own outcome in `outputs.status`; the
    # in-flight states have no terminal outcome yet, so they carry across as in flight.
    op.execute(
        """
        WITH latest AS (
            SELECT DISTINCT ON (document_id)
                   document_id, status, outputs ->> 'status' AS outcome, error
            FROM smart_files.ingestions
            ORDER BY document_id, updated_at DESC
        )
        UPDATE smart_files.documents AS documents
        SET status = CASE
                WHEN latest.status = 'failed' THEN 'failed'
                WHEN latest.status = 'embedding' THEN 'embedding'
                WHEN latest.status IN ('processing', 'published') THEN 'ingesting'
                WHEN latest.outcome IN ('needs_intervention', 'failed')
                    THEN latest.outcome
                ELSE 'ok'
            END,
            error = latest.error,
            published = latest.status = 'completed'
                AND COALESCE(latest.outcome, 'ok') = 'ok'
        FROM latest
        WHERE documents.document_id = latest.document_id
        """
    )

    op.drop_index(
        "ingestions_status_idx", table_name="ingestions", schema="smart_files"
    )
    op.drop_table("ingestions", schema="smart_files")


def downgrade() -> None:
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
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["smart_files.documents.document_id"],
            ondelete="CASCADE",
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
    op.drop_constraint(
        "documents_status_check", "documents", schema="smart_files", type_="check"
    )
    op.drop_column("documents", "published", schema="smart_files")
    op.drop_column("documents", "error", schema="smart_files")
    op.drop_column("documents", "status", schema="smart_files")
