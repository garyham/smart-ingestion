"""Create the chunks table.

A chunk belongs to its document, so deleting the document deletes it. The unique
constraint is the reuse rule: one set of chunks per document and chunker version.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260922_04"
down_revision: str | None = "20260922_03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "chunks",
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunker_name", sa.String(), nullable=False),
        sa.Column("chunker_version", sa.String(), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column(
            "headings",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["smart_files.documents.document_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("chunk_id"),
        # The reuse rule: a chunker version cuts a document once. `pipeline_version` is
        # deliberately absent, and so is configuration - chunker versions are immutable.
        sa.UniqueConstraint(
            "document_id",
            "chunker_name",
            "chunker_version",
            "chunk_index",
            name="chunks_reuse_key",
        ),
        schema="smart_files",
    )
    op.create_index(
        "chunks_chunker_idx",
        "chunks",
        ["chunker_name", "chunker_version"],
        schema="smart_files",
    )


def downgrade() -> None:
    op.drop_index("chunks_chunker_idx", table_name="chunks", schema="smart_files")
    op.drop_table("chunks", schema="smart_files")
