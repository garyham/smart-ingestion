"""Give extractions a table, and hang chunks off the extraction they were cut from.

An extraction was only objects: its text and a `metadata.json` that marked it complete. It
is now a row keyed by `(document_id, extractor_name, extractor_version)`, holding the MIME
type and metadata, with its text kept under its own `extraction_id`. Chunks reference the
extraction instead of repeating the document and extractor, so the tree is
documents -> extractions -> chunks -> embeddings.

Existing chunks cannot be pointed at an extraction row: the MIME type and metadata they
would need are only in object storage. Every chunk is deleted, with its embeddings, and
every document is unpublished, so the next run extracts, chunks, and embeds it again. The
old extraction objects are no longer read, and go with their document's prefix.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260923_09"
down_revision: str | None = "20260923_08"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "smart_files"


def upgrade() -> None:
    op.create_table(
        "extractions",
        sa.Column("extraction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("extractor_name", sa.String(), nullable=False),
        sa.Column("extractor_version", sa.String(), nullable=False),
        sa.Column("mime_type", sa.String(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["document_id"], [f"{SCHEMA}.documents.document_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("extraction_id"),
        sa.UniqueConstraint(
            "document_id",
            "extractor_name",
            "extractor_version",
            name="extractions_reuse_key",
        ),
        schema=SCHEMA,
    )

    op.execute(f"DELETE FROM {SCHEMA}.chunks")
    op.execute(
        f"UPDATE {SCHEMA}.documents SET published = false, status = 'uploaded', "
        "error = NULL"
    )

    op.drop_index("chunks_operators_idx", table_name="chunks", schema=SCHEMA)
    op.drop_constraint("chunks_reuse_key", "chunks", schema=SCHEMA)
    op.drop_column("chunks", "extractor_version", schema=SCHEMA)
    op.drop_column("chunks", "extractor_name", schema=SCHEMA)
    op.drop_column("chunks", "document_id", schema=SCHEMA)
    op.add_column(
        "chunks",
        sa.Column("extraction_id", postgresql.UUID(as_uuid=True), nullable=False),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "chunks_extraction_id_fkey",
        "chunks",
        "extractions",
        ["extraction_id"],
        ["extraction_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="CASCADE",
    )
    op.create_unique_constraint(
        "chunks_reuse_key",
        "chunks",
        ["extraction_id", "chunker_name", "chunker_version", "chunk_index"],
        schema=SCHEMA,
    )
    op.create_index(
        "chunks_chunker_idx",
        "chunks",
        ["chunker_name", "chunker_version"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    # Chunks cut from an extraction row have no document to fall back to, so they go too.
    op.execute(f"DELETE FROM {SCHEMA}.chunks")
    op.execute(f"UPDATE {SCHEMA}.documents SET published = false")

    op.drop_index("chunks_chunker_idx", table_name="chunks", schema=SCHEMA)
    op.drop_constraint("chunks_reuse_key", "chunks", schema=SCHEMA)
    op.drop_constraint("chunks_extraction_id_fkey", "chunks", schema=SCHEMA)
    op.drop_column("chunks", "extraction_id", schema=SCHEMA)
    op.add_column(
        "chunks",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "chunks_document_id_fkey",
        "chunks",
        "documents",
        ["document_id"],
        ["document_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="CASCADE",
    )
    op.add_column(
        "chunks", sa.Column("extractor_name", sa.String(), nullable=False), schema=SCHEMA
    )
    op.add_column(
        "chunks",
        sa.Column("extractor_version", sa.String(), nullable=False),
        schema=SCHEMA,
    )
    op.create_unique_constraint(
        "chunks_reuse_key",
        "chunks",
        [
            "document_id",
            "extractor_name",
            "extractor_version",
            "chunker_name",
            "chunker_version",
            "chunk_index",
        ],
        schema=SCHEMA,
    )
    op.create_index(
        "chunks_operators_idx",
        "chunks",
        ["extractor_name", "extractor_version", "chunker_name", "chunker_version"],
        schema=SCHEMA,
    )
    op.drop_table("extractions", schema=SCHEMA)
