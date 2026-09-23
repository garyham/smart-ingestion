"""Create the embedding storage table."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import SPARSEVEC, Vector
from sqlalchemy.dialects import postgresql

revision: str = "20260922_02"
down_revision: str | None = "20260922_01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The extension has to exist before the column types it provides can be named.
    # `WITH SCHEMA public` pins where `vector` and `sparsevec` live instead of letting
    # search_path decide, so a cast resolves the same way in every connection. Existing
    # databases keep whichever schema they already installed it into.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
    op.create_table(
        "chunk_embeddings",
        sa.Column("ingestion_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column(
            "headings",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("dense_model", sa.Text(), nullable=False),
        sa.Column("sparse_model", sa.Text(), nullable=False),
        # Undimensioned on purpose: the dense model's width is not fixed at schema time.
        sa.Column("dense_embedding", Vector(), nullable=False),
        sa.Column("sparse_embedding", SPARSEVEC(), nullable=False),
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
        sa.PrimaryKeyConstraint(
            "ingestion_id",
            "chunk_index",
            "dense_model",
            "sparse_model",
        ),
        schema="smart_files",
    )
    op.create_index(
        "chunk_embeddings_document_idx",
        "chunk_embeddings",
        ["document_id", "ingestion_id"],
        schema="smart_files",
    )


def downgrade() -> None:
    op.drop_index(
        "chunk_embeddings_document_idx",
        table_name="chunk_embeddings",
        schema="smart_files",
    )
    op.drop_table("chunk_embeddings", schema="smart_files")
