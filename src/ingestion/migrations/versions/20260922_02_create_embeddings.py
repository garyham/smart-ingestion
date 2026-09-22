"""Create the embedding storage table."""

from collections.abc import Sequence

from alembic import op

revision: str = "20260922_02"
down_revision: str | None = "20260922_01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(
        """
        CREATE TABLE smart_files.chunk_embeddings (
            ingestion_id uuid NOT NULL,
            document_id uuid NOT NULL,
            chunk_index integer NOT NULL,
            headings jsonb NOT NULL DEFAULT '[]'::jsonb,
            content text NOT NULL,
            dense_model text NOT NULL,
            sparse_model text NOT NULL,
            dense_embedding vector NOT NULL,
            sparse_embedding sparsevec NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (
                ingestion_id,
                chunk_index,
                dense_model,
                sparse_model
            )
        )
        """
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
