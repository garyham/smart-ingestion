"""Create the embeddings table.

An embedding belongs to its chunk, which belongs to its document, so deleting the document
deletes its embeddings. The primary key is the reuse rule: an embedder version embeds a
chunk once.

`smart_files.chunk_embeddings` is intentionally left in place. It holds no new writes, but
it keeps the pre-service embeddings readable through the rollback window.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import SPARSEVEC, Vector
from sqlalchemy.dialects import postgresql

revision: str = "20260922_05"
down_revision: str | None = "20260922_04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The extension has to exist before the column types it provides can be named.
    # `WITH SCHEMA public` pins where `vector` and `sparsevec` live instead of letting
    # search_path decide, so a cast resolves the same way in every connection. Existing
    # databases keep whichever schema they already installed it into.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
    op.create_table(
        "embeddings",
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("embedder_name", sa.String(), nullable=False),
        sa.Column("embedder_version", sa.String(), nullable=False),
        # Undimensioned on purpose: the dense model's width is not fixed at schema time.
        sa.Column("dense_embedding", Vector(), nullable=False),
        sa.Column("sparse_embedding", SPARSEVEC(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["chunk_id"],
            ["smart_files.chunks.chunk_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("chunk_id", "embedder_name", "embedder_version"),
        schema="smart_files",
    )
    op.create_index(
        "embeddings_embedder_idx",
        "embeddings",
        ["embedder_name", "embedder_version"],
        schema="smart_files",
    )


def downgrade() -> None:
    op.drop_index(
        "embeddings_embedder_idx", table_name="embeddings", schema="smart_files"
    )
    op.drop_table("embeddings", schema="smart_files")
