"""Create the document store: documents identified by content, and upload sessions.

Everything derived from a document hangs off it by a cascading foreign key, so deleting
the document deletes its upload sessions here, and its ingestions below.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260922_03"
down_revision: str | None = "20260922_02"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("content_id", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("object_key", sa.String(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "content_id ~ '^[0-9a-f]{64}$'", name="documents_content_check"
        ),
        sa.PrimaryKeyConstraint("document_id"),
        # Identity: the same bytes are the same document. The primary key is a surrogate
        # so references stay short; this constraint is what actually decides identity.
        sa.UniqueConstraint("content_id", name="documents_content_key"),
        schema="smart_files",
    )
    op.create_index(
        "documents_created_idx", "documents", ["created_at"], schema="smart_files"
    )

    op.create_table(
        "uploads",
        sa.Column("upload_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("object_key", sa.String(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("declared_size", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        # Null until the bytes arrive: which document they belong to is decided by their
        # hash, which is not known when the session opens.
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'committed')", name="uploads_status_check"
        ),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["smart_files.documents.document_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("upload_id"),
        schema="smart_files",
    )
    op.create_index(
        "uploads_status_idx",
        "uploads",
        ["status", "expires_at"],
        schema="smart_files",
    )

    # Ingestions predate the document store, so a database migrated from before it may
    # hold rows naming documents it never recorded. NOT VALID leaves those rows alone while
    # enforcing the key - and its cascade - for every document that does exist.
    op.execute(
        """
        ALTER TABLE smart_files.ingestions
        ADD CONSTRAINT ingestions_document_id_fkey
        FOREIGN KEY (document_id) REFERENCES smart_files.documents (document_id)
        ON DELETE CASCADE NOT VALID
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "ingestions_document_id_fkey", "ingestions", schema="smart_files"
    )
    op.drop_index("uploads_status_idx", table_name="uploads", schema="smart_files")
    op.drop_table("uploads", schema="smart_files")
    op.drop_index(
        "documents_created_idx", table_name="documents", schema="smart_files"
    )
    op.drop_table("documents", schema="smart_files")
