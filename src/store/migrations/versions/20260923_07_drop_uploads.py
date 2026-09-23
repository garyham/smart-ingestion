"""Drop upload sessions: an upload writes straight to its candidate document's key.

The candidate's row is only written once the bytes are hashed, so there is nothing to
record while they are in flight. A session still pending when this runs loses its row; its
bytes under `uploads/` are no longer read by anything and can be removed with the prefix.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260923_07"
down_revision: str | None = "20260923_06"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("uploads_status_idx", table_name="uploads", schema="smart_files")
    op.drop_table("uploads", schema="smart_files")


def downgrade() -> None:
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
