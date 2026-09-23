"""Record the extractor on every chunk, and make it part of the chunks' reuse key.

Extraction is its own operator now, so chunks are named by the extractor version whose
text they were cut from as well as the chunker version that cut it. Every existing row was
cut by `chunker@1`, which extracted with exactly what `tika@1` is, so that is what they
record.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260923_08"
down_revision: str | None = "20260923_07"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "smart_files"


def upgrade() -> None:
    for column, value in (("extractor_name", "tika"), ("extractor_version", "1")):
        op.add_column(
            "chunks",
            sa.Column(column, sa.String(), nullable=False, server_default=value),
            schema=SCHEMA,
        )
        op.alter_column("chunks", column, server_default=None, schema=SCHEMA)

    op.drop_constraint("chunks_reuse_key", "chunks", schema=SCHEMA)
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
    op.drop_index("chunks_chunker_idx", table_name="chunks", schema=SCHEMA)
    op.create_index(
        "chunks_operators_idx",
        "chunks",
        ["extractor_name", "extractor_version", "chunker_name", "chunker_version"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("chunks_operators_idx", table_name="chunks", schema=SCHEMA)
    op.create_index(
        "chunks_chunker_idx",
        "chunks",
        ["chunker_name", "chunker_version"],
        schema=SCHEMA,
    )
    # Chunks cut from any extraction but tika@1 have no place in the old key.
    op.execute(
        f"DELETE FROM {SCHEMA}.chunks "
        "WHERE extractor_name <> 'tika' OR extractor_version <> '1'"
    )
    op.drop_constraint("chunks_reuse_key", "chunks", schema=SCHEMA)
    op.create_unique_constraint(
        "chunks_reuse_key",
        "chunks",
        ["document_id", "chunker_name", "chunker_version", "chunk_index"],
        schema=SCHEMA,
    )
    op.drop_column("chunks", "extractor_version", schema=SCHEMA)
    op.drop_column("chunks", "extractor_name", schema=SCHEMA)
