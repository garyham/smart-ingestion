"""Every table the store owns: documents, extractions, chunks, and embeddings.

They form one tree rooted at the document, every edge a cascading foreign key, so deleting
a document deletes everything derived from it.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pgvector.sqlalchemy import SPARSEVEC, Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    false,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from db import SCHEMA, Base


class Document(Base):
    """A document, identified by the SHA-256 of its bytes.

    The same bytes are the same document, however many times they are uploaded. The
    primary key is a generated surrogate so references stay short and stable, and the
    uniqueness rule on `content_id` is what actually decides identity. An upload is given a
    candidate `document_id` before any bytes arrive, but the row is only written once they
    have been hashed: a candidate whose bytes are already a document never gets a row.

    `status` is where the pipeline last got to, for display; nothing decides what runs
    from it. `published` turns true once the document is completely ingested and stays
    true, so a later run that fails leaves what was already built searchable.
    """

    __tablename__ = "documents"
    __table_args__ = (
        CheckConstraint(
            "content_id ~ '^[0-9a-f]{64}$'", name="documents_content_check"
        ),
        CheckConstraint(
            "status IN ('uploaded', 'ingesting', 'embedding', 'ok', "
            "'needs_intervention', 'failed')",
            name="documents_status_check",
        ),
        UniqueConstraint("content_id", name="documents_content_key"),
        Index("documents_created_idx", "created_at"),
        {"schema": SCHEMA},
    )

    document_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    content_id: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    bucket: Mapped[str] = mapped_column(String, nullable=False)
    object_key: Mapped[str] = mapped_column(String, nullable=False)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    content_type: Mapped[str] = mapped_column(String, nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="uploaded", server_default="uploaded"
    )
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Extraction(Base):
    """What one extractor version got out of one document: its MIME type and metadata.

    The unique constraint is the reuse rule: an extractor version extracts a document once.
    The text is kept as an object named by `extraction_id`, written before the row, so a
    row is only there once its text is. Two racing runs each write their own object, and
    the one whose insert loses removes its copy.
    """

    __tablename__ = "extractions"
    __table_args__ = (
        UniqueConstraint(
            "document_id",
            "extractor_name",
            "extractor_version",
            name="extractions_reuse_key",
        ),
        {"schema": SCHEMA},
    )

    extraction_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    document_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.documents.document_id", ondelete="CASCADE"),
        nullable=False,
    )
    extractor_name: Mapped[str] = mapped_column(String, nullable=False)
    extractor_version: Mapped[str] = mapped_column(String, nullable=False)
    mime_type: Mapped[str] = mapped_column(String, nullable=False)
    # `metadata` is reserved on a declarative class.
    document_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Chunk(Base):
    """One chunk, owned by the extraction whose text it was cut from.

    The unique constraint is the reuse rule: an extraction and the chunker version that cut
    it produce exactly one set of chunks. Operator versions are immutable, so there is no
    configuration in the key, and `pipeline_version` is absent by design.

    There is no set row and no status. An extraction's chunks are written in one
    transaction, so they are all there or none of them are, and deleting the extraction -
    or its document - deletes them.
    """

    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint(
            "extraction_id",
            "chunker_name",
            "chunker_version",
            "chunk_index",
            name="chunks_reuse_key",
        ),
        Index("chunks_chunker_idx", "chunker_name", "chunker_version"),
        {"schema": SCHEMA},
    )

    chunk_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    extraction_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.extractions.extraction_id", ondelete="CASCADE"),
        nullable=False,
    )
    chunker_name: Mapped[str] = mapped_column(String, nullable=False)
    chunker_version: Mapped[str] = mapped_column(String, nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    headings: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list, server_default="[]"
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Embedding(Base):
    """One chunk, embedded by one embedder version.

    The primary key is the reuse rule: an embedder version embeds a chunk once. Embedder
    versions are immutable, so there is no configuration in the key. The row belongs to its
    chunk, which belongs to its extraction and so to its document, so deleting the document
    deletes its embeddings.

    `vectors.py` reads and writes it in raw SQL, because pgvector's ranking operators have
    no ORM spelling. This model is what puts the table in `Base.metadata`, so Alembic sees
    the whole schema.
    """

    __tablename__ = "embeddings"
    __table_args__ = (
        Index("embeddings_embedder_idx", "embedder_name", "embedder_version"),
        {"schema": SCHEMA},
    )

    chunk_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.chunks.chunk_id", ondelete="CASCADE"),
        primary_key=True,
    )
    embedder_name: Mapped[str] = mapped_column(String, primary_key=True)
    embedder_version: Mapped[str] = mapped_column(String, primary_key=True)
    # Undimensioned on purpose: the dense model's width is not fixed at schema time.
    dense_embedding: Mapped[list[float]] = mapped_column(Vector(), nullable=False)
    sparse_embedding: Mapped[object] = mapped_column(SPARSEVEC(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
