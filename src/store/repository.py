"""The store's persistence: documents, extractions, chunks, and embeddings.

There is no claim and no lease. Each operator's output is inserted in one transaction, and
the table's unique key is what stops two runs becoming two copies: the second run's rows
conflict with the first's and are dropped, which is safe because an immutable operator
version produces the same output every time.
"""

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.dialects.postgresql import insert

from contracts.chunks import Chunk, ChunkDraft
from contracts.embeddings import Embedding
from contracts.operators import OperatorRef
from db import SessionLocal
from store import models, vectors
from store.models import Document

EXTRACTIONS_REUSE_KEY = "extractions_reuse_key"
CHUNKS_REUSE_KEY = "chunks_reuse_key"


class DocumentRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    document_id: UUID
    content_id: str
    title: str
    bucket: str
    object_key: str
    filename: str
    content_type: str
    size: int
    status: str
    error: dict[str, Any] | None
    published: bool
    created_at: datetime


def get_document(document_id: UUID) -> DocumentRecord | None:
    with SessionLocal() as session:
        row = session.get(Document, document_id)
        return DocumentRecord.model_validate(row) if row else None


def find_by_content(content_id: str) -> DocumentRecord | None:
    with SessionLocal() as session:
        row = session.scalars(
            select(Document).where(Document.content_id == content_id)
        ).one_or_none()
        return DocumentRecord.model_validate(row) if row else None


def add_document(
    document_id: UUID,
    *,
    content_id: str,
    title: str,
    bucket: str,
    object_key: str,
    filename: str,
    content_type: str,
    size: int,
    now: datetime,
) -> DocumentRecord:
    """Record a document, resolving a concurrent commit of the same bytes to one row.

    A commit whose bytes are already a document loses the insert and reads back the winner,
    so the same bytes never become two documents. A candidate's key is written once, so a
    second commit of the same candidate hashes the same bytes and finds its own row.
    """
    with SessionLocal.begin() as session:
        statement = (
            insert(Document)
            .values(
                document_id=document_id,
                content_id=content_id,
                title=title,
                bucket=bucket,
                object_key=object_key,
                filename=filename,
                content_type=content_type,
                size=size,
                created_at=now,
            )
            .on_conflict_do_nothing(constraint="documents_content_key")
            .returning(Document)
        )
        row = session.scalars(statement).one_or_none()
        if row is None:
            row = session.scalars(
                select(Document).where(Document.content_id == content_id)
            ).one()
        return DocumentRecord.model_validate(row)


def existing_document_ids(document_ids: Iterable[UUID]) -> set[UUID]:
    """Which of these IDs name a document."""
    ids = list(document_ids)
    if not ids:
        return set()
    with SessionLocal() as session:
        return set(
            session.scalars(
                select(Document.document_id).where(Document.document_id.in_(ids))
            )
        )


def list_documents(limit: int = 100, offset: int = 0) -> list[DocumentRecord]:
    with SessionLocal() as session:
        rows = session.scalars(
            select(Document)
            .order_by(Document.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return [DocumentRecord.model_validate(row) for row in rows]


def set_status(
    document_id: UUID, status: str, error: dict[str, Any] | None = None
) -> bool:
    """Overwrite where the document last got to. Returns False when it is gone."""
    with SessionLocal.begin() as session:
        result = session.execute(
            update(Document)
            .where(Document.document_id == document_id)
            .values(status=status, error=error)
        )
        return result.rowcount > 0


def mark_published(document_id: UUID) -> bool:
    """Record that the document is completely ingested. Returns False when it is gone."""
    with SessionLocal.begin() as session:
        result = session.execute(
            update(Document)
            .where(Document.document_id == document_id)
            .values(status="ok", error=None, published=True)
        )
        return result.rowcount > 0


def delete_document(
    document_id: UUID, remove_objects: Callable[[DocumentRecord], None]
) -> bool:
    """Delete a document and everything derived from it, in one transaction.

    The delete cascades through the foreign keys to its extractions, chunks, and
    embeddings.
    `remove_objects` runs after the rows are gone but before the commit, so a failure there
    rolls the rows back and the document can be deleted again. Returns False when there was
    no such document.
    """
    with SessionLocal.begin() as session:
        row = session.get(Document, document_id, with_for_update=True)
        if row is None:
            return False
        document = DocumentRecord.model_validate(row)
        session.execute(delete(Document).where(Document.document_id == document_id))
        remove_objects(document)
        return True


# ----------------------------------------------------------------------- extractions


class ExtractionRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    extraction_id: UUID
    document_id: UUID
    extractor_name: str
    extractor_version: str
    mime_type: str
    document_metadata: dict[str, Any]
    created_at: datetime

    @property
    def extractor(self) -> str:
        return f"{self.extractor_name}@{self.extractor_version}"


def find_extraction(
    document_id: UUID, extractor: OperatorRef
) -> ExtractionRecord | None:
    with SessionLocal() as session:
        row = session.scalars(
            select(models.Extraction).where(
                models.Extraction.document_id == document_id,
                models.Extraction.extractor_name == extractor.name,
                models.Extraction.extractor_version == extractor.version,
            )
        ).one_or_none()
        return ExtractionRecord.model_validate(row) if row else None


def get_extraction(extraction_id: UUID) -> ExtractionRecord | None:
    with SessionLocal() as session:
        row = session.get(models.Extraction, extraction_id)
        return ExtractionRecord.model_validate(row) if row else None


def add_extraction(
    extraction_id: UUID,
    document_id: UUID,
    extractor: OperatorRef,
    *,
    mime_type: str,
    metadata: dict[str, Any],
) -> ExtractionRecord:
    """Record an extraction, resolving two racing runs to one row.

    A run whose insert conflicts reads back the winner, so the caller can tell by the
    `extraction_id` whether its own text is the one the row names.
    """
    with SessionLocal.begin() as session:
        row = session.scalars(
            insert(models.Extraction)
            .values(
                extraction_id=extraction_id,
                document_id=document_id,
                extractor_name=extractor.name,
                extractor_version=extractor.version,
                mime_type=mime_type,
                document_metadata=metadata,
                created_at=datetime.now(UTC),
            )
            .on_conflict_do_nothing(constraint=EXTRACTIONS_REUSE_KEY)
            .returning(models.Extraction)
        ).one_or_none()
        if row is None:
            row = session.scalars(
                select(models.Extraction).where(
                    models.Extraction.document_id == document_id,
                    models.Extraction.extractor_name == extractor.name,
                    models.Extraction.extractor_version == extractor.version,
                )
            ).one()
        return ExtractionRecord.model_validate(row)


# ---------------------------------------------------------------------------- chunks


def _chunk(row: models.Chunk, extraction: models.Extraction) -> Chunk:
    return Chunk(
        chunk_id=row.chunk_id,
        extraction_id=row.extraction_id,
        document_id=extraction.document_id,
        extractor=f"{extraction.extractor_name}@{extraction.extractor_version}",
        chunker=f"{row.chunker_name}@{row.chunker_version}",
        index=row.chunk_index,
        headings=row.headings,
        text=row.text,
    )


def _select_chunks():
    return select(models.Chunk, models.Extraction).join(
        models.Extraction,
        models.Extraction.extraction_id == models.Chunk.extraction_id,
    )


def _chunks_of(extraction_id: UUID, chunker: OperatorRef):
    return (
        models.Chunk.extraction_id == extraction_id,
        models.Chunk.chunker_name == chunker.name,
        models.Chunk.chunker_version == chunker.version,
    )


def count_chunks(extraction_id: UUID, chunker: OperatorRef) -> int:
    with SessionLocal() as session:
        return session.scalar(
            select(func.count())
            .select_from(models.Chunk)
            .where(*_chunks_of(extraction_id, chunker))
        )


def add_chunks(
    extraction_id: UUID, chunker: OperatorRef, chunks: Sequence[ChunkDraft]
) -> int:
    """Insert an extraction's chunks in one transaction. Returns how many it holds."""
    now = datetime.now(UTC)
    rows = [
        {
            "chunk_id": uuid4(),
            "extraction_id": extraction_id,
            "chunker_name": chunker.name,
            "chunker_version": chunker.version,
            "chunk_index": chunk.index,
            "headings": chunk.headings,
            "text": chunk.text,
            "created_at": now,
        }
        for chunk in chunks
    ]
    with SessionLocal.begin() as session:
        if rows:
            session.execute(
                insert(models.Chunk).on_conflict_do_nothing(
                    constraint=CHUNKS_REUSE_KEY
                ),
                rows,
            )
        return session.scalar(
            select(func.count())
            .select_from(models.Chunk)
            .where(*_chunks_of(extraction_id, chunker))
        )


def read_chunks(extraction_id: UUID, chunker: OperatorRef) -> list[Chunk]:
    statement = (
        _select_chunks()
        .where(*_chunks_of(extraction_id, chunker))
        .order_by(models.Chunk.chunk_index)
    )
    with SessionLocal() as session:
        return [_chunk(*row) for row in session.execute(statement)]


def get_chunks(chunk_ids: Iterable[UUID]) -> list[Chunk]:
    ids = list(chunk_ids)
    if not ids:
        return []
    with SessionLocal() as session:
        rows = session.execute(
            _select_chunks().where(models.Chunk.chunk_id.in_(ids))
        )
        return [_chunk(*row) for row in rows]


# ------------------------------------------------------------------------ embeddings


def unembedded_chunks(
    extraction_id: UUID, chunker: OperatorRef, embedder: OperatorRef
) -> list[Chunk]:
    """The extraction's chunks this embedder version has not embedded, in order."""
    embedded = exists().where(
        models.Embedding.chunk_id == models.Chunk.chunk_id,
        models.Embedding.embedder_name == embedder.name,
        models.Embedding.embedder_version == embedder.version,
    )
    statement = (
        _select_chunks()
        .where(*_chunks_of(extraction_id, chunker), ~embedded)
        .order_by(models.Chunk.chunk_index)
    )
    with SessionLocal() as session:
        return [_chunk(*row) for row in session.execute(statement)]


def add_embeddings(embedder: OperatorRef, embeddings: Mapping[UUID, Embedding]) -> int:
    """Write a batch of embeddings in one transaction: all of them, or none."""
    with SessionLocal.begin() as session:
        return vectors.write(session, embedder, embeddings)
