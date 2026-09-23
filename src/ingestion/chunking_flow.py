"""The chunk-document flow: find or cut, and keep, the chunks of one extraction's text."""

from prefect import flow, task
from pydantic import BaseModel

from contracts.chunks import ChunkDraft
from contracts.refs import ArtifactStatus, ChunksRef, ExtractionRef
from ingestion.common import RETRY, document_store
from operators import chunkers


class ChunkedDocument(BaseModel):
    """What the chunk-document flow found: the outcome and the chunks."""

    status: ArtifactStatus
    chunks: ChunksRef | None = None
    error: dict | None = None


@task(name="find-chunks", retries=2, **RETRY)
def find_chunks(extraction: ExtractionRef, chunker: str) -> ChunksRef | None:
    """Short-circuit: the same text and the same chunker are already these chunks."""
    return document_store().find_chunks(extraction, chunker)


@task(name="split-text", retries=2, **RETRY)
def split_text(extraction: ExtractionRef, chunker: str) -> list[ChunkDraft]:
    """Read the extracted text back from the store and split it. Only the read retries."""
    return chunkers.get(chunker).split(document_store().extracted_text(extraction))


@task(name="store-chunks", retries=3, **RETRY)
def store_chunks(
    extraction: ExtractionRef, chunker: str, chunks: list[ChunkDraft]
) -> ChunksRef:
    """Write every chunk in one transaction: until it commits, nothing can read them."""
    return document_store().add_chunks(extraction, chunker, chunks)


@flow(name="chunk-document")
def chunk_document_flow(extraction: ExtractionRef, chunker: str) -> ChunkedDocument:
    """Chunk one extraction's text: find the chunks, or split it and store them.

    Chunks that already exist are reused rather than cut again - identical bytes are one
    document, so they are one set of chunks per extraction and chunker version however
    many times they are uploaded.
    """
    chunks = find_chunks(extraction, chunker)
    if chunks is None:
        pieces = split_text(extraction, chunker)
        if not pieces:
            # A valid outcome, not an error: the run happened and records why it holds
            # nothing.
            return ChunkedDocument(
                status=ArtifactStatus.NEEDS_INTERVENTION,
                error={"reason": "no_content_extracted"},
            )
        chunks = store_chunks(extraction, chunker, pieces)
    return ChunkedDocument(status=ArtifactStatus.OK, chunks=chunks)
