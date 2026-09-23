"""The embed-document flow, and the `embed-release` queue entry point and worker that run it."""

from uuid import UUID

from prefect import flow, task
from prefect.task_worker import serve

from contracts.chunks import Chunk
from contracts.embeddings import Embedding
from contracts.refs import DocumentStatus, EmbeddingsRef
from ingestion.common import (
    RETRY,
    document_store,
    error_detail,
    record_crash,
    record_status,
)
from operators import embedders
from releases import active_release


def _embed_crashed(_flow, flow_run, state) -> None:
    record_crash(UUID(str(flow_run.parameters["document_id"])), state)


@task(name="find-pending-chunks", retries=3, **RETRY)
def find_pending_chunks(
    document_id: UUID, extractor: str, chunker: str, embedder: str
) -> tuple[int, list[Chunk]]:
    """How many chunks the document has, and which this embedder has yet to embed."""
    store = document_store()
    extraction = store.find_extraction(document_id, extractor)
    if extraction is None:
        return 0, []
    chunks = store.find_chunks(extraction, chunker)
    pending = store.unembedded_chunks(extraction, chunker, embedder)
    return (chunks.chunk_count if chunks else 0), pending


@task(name="encode-chunks", retries=2, **RETRY)
def encode_chunks(embedder: str, texts: list[str]) -> list[Embedding]:
    """The expensive stage: model inference, on whatever device is configured."""
    return embedders.get(embedder).embed(texts)


@task(name="store-embeddings", retries=2, **RETRY)
def store_embeddings(
    embedder: str, chunk_ids: list[UUID], embeddings: list[Embedding]
) -> int:
    """Every embedding in the batch commits together, or none of them do."""
    return document_store().add_embeddings(
        embedder, dict(zip(chunk_ids, embeddings, strict=True))
    )


@flow(name="embed-document", on_crashed=[_embed_crashed])
def embed_document_flow(
    document_id: UUID, extractor: str, chunker: str, embedder: str
) -> EmbeddingsRef:
    """Embed one document's chunks with one embedder: find what is missing, encode, store."""
    chunk_count, pending = find_pending_chunks(
        document_id, extractor, chunker, embedder
    )
    embedded = 0
    if pending:
        embeddings = encode_chunks(embedder, [chunk.text for chunk in pending])
        embedded = store_embeddings(
            embedder, [chunk.chunk_id for chunk in pending], embeddings
        )
    return EmbeddingsRef(
        document_id=document_id,
        embedder=embedder,
        chunk_count=chunk_count,
        embedded=embedded,
    )


@task(name="embed-release", retries=2, log_prints=True, **RETRY)
def embed_release(document_id: str, release_name: str) -> dict:
    """Embed one document's chunks with every embedder the release names.

    Chunks an embedder has already embedded are reused rather than re-encoded, so adding
    an embedder to a release never re-runs chunking or re-embeds with the others. Once
    every embedder has run, the document is published.
    """
    release = active_release()
    document = UUID(document_id)
    record_status(document, DocumentStatus.EMBEDDING)

    built = []
    try:
        for embedder in release.embedders:
            embeddings = embed_document_flow(
                document, release.extractor, release.chunker, embedder
            )
            built.append(
                {
                    "embedder": embedder,
                    "chunk_count": embeddings.chunk_count,
                    "embedded": embeddings.embedded,
                    "reused": embeddings.reused,
                }
            )
    except Exception as error:
        record_status(document, DocumentStatus.FAILED, error_detail(error))
        raise

    document_store().mark_published(document)
    return {"release": release_name, "embeddings": built}


def embedding_worker() -> None:
    serve(embed_release, limit=1)
