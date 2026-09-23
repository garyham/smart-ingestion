"""The only place document object keys are built.

Callers outside the store hold a `DocumentRef`, never a bucket or a key, so the
layout below can change without touching the API or the pipeline.
"""

from uuid import UUID

from upload_events import ARTIFACT_PREFIX, safe_filename

DOCUMENT_PREFIX = "documents"
EXTRACTION_PREFIX = "extractions"


def document_key(document_id: UUID, filename: str) -> str:
    """Where a document's bytes live, immutably, for as long as it is retained.

    A client writes here directly, once, before the bytes are hashed: the key is the
    candidate document's, and it becomes the document's own when `add_document` records it.
    """
    return f"{DOCUMENT_PREFIX}/{document_id}/{safe_filename(filename)}"


def source_prefix(document_id: UUID) -> str:
    """The prefix that holds nothing but one document's source bytes."""
    return f"{DOCUMENT_PREFIX}/{document_id}/"


def document_id_of(key: str) -> UUID | None:
    """The document a source key belongs to, or None for a key outside the layout."""
    prefix, _, rest = key.partition("/")
    candidate = rest.partition("/")[0]
    if prefix != DOCUMENT_PREFIX:
        return None
    try:
        return UUID(candidate)
    except ValueError:
        return None


def extraction_text_key(document_id: UUID, extraction_id: UUID) -> str:
    """Where one extraction's text lives.

    It is named by the extraction, not the extractor version, so two racing runs never
    write the same key: each writes its own, and the one whose row loses removes it. It
    sits outside the source prefix, which holds nothing but the uploaded bytes.
    """
    return f"{EXTRACTION_PREFIX}/{document_id}/{extraction_id}.txt"


def document_prefixes(document_id: UUID) -> list[str]:
    """Every prefix that holds nothing but this document's objects.

    Extractions and ingestion bundles are included: they are derived from the document and
    go with it.
    """
    return [
        source_prefix(document_id),
        f"{EXTRACTION_PREFIX}/{document_id}/",
        f"{ARTIFACT_PREFIX}/{document_id}/",
    ]
