"""References the orchestrator passes between tasks and the store.

A reference names an artifact. It never carries a file path, an S3 key, a database table,
or a storage format - those belong to the service that owns the artifact.
"""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ArtifactStatus(StrEnum):
    """The outcomes an ingestion can record in its `status.json`. All three are terminal.

    `needs_intervention` is a valid outcome, not an error: the pipeline ran, produced what
    it could, and recorded why a person has to look at the result.
    """

    OK = "ok"
    NEEDS_INTERVENTION = "needs_intervention"
    FAILED = "failed"


class DocumentStatus(StrEnum):
    """Where a document last got to in the pipeline, for display only.

    Any flow may set it, and nothing reads it to decide what runs: Prefect sequences and
    retries the work, and each table's reuse key stops it being done twice. A worker that
    dies without a failure hook leaves the last value standing until the next run.
    """

    UPLOADED = "uploaded"
    INGESTING = "ingesting"
    EMBEDDING = "embedding"
    OK = "ok"
    NEEDS_INTERVENTION = "needs_intervention"
    FAILED = "failed"


class Ref(BaseModel):
    model_config = ConfigDict(frozen=True)


class UploadRef(Ref):
    """Where a client may write one document's bytes, once.

    `document_id` is a candidate: nothing is recorded until the bytes have been hashed,
    and bytes that are already a document resolve to that document instead, so the
    candidate is discarded. The client must send `upload_headers` exactly - they are part
    of the signature, and they make the write happen once, at the declared size.
    """

    document_id: UUID
    upload_url: str
    upload_headers: dict[str, str]
    expires_at: datetime


class DocumentRef(Ref):
    """One document, identified by the content of its source bytes.

    The same bytes are the same document. `document_id` is a generated surrogate with a
    uniqueness rule on `content_id` behind it, so it is stable but not derived.
    """

    document_id: UUID
    content_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExtractionRef(Ref):
    """What one extractor version got out of one document, kept by the store.

    It carries the MIME type because routing needs nothing else; the text and metadata
    are read back from the store by this reference.
    """

    extraction_id: UUID
    document_id: UUID
    extractor: str
    mime_type: str
    reused: bool = False


class ChunksRef(Ref):
    """The chunks one chunker version cut from one extraction's text of a document."""

    extraction_id: UUID
    document_id: UUID
    extractor: str
    chunker: str
    chunk_count: int = Field(ge=0)
    reused: bool = False


class EmbeddingsRef(Ref):
    """The embeddings one embedder version holds for one document's chunks.

    `embedded` counts what this run wrote; the rest were already there and were reused.
    """

    document_id: UUID
    embedder: str
    chunk_count: int = Field(ge=0)
    embedded: int = Field(default=0, ge=0)

    @property
    def reused(self) -> bool:
        return self.embedded == 0
