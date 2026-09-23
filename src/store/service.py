"""DocumentStore: persists documents, their bytes, and everything derived from them.

It owns every table and every object key - documents, extractions, chunks, and
embeddings - and one Alembic chain for all of them. It stores, reads, and searches; it
never decides. Extractors, chunkers, and embedders are stateless operators the flow runs
first, and the flow hands what they produced to `add_extraction`, `add_chunks`, and
`add_embeddings`.

Two identities, kept apart on purpose:

- `content_id` - the SHA-256 of the bytes, which is what a document *is*.
- `document_id` - the surrogate key that names that document in a reference.

An upload is handed a candidate `document_id` and writes straight to that document's key,
once. Nothing is recorded until the bytes are hashed: if they are already a document, the
candidate's bytes are discarded and the existing document is the answer.

This service stores and reads; it does not decide. Hashing and validating an upload is the
caller's job - the ingestion flow reads the upload through `open_upload` and hands the
result to `add_document`, which records whatever those bytes turned out to be.
"""

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field

from contracts.chunks import Chunk, ChunkDraft
from contracts.embeddings import Embedding, SearchHit, SearchOptions
from contracts.extractions import Extraction
from contracts.operators import OperatorRef
from contracts.refs import (
    ChunksRef,
    DocumentRef,
    DocumentStatus,
    ExtractionRef,
    UploadRef,
)
from store import repository, vectors
from store.objects import (
    DOCUMENT_PREFIX,
    document_id_of,
    document_key,
    document_prefixes,
    extraction_text_key,
    source_prefix,
)
from upload_events import S3_BUCKET, presign_client, s3_client

UPLOAD_TTL_SECONDS = 15 * 60
READ_BLOCK_SIZE = 1024 * 1024
DELETE_BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


class UnknownDocument(LookupError):
    pass


class UnknownExtraction(LookupError):
    pass


class UploadNotWritten(LookupError):
    """No bytes were ever written for this candidate document."""


class UploadRequest(BaseModel):
    """What a client tells us before it writes any bytes.

    It cannot name a document: the bytes decide that. The size is signed into the upload
    URL, so the bytes that arrive are exactly this many or none at all.
    """

    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=255)
    size: int = Field(gt=0)


class UploadInfo(BaseModel):
    """What a caller may know about the bytes written for a candidate: never where."""

    model_config = ConfigDict(frozen=True)

    document_id: UUID
    filename: str
    content_type: str
    size: int
    uploaded_at: datetime


class DocumentInfo(BaseModel):
    """What a caller may know about a document: its identity, never its location."""

    model_config = ConfigDict(frozen=True)

    document_id: UUID
    content_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    filename: str
    content_type: str
    size: int = Field(ge=0)
    status: DocumentStatus
    error: dict[str, Any] | None = None
    published: bool
    created_at: datetime


class DocumentStore:
    def __init__(
        self, client=None, bucket: str = S3_BUCKET, presigner=None
    ) -> None:
        self._client = client
        self._presigner = presigner or client
        self._bucket = bucket

    @property
    def client(self):
        if self._client is None:
            self._client = s3_client()
        return self._client

    @property
    def presigner(self):
        """Signs upload URLs for the endpoint the client will call, not this one."""
        if self._presigner is None:
            self._presigner = presign_client()
        return self._presigner

    # ------------------------------------------------------------------------- uploads

    def create_upload(self, request: UploadRequest) -> UploadRef:
        """Hand back a URL the client can write one candidate document's bytes to.

        Nothing is recorded. The URL is signed for one write of exactly the declared size:
        the store refuses a second write to the key, so the bytes that get hashed are the
        bytes the document keeps.
        """
        document_id = uuid4()
        expires_at = datetime.now(UTC) + timedelta(seconds=UPLOAD_TTL_SECONDS)
        upload_url = self.presigner.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self._bucket,
                "Key": document_key(document_id, request.filename),
                "ContentType": request.content_type,
                "ContentLength": request.size,
                "IfNoneMatch": "*",
            },
            ExpiresIn=UPLOAD_TTL_SECONDS,
        )
        return UploadRef(
            document_id=document_id,
            upload_url=upload_url,
            upload_headers={
                "Content-Type": request.content_type,
                "If-None-Match": "*",
            },
            expires_at=expires_at,
        )

    def describe_upload(self, document_id: UUID) -> UploadInfo:
        """What was written for a candidate document, without reading the bytes."""
        key = self._upload_key(document_id)
        head = self.client.head_object(Bucket=self._bucket, Key=key)
        return UploadInfo(
            document_id=document_id,
            filename=key.rsplit("/", 1)[-1],
            content_type=head.get("ContentType") or "application/octet-stream",
            size=head["ContentLength"],
            uploaded_at=head["LastModified"],
        )

    def open_upload(self, document_id: UUID) -> BinaryIO:
        """Stream the bytes a client wrote for a candidate document."""
        key = self._upload_key(document_id)
        return self.client.get_object(Bucket=self._bucket, Key=key)["Body"]

    def add_document(
        self, document_id: UUID, *, content_id: str, size: int
    ) -> DocumentRef:
        """Record a candidate's bytes as the document with this content ID.

        The bytes are already at the candidate's key, so a new document is just its row.
        Bytes that are already a document resolve to it - recording the same content twice,
        or from two racing uploads, leaves one document - and the candidate's copy is
        discarded.
        """
        upload = self.describe_upload(document_id)
        document = repository.add_document(
            document_id,
            content_id=content_id,
            title=upload.filename,
            bucket=self._bucket,
            object_key=document_key(document_id, upload.filename),
            filename=upload.filename,
            content_type=upload.content_type,
            size=size,
            now=datetime.now(UTC),
        )
        if document.document_id != document_id:
            self._discard_upload(document_id)
        return _ref(document)

    def discard_abandoned_uploads(self, written_before: datetime) -> list[UUID]:
        """Remove candidates' bytes that never became a document.

        A candidate is abandoned when everything under its key was written before
        `written_before` and no document carries its ID. Returns the candidates removed.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        newest: dict[UUID, datetime] = {}
        keys: dict[UUID, list[str]] = {}
        for page in paginator.paginate(
            Bucket=self._bucket, Prefix=f"{DOCUMENT_PREFIX}/"
        ):
            for item in page.get("Contents", []):
                document_id = document_id_of(item["Key"])
                if document_id is None:
                    continue
                keys.setdefault(document_id, []).append(item["Key"])
                newest[document_id] = max(
                    item["LastModified"], newest.get(document_id, item["LastModified"])
                )

        stale = {
            document_id
            for document_id, written in newest.items()
            if written < written_before
        }
        abandoned = sorted(stale - repository.existing_document_ids(stale))
        self._delete_keys(
            self._bucket,
            [key for document_id in abandoned for key in keys[document_id]],
        )
        return abandoned

    # ----------------------------------------------------------------------- documents

    def lookup(self, content_id: str) -> DocumentRef | None:
        """The document these bytes already are, if any."""
        document = repository.find_by_content(content_id)
        return _ref(document) if document is not None else None

    def describe(self, document_id: UUID) -> DocumentInfo:
        """What a reader may know about a document: never its bucket or its key."""
        return _info(self._document(document_id))

    def list_documents(self, limit: int = 100, offset: int = 0) -> list[DocumentInfo]:
        """Every document, newest first, described the same way as `describe`."""
        return [
            _info(document)
            for document in repository.list_documents(limit=limit, offset=offset)
        ]

    def open(self, document_id: UUID) -> BinaryIO:
        """Stream a document's bytes. Callers never learn where they came from."""
        document = self._document(document_id)
        return self.client.get_object(
            Bucket=document.bucket, Key=document.object_key
        )["Body"]

    def download(self, document_id: UUID, destination: Path) -> Path:
        """Copy a document to a local path, for operators that need a file."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self.open(document_id) as body, destination.open("wb") as target:
            for block in iter(lambda: body.read(READ_BLOCK_SIZE), b""):
                target.write(block)
        return destination

    def document_ref(self, document_id: UUID) -> DocumentRef:
        return _ref(self._document(document_id))

    def set_status(
        self,
        document_id: UUID,
        status: DocumentStatus,
        error: dict[str, Any] | None = None,
    ) -> None:
        """Record where the pipeline last got to with this document, for display.

        Any flow may call it; the last write wins. A document deleted while a run was
        still going has nowhere to record it, so that is not an error.
        """
        repository.set_status(document_id, DocumentStatus(status).value, error)

    def mark_published(self, document_id: UUID) -> None:
        """Record that the document is completely ingested and searchable.

        It stays published: a later run that fails sets the status, but what the earlier
        run built is still there to search.
        """
        repository.mark_published(document_id)

    def delete_document(self, document_id: UUID) -> None:
        """Delete a document, everything derived from it, and every object it owns.

        One transaction: the rows go first, cascading to chunks and embeddings; the
        objects are removed before it commits. If removing them fails the
        rows roll back, so the document is still there to delete again.
        Deleting objects that are already gone is a no-op, so a retry always converges.
        """

        def remove_objects(document) -> None:
            keys = [document.object_key]
            for prefix in document_prefixes(document.document_id):
                keys.extend(self._keys_under(document.bucket, prefix))
            self._delete_keys(document.bucket, keys)

        if not repository.delete_document(document_id, remove_objects):
            raise UnknownDocument(f"no document {document_id}")

    # --------------------------------------------------------------------- extractions

    def add_extraction(
        self,
        document_id: UUID,
        extractor: str | OperatorRef,
        extraction: Extraction,
    ) -> ExtractionRef:
        """Keep what an extractor version got out of a document.

        The text is written first, under a new `extraction_id`, and the row last, so an
        extraction is only found once its text is there. Two racing runs each write their
        own text; the row's reuse key keeps one, and the run whose insert lost removes its
        copy and hands back the winner.
        """
        operator = OperatorRef.parse(extractor)
        document = self._document(document_id)
        extraction_id = uuid4()
        text_key = extraction_text_key(document_id, extraction_id)
        self.client.put_object(
            Bucket=document.bucket,
            Key=text_key,
            Body=extraction.text.encode(),
            ContentType="text/plain; charset=utf-8",
        )
        record = repository.add_extraction(
            extraction_id,
            document_id,
            operator,
            mime_type=extraction.mime_type,
            # Round-trip through JSON: Tika metadata can hold values JSONB cannot.
            metadata=json.loads(json.dumps(extraction.metadata, default=str)),
        )
        if record.extraction_id != extraction_id:
            self._discard_extraction_text(document.bucket, text_key)
            return _extraction_ref(record, reused=True)
        return _extraction_ref(record)

    def find_extraction(
        self, document_id: UUID, extractor: str | OperatorRef
    ) -> ExtractionRef | None:
        """What this extractor version already got out of this document, if anything."""
        record = repository.find_extraction(document_id, OperatorRef.parse(extractor))
        return _extraction_ref(record, reused=True) if record else None

    def extracted_text(self, extraction: ExtractionRef) -> str:
        """The plain text an extractor version got out of a document."""
        document = self._document(extraction.document_id)
        text_key = extraction_text_key(extraction.document_id, extraction.extraction_id)
        body = self.client.get_object(Bucket=document.bucket, Key=text_key)["Body"]
        with body:
            return body.read().decode()

    def extraction_metadata(self, extraction: ExtractionRef) -> dict[str, Any]:
        """The metadata an extractor version got out of a document."""
        record = repository.get_extraction(extraction.extraction_id)
        if record is None:
            raise UnknownExtraction(f"no extraction {extraction.extraction_id}")
        return record.document_metadata

    def _discard_extraction_text(self, bucket: str, key: str) -> None:
        """Remove the text of an extraction whose row lost the race.

        The winner's row is already recorded, so failing here must not fail the run: the
        object sits under the document's prefix and goes when the document does.
        """
        try:
            self._delete_keys(bucket, [key])
        except (ClientError, RuntimeError):
            logger.warning(
                "could not remove the text of duplicate extraction %s", key, exc_info=True
            )

    # -------------------------------------------------------------------------- chunks

    def add_chunks(
        self,
        extraction: ExtractionRef,
        chunker: str | OperatorRef,
        chunks: Sequence[ChunkDraft],
    ) -> ChunksRef:
        """Keep an extraction's chunks in one transaction: all of them, or none.

        The same chunker version cuts the same chunks from the same text every time, so
        writing them again - or from two racing runs - leaves one copy.
        """
        cut = OperatorRef.parse(chunker)
        count = repository.add_chunks(extraction.extraction_id, cut, chunks)
        return _chunks_ref(extraction, cut, count)

    def find_chunks(
        self, extraction: ExtractionRef, chunker: str | OperatorRef
    ) -> ChunksRef | None:
        """The chunks this chunker version already cut from this extraction, if any."""
        cut = OperatorRef.parse(chunker)
        count = repository.count_chunks(extraction.extraction_id, cut)
        if not count:
            return None
        return _chunks_ref(extraction, cut, count, reused=True)

    def chunks(
        self, extraction: ExtractionRef, chunker: str | OperatorRef
    ) -> list[Chunk]:
        """Every chunk this chunker version cut from the extraction, in order."""
        return repository.read_chunks(
            extraction.extraction_id, OperatorRef.parse(chunker)
        )

    def get_chunks(self, chunk_ids: Iterable[UUID]) -> list[Chunk]:
        """The named chunks, whichever documents they came from."""
        return repository.get_chunks(chunk_ids)

    # ---------------------------------------------------------------------- embeddings

    def add_embeddings(
        self, embedder: str | OperatorRef, embeddings: Mapping[UUID, Embedding]
    ) -> int:
        """Keep a batch of embeddings, keyed by chunk, in one transaction.

        Returns how many were given. A chunk this embedder version already embedded keeps
        its first embedding.
        """
        return repository.add_embeddings(OperatorRef.parse(embedder), embeddings)

    def unembedded_chunks(
        self,
        extraction: ExtractionRef,
        chunker: str | OperatorRef,
        embedder: str | OperatorRef,
    ) -> list[Chunk]:
        """The extraction's chunks this embedder version has yet to embed, in order.

        Whatever is already embedded is left out, so a retry only does the work the last
        attempt did not finish.
        """
        return repository.unembedded_chunks(
            extraction.extraction_id,
            OperatorRef.parse(chunker),
            OperatorRef.parse(embedder),
        )

    def search_embeddings(
        self, query: Embedding, options: SearchOptions
    ) -> list[SearchHit]:
        """Search one embedder version's embeddings with a query that version embedded.

        The store never runs a model: the caller embeds the query and asks the embedder
        for its `search_options()`, so ranking stays part of what the version means.
        """
        return vectors.search(query, options)

    # ------------------------------------------------------------------------ internal

    def _document(self, document_id: UUID):
        document = repository.get_document(document_id)
        if document is None:
            raise UnknownDocument(f"no document {document_id}")
        return document

    def _upload_key(self, document_id: UUID) -> str:
        """The one key a candidate's upload URL was signed for, if it was written."""
        keys = self._keys_under(self._bucket, source_prefix(document_id))
        if not keys:
            raise UploadNotWritten(f"nothing was uploaded for {document_id}")
        return keys[0]

    def _keys_under(self, bucket: str, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        return [
            item["Key"]
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for item in page.get("Contents", [])
        ]

    def _discard_upload(self, document_id: UUID) -> None:
        """Remove a candidate's bytes once they have resolved to another document.

        The document is already recorded, so failing here must not fail the commit: the
        candidate has no row, and `discard_abandoned_uploads` removes it later.
        """
        try:
            self._delete_keys(
                self._bucket, self._keys_under(self._bucket, source_prefix(document_id))
            )
        except (ClientError, RuntimeError):
            logger.warning(
                "could not remove the bytes of duplicate upload %s",
                document_id,
                exc_info=True,
            )

    def _delete_keys(self, bucket: str, keys: list[str]) -> None:
        unique = sorted(set(keys))
        for start in range(0, len(unique), DELETE_BATCH_SIZE):
            batch = unique[start : start + DELETE_BATCH_SIZE]
            response = self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
            )
            if errors := response.get("Errors"):
                raise RuntimeError(f"could not delete {len(errors)} object(s): {errors}")


def _extraction_ref(record, reused: bool = False) -> ExtractionRef:
    return ExtractionRef(
        extraction_id=record.extraction_id,
        document_id=record.document_id,
        extractor=record.extractor,
        mime_type=record.mime_type,
        reused=reused,
    )


def _chunks_ref(
    extraction: ExtractionRef, chunker: OperatorRef, count: int, reused: bool = False
) -> ChunksRef:
    return ChunksRef(
        extraction_id=extraction.extraction_id,
        document_id=extraction.document_id,
        extractor=extraction.extractor,
        chunker=str(chunker),
        chunk_count=count,
        reused=reused,
    )


def _ref(document) -> DocumentRef:
    return DocumentRef(
        document_id=document.document_id, content_id=document.content_id
    )


def _info(document) -> DocumentInfo:
    return DocumentInfo(
        document_id=document.document_id,
        content_id=document.content_id,
        filename=document.filename,
        content_type=document.content_type,
        size=document.size,
        status=document.status,
        error=document.error,
        published=document.published,
        created_at=document.created_at,
    )
