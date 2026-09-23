"""The ingest-document flow, and the `process-upload` queue entry point and worker that run it."""

import hashlib
import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from prefect import flow, task
from prefect.task_worker import serve
from pydantic import BaseModel

from contracts.extractions import Extraction
from contracts.refs import ChunksRef, DocumentRef, DocumentStatus, ExtractionRef
from ingestion.assets import stage_chunked
from ingestion.chunking_flow import ChunkedDocument, chunk_document_flow
from ingestion.common import (
    RETRY,
    document_store,
    error_detail,
    record_crash,
    record_outcome,
    record_status,
)
from ingestion.embedding_flow import embed_release
from ingestion.publish import publish_bundle
from ingestion.routing import DUCKDB, concurrency_slot, route_mime_type
from ingestion.xlsx_flow import xlsx_ingest_flow
from operators import extractors
from releases import active_release
from store.service import UnknownDocument, UploadInfo, UploadNotWritten
from upload_events import safe_filename

READ_BLOCK_SIZE = 1024 * 1024

# Bytes older than the commit window are refused; the sweep only removes a candidate once
# it is twice that old.
UPLOAD_COMMIT_WINDOW = timedelta(hours=12)


class InvalidUploadEvent(ValueError):
    pass


class InvalidUpload(ValueError):
    """Nothing usable arrived for the candidate document, and nothing will."""


def parse_upload_event(value: bytes | dict) -> dict:
    if isinstance(value, bytes):
        try:
            event = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidUploadEvent("message body is not valid JSON") from exc
    else:
        event = value

    if not isinstance(event, dict) or event.get("event") != "upload.notified":
        raise InvalidUploadEvent("message is not an upload.notified event")
    if event.get("schema_version") != 4:
        raise InvalidUploadEvent("message has no supported schema_version")
    for field in ("event_id", "document_id", "release"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise InvalidUploadEvent(f"message has no valid {field}")
    try:
        UUID(event["event_id"])
        UUID(event["document_id"])
    except ValueError as exc:
        raise InvalidUploadEvent("message has no valid identifiers") from exc
    return event


def check_upload(upload: UploadInfo) -> None:
    """The cheap checks: something arrived, and recently enough to commit.

    The size needs no check against what was declared: it is signed into the upload URL,
    so the bytes arrive at exactly that size or not at all.
    """
    if upload.size == 0:
        raise InvalidUpload("upload is empty")
    if datetime.now(UTC) - upload.uploaded_at > UPLOAD_COMMIT_WINDOW:
        raise InvalidUpload(f"upload {upload.document_id} is too old to commit")


def _retryable(_task, _task_run, state) -> bool:
    """An upload that never arrived or cannot be committed stays that way: never retry it."""
    failure = state.result(raise_on_failure=False)
    return not isinstance(failure, (InvalidUploadEvent, InvalidUpload))


def _ingest_crashed(flow, flow_run, state) -> None:
    """A candidate that was never committed, or was a duplicate, has no row to record on.

    Recording on a missing document is a no-op, so the candidate ID is enough.
    """
    record_crash(UUID(str(flow_run.parameters["event"]["document_id"])), state)


@task(name="commit-upload", retries=3, retry_condition_fn=_retryable, **RETRY)
def commit_upload(document_id: UUID) -> DocumentRef:
    """Hash what actually arrived for a candidate, and record it as a document.

    The bytes decide which document this is. New bytes become a document under the
    candidate's own ID; bytes that have been seen before resolve to the document they
    already are, and the candidate's copy is discarded.

    A candidate that already became a document is returned as it is, without hashing or
    the age check: a second `/notify`, a re-run of the flow, or a retry after the row was
    written all replay a committed candidate, possibly long after its commit window. A
    duplicate candidate cannot be replayed - its bytes are gone and no row carries its ID.
    """
    store = document_store()
    try:
        return store.document_ref(document_id)
    except UnknownDocument:
        pass

    try:
        check_upload(store.describe_upload(document_id))
        body = store.open_upload(document_id)
    except UploadNotWritten as error:
        raise InvalidUpload(str(error)) from error
    digest = hashlib.sha256()
    size = 0
    with body:
        for block in iter(lambda: body.read(READ_BLOCK_SIZE), b""):
            digest.update(block)
            size += len(block)

    return store.add_document(document_id, content_id=digest.hexdigest(), size=size)


class RoutedDocument(BaseModel):
    """Which path a document took, and - on the chunking path - what chunking found."""

    artifact_type: str
    chunked: ChunkedDocument | None = None


@task(name="find-extraction", retries=2, **RETRY)
def find_extraction(document_id: UUID, extractor: str) -> ExtractionRef | None:
    """Short-circuit: the same bytes and the same extractor are already this extraction."""
    return document_store().find_extraction(document_id, extractor)


@task(name="extract-document", retries=3, **RETRY)
def extract_document(doc: Path, extractor: str) -> Extraction:
    """The one full parse a document gets: its MIME type, text, and metadata.

    Tika is a network call, so this retries, holding a `tika` slot while it runs.
    """
    with concurrency_slot("tika"):
        return extractors.get(extractor).extract(doc.read_bytes(), doc.name)


@task(name="store-extraction", retries=3, **RETRY)
def store_extraction(
    document_id: UUID, extractor: str, extraction: Extraction
) -> ExtractionRef:
    """Keep the extraction, so later stages are handed a reference rather than its text."""
    return document_store().add_extraction(document_id, extractor, extraction)


# No retries: the subflow's own tasks retry, and retrying here would run the path again.
@task(name="route-document")
def route_document(
    doc: Path, extraction: ExtractionRef, chunker: str, output_root: Path
) -> RoutedDocument:
    """Dispatch the document to the path its MIME type belongs to."""
    artifact_type = route_mime_type(extraction.mime_type)
    print(f"{doc.name}: {extraction.mime_type} -> {artifact_type}")
    if artifact_type == DUCKDB:
        with concurrency_slot("xlsx"):
            xlsx_ingest_flow(doc, output_root, extraction)
        return RoutedDocument(artifact_type=artifact_type)
    return RoutedDocument(
        artifact_type=artifact_type,
        chunked=chunk_document_flow(extraction, chunker),
    )


@task(name="stage-bundle", retries=2, **RETRY)
def stage_bundle(
    doc: Path, doc_dir: Path, extraction: ExtractionRef, chunked: ChunkedDocument
) -> None:
    """Build a chunked document's bundle from what extraction and chunking found."""
    status = {"status": chunked.status.value, **(chunked.error or {})}
    stage_chunked(
        doc,
        doc_dir,
        status=status,
        extraction=document_store().extraction_metadata(extraction),
        extractor=extraction.extractor,
        chunker=chunked.chunks.chunker if chunked.chunks else None,
        chunk_count=chunked.chunks.chunk_count if chunked.chunks else 0,
    )


@task(name="publish-bundle", retries=3, **RETRY)
def publish_ingestion_bundle(
    document: DocumentRef,
    ingestion_id: str,
    filename: str,
    doc_dir: Path,
    artifact_type: str,
    chunks: ChunksRef | None = None,
) -> dict:
    """Upload every artifact, then the manifest last, as the completion marker."""
    return publish_bundle(
        str(document.document_id),
        ingestion_id,
        {
            "document_id": str(document.document_id),
            "content_id": document.content_id,
            "filename": filename,
        },
        doc_dir,
        artifact_type=artifact_type,
        chunks=(
            {
                "extractor": chunks.extractor,
                "chunker": chunks.chunker,
                "count": chunks.chunk_count,
            }
            if chunks
            else None
        ),
    )


@flow(name="ingest-document", log_prints=True, on_crashed=[_ingest_crashed])
def ingest_document_flow(event: dict) -> dict:
    """Commit one candidate as a document, then run it through the release's operators.

    The flow only passes references and dispatches subflows. Where the bytes, the
    extraction, the chunks, and the vectors are kept is the store's business. Every run
    does the work again: what an operator version already produced is reused by its key,
    not skipped here.
    """
    document = commit_upload(UUID(event["document_id"]))
    filename = document_store().describe(document.document_id).filename
    release = active_release()
    record_status(document.document_id, DocumentStatus.INGESTING)

    with tempfile.TemporaryDirectory(prefix="smart-files-") as temp_dir:
        work_dir = Path(temp_dir)
        output_root = work_dir / "output"

        try:
            doc = document_store().download(
                document.document_id, work_dir / safe_filename(filename)
            )
            extraction = find_extraction(document.document_id, release.extractor)
            if extraction is None:
                extraction = store_extraction(
                    document.document_id,
                    release.extractor,
                    extract_document(doc, release.extractor),
                )
            routed = route_document(doc, extraction, release.chunker, output_root)
            doc_dir = output_root / doc.stem
            chunked = routed.chunked

            if chunked is not None:
                stage_bundle(doc, doc_dir, extraction, chunked)
            if not (doc_dir / "status.json").exists():
                raise RuntimeError("no status.json was written")
            outcome = json.loads((doc_dir / "status.json").read_text())

            chunks = chunked.chunks if chunked else None
            completion = publish_ingestion_bundle(
                document,
                event["event_id"],
                filename,
                doc_dir,
                routed.artifact_type,
                chunks,
            )
            completion["release"] = release.name
            completion["extraction_reused"] = extraction.reused
            completion["chunks_reused"] = bool(chunks and chunks.reused)
            record_outcome(document.document_id, outcome, embeds=chunks is not None)
            return completion
        except Exception as error:
            record_status(
                document.document_id, DocumentStatus.FAILED, error_detail(error)
            )
            raise


@task(
    name="process-upload",
    retries=4,
    retry_condition_fn=_retryable,
    log_prints=True,
    **RETRY,
)
def process_upload(value: bytes | dict) -> dict:
    """Ingest one upload, then submit its document's embedding work to Prefect."""
    event = parse_upload_event(value)
    completion = ingest_document_flow(event)
    if completion["status"] == "ok" and completion.get("chunks"):
        future = embed_release.delay(completion["document_id"], event["release"])
        completion["embedding_task_run_id"] = str(future.task_run_id)
    return completion


def ingest_worker() -> None:
    serve(process_upload, limit=1)
