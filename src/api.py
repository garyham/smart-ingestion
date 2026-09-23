from pathlib import Path
from typing import Annotated
from uuid import UUID, uuid4

import httpx
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from prefect.exceptions import PrefectException
from psycopg import Error as PostgresError
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError

from contracts.embeddings import SearchHit
from ingestion.ingestion_flow import InvalidUpload, check_upload, process_upload
from operators import embedders
from operators.embedders import UnknownEmbedder
from releases import Release, active_release
from store.service import (
    DocumentInfo,
    DocumentStore,
    UnknownDocument,
    UploadNotWritten,
    UploadRequest,
)

INDEX_PATH = Path(__file__).with_name("static") / "index.html"
QUERY_PATH = Path(__file__).with_name("static") / "query.html"
LIBRARY_PATH = Path(__file__).with_name("static") / "documents.html"


app = FastAPI(title="Smart Files")


def documents() -> DocumentStore:
    return DocumentStore()


def search(text: str, embedder: str, limit: int) -> list[SearchHit]:
    """Embed the query with one embedder version, then search what that version embedded."""
    operator = embedders.get(embedder)
    return documents().search_embeddings(
        operator.embed_query(text), operator.search_options(limit)
    )


def search_release(release: Release, text: str, limit: int) -> list[SearchHit]:
    """Search every embedder the release names.

    Each embedder ranks and fuses over its own embeddings, so versions never share a
    ranking. Where a release names more than one embedder, the merged list keeps each
    chunk once, at its best score, rather than once per embedder that indexed it.
    """
    hits = [
        hit for embedder in release.embedders for hit in search(text, embedder, limit)
    ]
    hits.sort(key=lambda hit: hit.score, reverse=True)
    best: dict[UUID, SearchHit] = {}
    for hit in hits:
        best.setdefault(hit.chunk.chunk_id, hit)
    return list(best.values())[:limit]


class FileDetails(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=255)
    size: int = Field(gt=0)


class NotifyRequest(BaseModel):
    document_id: UUID


class QueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=3, ge=1, le=10)
    # An embedder version, written `hybrid@1`.
    embedder: str | None = Field(
        default=None, pattern=r"^[a-z0-9][a-z0-9_-]*@[A-Za-z0-9][A-Za-z0-9._-]*$"
    )


@app.get("/", response_class=FileResponse)
def index() -> Path:
    return INDEX_PATH


@app.get("/query", response_class=FileResponse)
def query_page() -> Path:
    return QUERY_PATH


@app.get("/library", response_class=FileResponse)
def library_page() -> Path:
    return LIBRARY_PATH


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/presign")
def presign(file: FileDetails) -> dict[str, str | dict[str, str]]:
    """Hand out a candidate document ID and a URL to write its bytes to, once.

    Nothing is recorded. The candidate only becomes a document once the ingestion flow has
    hashed its bytes, and bytes that are already a document resolve to that one instead.
    """
    try:
        upload = documents().create_upload(
            UploadRequest(
                filename=file.filename,
                content_type=file.content_type,
                size=file.size,
            )
        )
    except (BotoCoreError, ClientError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not create the upload URL",
        ) from error

    return {
        "document_id": str(upload.document_id),
        "upload_url": upload.upload_url,
        "upload_headers": upload.upload_headers,
        "expires_at": upload.expires_at.isoformat(),
    }


@app.get("/documents", response_model=list[DocumentInfo])
def list_documents(
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[DocumentInfo]:
    """Every document, newest first. Identity and file details only - never a location."""
    try:
        return documents().list_documents(limit=limit, offset=offset)
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not list the documents",
        ) from error


@app.get("/documents/lookup")
def lookup_document(
    sha256: Annotated[
        str, Query(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    ],
) -> dict[str, str | bool]:
    """Answer whether bytes with this SHA-256 are already a document.

    A client that hashes its own file can find out it has nothing to upload before it
    sends anything, which is the one question it can ask without the bytes travelling.

    The hash is unverified - holding it is not holding the bytes - so it decides nothing
    beyond this answer. An upload still hashes what actually arrives.
    """
    try:
        document = documents().lookup(sha256)
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not look up the document",
        ) from error

    if document is None:
        return {"known": False}
    return {
        "known": True,
        "document_id": str(document.document_id),
        "content_id": document.content_id,
    }


@app.post("/notify", status_code=status.HTTP_202_ACCEPTED)
def notify(request: NotifyRequest) -> dict[str, str]:
    """Check that something arrived, then queue the pipeline.

    Only the cheap checks happen here, without reading the bytes. Hashing them - and so
    deciding which document they are - is the ingestion flow's first task. A candidate
    that is already a document is queued again without them.
    """
    store = documents()
    try:
        try:
            store.document_ref(request.document_id)
        except UnknownDocument:
            check_upload(store.describe_upload(request.document_id))
    except UploadNotWritten as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Nothing was uploaded"
        ) from error
    except InvalidUpload as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)
        ) from error
    except (BotoCoreError, ClientError, SQLAlchemyError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not read the upload",
        ) from error

    event_id = str(uuid4())
    event = {
        "event_id": event_id,
        "event": "upload.notified",
        "schema_version": 4,
        "document_id": str(request.document_id),
        "release": active_release().name,
    }

    try:
        future = process_upload.delay(event)
    except (httpx.HTTPError, PrefectException, OSError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not queue the upload",
        ) from error

    return {
        "status": "notified",
        "event_id": event_id,
        "document_id": str(request.document_id),
        "task_run_id": str(future.task_run_id),
    }


@app.delete("/documents/{document_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_document(document_id: UUID) -> None:
    """Delete a document, everything derived from it, and every object it owns."""
    try:
        documents().delete_document(document_id)
    except UnknownDocument as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Unknown document"
        ) from error
    except (BotoCoreError, ClientError, SQLAlchemyError, RuntimeError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not delete the document",
        ) from error


@app.post("/query")
def query_chunks(request: QueryRequest) -> dict[str, list[SearchHit]]:
    """Search one named embedder version, or the active release's embedders.

    Never every embedding there is: without an explicit embedder, the active release
    decides which embedders are in scope.
    """
    try:
        if request.embedder is not None:
            results = search(request.query, request.embedder, request.limit)
        else:
            results = search_release(active_release(), request.query, request.limit)
    except UnknownEmbedder as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Unknown embedder"
        ) from error
    except (PostgresError, SQLAlchemyError, OSError, RuntimeError, ValueError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not search the indexed chunks",
        ) from error
    return {"results": results}


def main() -> None:
    uvicorn.run("api:app", host="127.0.0.1", port=8000)
