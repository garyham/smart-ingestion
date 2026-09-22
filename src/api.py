from datetime import UTC, datetime
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

from background_tasks import process_upload
from embeddings.flow import (
    available_embedding_providers,
    configured_models,
    generate_query_embeddings,
)
from embeddings.storage import hybrid_search
from ingestion.schemas import IngestionRead, IngestionStatus
from ingestion.storage import get_ingestion, list_ingestions
from upload_events import S3_BUCKET, s3_client, safe_filename

PRESIGN_TTL_SECONDS = 15 * 60
INDEX_PATH = Path(__file__).with_name("static") / "index.html"
QUERY_PATH = Path(__file__).with_name("static") / "query.html"


app = FastAPI(title="Smart Files")


class FileDetails(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)


class NotifyRequest(FileDetails):
    object_key: str = Field(min_length=1, max_length=1024)
    document_id: UUID


class QueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=3, ge=1, le=3)


@app.get("/", response_class=FileResponse)
def index() -> Path:
    return INDEX_PATH


@app.get("/query", response_class=FileResponse)
def query_page() -> Path:
    return QUERY_PATH


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/presign")
def presign(file: FileDetails) -> dict[str, str | int]:
    document_id = str(uuid4())
    object_key = f"uploads/{document_id}/{safe_filename(file.filename)}"
    try:
        upload_url = s3_client().generate_presigned_url(
            "put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": object_key,
                "ContentType": file.content_type,
            },
            ExpiresIn=PRESIGN_TTL_SECONDS,
        )
    except (BotoCoreError, ClientError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not create the upload URL",
        ) from error

    return {
        "upload_url": upload_url,
        "object_key": object_key,
        "document_id": document_id,
        "expires_in": PRESIGN_TTL_SECONDS,
    }


@app.post("/notify", status_code=status.HTTP_202_ACCEPTED)
def notify(file: NotifyRequest) -> dict[str, str]:
    if not file.object_key.startswith(f"uploads/{file.document_id}/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="document_id does not match object_key",
        )
    event_id = str(uuid4())
    event: dict[str, str | int] = {
        "event_id": event_id,
        "event": "file.uploaded",
        "schema_version": 1,
        "document_id": str(file.document_id),
        "bucket": S3_BUCKET,
        "object_key": file.object_key,
        "filename": file.filename,
        "content_type": file.content_type,
        "size": file.size,
        "uploaded_at": datetime.now(UTC).isoformat(),
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
        "task_run_id": str(future.task_run_id),
    }


@app.post("/query")
def query_chunks(request: QueryRequest) -> dict[str, list[dict]]:
    dense_model, sparse_model, device = configured_models()
    try:
        providers = available_embedding_providers(device)
        dense, sparse = generate_query_embeddings(
            request.query, dense_model, sparse_model, providers
        )
        results = hybrid_search(dense, sparse, dense_model, sparse_model, request.limit)
    except (PostgresError, OSError, RuntimeError, ValueError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not search the indexed chunks",
        ) from error
    return {"results": results}


@app.get("/ingestions", response_model=list[IngestionRead])
def query_ingestions(
    status_filter: Annotated[IngestionStatus | None, Query(alias="status")] = None,
    source_sha256: Annotated[
        str | None,
        Query(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"),
    ] = None,
    document_id: UUID | None = None,
    pipeline_version: Annotated[str | None, Query(min_length=1)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[IngestionRead]:
    try:
        return list_ingestions(
            status=status_filter,
            source_sha256=source_sha256,
            document_id=document_id,
            pipeline_version=pipeline_version,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not read ingestions",
        ) from error


@app.get("/ingestions/{ingestion_id}", response_model=IngestionRead)
def read_ingestion(ingestion_id: UUID) -> IngestionRead:
    try:
        ingestion = get_ingestion(ingestion_id)
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not read the ingestion",
        ) from error
    if ingestion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ingestion not found",
        )
    return ingestion


def main() -> None:
    uvicorn.run("api:app", host="127.0.0.1", port=8000)
