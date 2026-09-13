import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pika
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from upload_events import (
    INGEST_QUEUE,
    RABBITMQ_URL,
    S3_BUCKET,
    UPLOAD_EXCHANGE,
    s3_client,
    safe_filename,
)

PRESIGN_TTL_SECONDS = 15 * 60
INDEX_PATH = Path(__file__).with_name("static") / "index.html"

app = FastAPI(title="Smart Files")


class FileDetails(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)


class NotifyRequest(FileDetails):
    object_key: str = Field(min_length=1, max_length=1024)
    document_id: UUID


def publish_upload(event: dict[str, str | int]) -> None:
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=UPLOAD_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.queue_declare(queue=INGEST_QUEUE, durable=True)
        channel.queue_bind(queue=INGEST_QUEUE, exchange=UPLOAD_EXCHANGE)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=UPLOAD_EXCHANGE,
            routing_key="",
            body=json.dumps(event).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=str(event["event_id"]),
                type=str(event["event"]),
            ),
            mandatory=True,
        )
    finally:
        connection.close()


@app.get("/", response_class=FileResponse)
def index() -> Path:
    return INDEX_PATH


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
        publish_upload(event)
    except (pika.exceptions.AMQPError, OSError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not publish the upload notification",
        ) from error

    return {"status": "notified", "event_id": event_id}


def main() -> None:
    uvicorn.run("api:app", host="127.0.0.1", port=8000)
