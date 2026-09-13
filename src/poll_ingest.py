import json
import os
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from uuid import UUID

# Prefect reads this setting when it is imported.
_DEFAULT_PREFECT_API_URL = "http://127.0.0.1:4200/api"
os.environ.setdefault("PREFECT_API_URL", _DEFAULT_PREFECT_API_URL)

import pika
from prefect import flow

from ingestion.config import load_config
from ingestion.detect import identify_mime_type
from ingestion.publish import publish_bundle
from ingestion.routing import ensure_concurrency_limits, route_document
from upload_events import (
    ARTIFACT_PREFIX,
    COMPLETION_EXCHANGE,
    INGEST_QUEUE,
    PROCESS_QUEUE,
    RABBITMQ_URL,
    S3_BUCKET,
    UPLOAD_EXCHANGE,
    s3_client,
    safe_filename,
)

_config = load_config()
_allowed_mime_types = set(_config["mime_types"])
_concurrency_limits = _config.get("concurrency_limits", {})


class InvalidUploadEvent(ValueError):
    pass


def publish_completion(event: dict) -> None:
    """Publish a persistent completion event and wait for broker confirmation."""
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=COMPLETION_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
        channel.queue_bind(queue=PROCESS_QUEUE, exchange=COMPLETION_EXCHANGE)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=COMPLETION_EXCHANGE,
            routing_key="",
            body=json.dumps(event).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event["ingestion_id"],
                type=event["event"],
            ),
            mandatory=True,
        )
    finally:
        connection.close()


def parse_upload_event(body: bytes) -> dict:
    try:
        event = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidUploadEvent("message body is not valid JSON") from exc

    if not isinstance(event, dict) or event.get("event") != "file.uploaded":
        raise InvalidUploadEvent("message is not a file.uploaded event")
    if event.get("schema_version") != 1:
        raise InvalidUploadEvent("message has no supported schema_version")
    for field in ("event_id", "document_id", "bucket", "object_key", "filename"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise InvalidUploadEvent(f"message has no valid {field}")
    for field in ("event_id", "document_id"):
        try:
            UUID(event[field])
        except ValueError as exc:
            raise InvalidUploadEvent(f"message has no valid {field}") from exc
    return event


@flow(name="ingest-upload")
def ingest_upload(event: dict) -> None:
    filename = safe_filename(event["filename"])
    with tempfile.TemporaryDirectory(prefix="smart-files-") as temp_dir:
        work_dir = Path(temp_dir)
        doc = work_dir / filename
        output_root = work_dir / "output"
        client = s3_client()
        client.download_file(event["bucket"], event["object_key"], str(doc))
        detected = identify_mime_type(doc)
        route_document(doc, detected, output_root, _allowed_mime_types)
        doc_dir = output_root / doc.stem
        if not (doc_dir / "status.json").exists():
            raise RuntimeError("no status.json was written")
        completion = publish_bundle(
            client,
            S3_BUCKET,
            ARTIFACT_PREFIX,
            event["document_id"],
            event["event_id"],
            event,
            doc_dir,
        )
        publish_completion(completion)


def handle_message(channel, method, _properties, body: bytes) -> None:
    try:
        event = parse_upload_event(body)
    except InvalidUploadEvent as exc:
        print(f"Rejecting invalid upload event: {exc}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        ingest_upload(event)
    except Exception as exc:  # noqa: BLE001 - failed work must remain queued
        print(f"Ingestion failed for {event['object_key']}: {exc}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)


def consume_uploads() -> None:
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
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=INGEST_QUEUE,
            on_message_callback=handle_message,
            auto_ack=False,
        )
        print(f"Waiting for uploads on {UPLOAD_EXCHANGE} ({INGEST_QUEUE})")
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


def _require_prefect_server() -> None:
    api_url = os.environ["PREFECT_API_URL"]
    try:
        with urlopen(f"{api_url}/health", timeout=5) as resp:
            if resp.status != 200:
                raise URLError(f"unexpected status {resp.status}")
    except URLError as exc:
        raise SystemExit(
            f"Cannot reach Prefect server at {api_url} ({exc}). "
            "Make sure the Prefect service is running before ingestion."
        ) from exc


def main() -> None:
    _require_prefect_server()
    ensure_concurrency_limits(_concurrency_limits)
    while True:
        try:
            consume_uploads()
        except (pika.exceptions.AMQPError, OSError) as exc:
            print(f"RabbitMQ connection failed: {exc}; retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    main()
