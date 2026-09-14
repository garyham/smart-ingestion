import argparse
import json
import mimetypes
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pika

from upload_events import (
    COMPLETION_EXCHANGE,
    INGEST_QUEUE,
    RABBITMQ_URL,
    S3_BUCKET,
    UPLOAD_EXCHANGE,
    s3_client,
    safe_filename,
)


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def make_event(path: Path, object_key: str) -> dict[str, str | int]:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return {
        "event_id": str(uuid4()),
        "event": "file.uploaded",
        "schema_version": 1,
        "document_id": str(uuid4()),
        "bucket": S3_BUCKET,
        "object_key": object_key,
        "filename": path.name,
        "content_type": content_type,
        "size": path.stat().st_size,
        "uploaded_at": datetime.now(UTC).isoformat(),
    }


def wait_for_completions(channel, queue: str, expected: set[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    completed: set[str] = set()
    while completed != expected:
        method, _properties, body = channel.basic_get(queue=queue, auto_ack=True)
        if method:
            event = json.loads(body)
            ingestion_id = event.get("ingestion_id")
            if ingestion_id in expected:
                completed.add(ingestion_id)
                if len(completed) % 10 == 0 or completed == expected:
                    print(f"Completed {len(completed)}/{len(expected)}")
            continue
        if time.monotonic() >= deadline:
            missing = len(expected - completed)
            raise TimeoutError(f"timed out with {missing} ingestion(s) unfinished")
        channel.connection.process_data_events(time_limit=1)


def run(count: int, data_dir: Path, timeout: float) -> None:
    files = sorted(path for path in data_dir.iterdir() if path.is_file())
    if not files:
        raise SystemExit(f"No files found in {data_dir}")

    run_id = str(uuid4())
    object_keys = {
        path: f"uploads/load-test/{run_id}/{safe_filename(path.name)}" for path in files
    }
    client = s3_client()
    connection = None
    try:
        for path, object_key in object_keys.items():
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            client.upload_file(
                str(path),
                S3_BUCKET,
                object_key,
                ExtraArgs={"ContentType": content_type},
            )
        print(f"Uploaded {len(files)} source file(s)")

        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.exchange_declare(
            exchange=UPLOAD_EXCHANGE, exchange_type="fanout", durable=True
        )
        channel.queue_declare(queue=INGEST_QUEUE, durable=True)
        channel.queue_bind(queue=INGEST_QUEUE, exchange=UPLOAD_EXCHANGE)
        channel.exchange_declare(
            exchange=COMPLETION_EXCHANGE, exchange_type="fanout", durable=True
        )
        result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        completion_queue = result.method.queue
        channel.queue_bind(queue=completion_queue, exchange=COMPLETION_EXCHANGE)
        channel.confirm_delivery()

        expected: set[str] = set()
        for index in range(count):
            path = files[index % len(files)]
            event = make_event(path, object_keys[path])
            event_id = str(event["event_id"])
            channel.basic_publish(
                exchange=UPLOAD_EXCHANGE,
                routing_key="",
                body=json.dumps(event).encode(),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=event_id,
                    type="file.uploaded",
                ),
                mandatory=True,
            )
            expected.add(event_id)
        print(f"Published {count} notification(s)")
        wait_for_completions(channel, completion_queue, expected, timeout)
    finally:
        if connection is not None and connection.is_open:
            connection.close()
        client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": [{"Key": key} for key in object_keys.values()]},
        )
        print(f"Removed {len(object_keys)} source file(s) from SeaweedFS")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress test file ingestion")
    parser.add_argument("--count", type=positive_int, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--timeout", type=positive_int, default=1800, help="seconds")
    args = parser.parse_args()
    run(args.count, args.data_dir, args.timeout)


if __name__ == "__main__":
    main()
