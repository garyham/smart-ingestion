import argparse
import mimetypes
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from postgres_queue import completed_ingestion_ids, enqueue_upload, ensure_schema
from upload_events import S3_BUCKET, s3_client, safe_filename


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


def wait_for_completions(expected: set[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    completed: set[str] = set()
    while completed != expected:
        found = completed_ingestion_ids(expected)
        if found != completed:
            completed = found
            print(f"Completed {len(completed)}/{len(expected)}")
        if time.monotonic() >= deadline:
            missing = len(expected - completed)
            raise TimeoutError(f"timed out with {missing} ingestion(s) unfinished")
        time.sleep(1)


def run(count: int, data_dir: Path, timeout: float) -> None:
    files = sorted(path for path in data_dir.iterdir() if path.is_file())
    if not files:
        raise SystemExit(f"No files found in {data_dir}")

    run_id = str(uuid4())
    object_keys = {
        path: f"uploads/load-test/{run_id}/{safe_filename(path.name)}" for path in files
    }
    client = s3_client()
    try:
        ensure_schema()
        for path, object_key in object_keys.items():
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            client.upload_file(
                str(path),
                S3_BUCKET,
                object_key,
                ExtraArgs={"ContentType": content_type},
            )
        print(f"Uploaded {len(files)} source file(s)")

        expected: set[str] = set()
        for index in range(count):
            path = files[index % len(files)]
            event = make_event(path, object_keys[path])
            event_id = str(event["event_id"])
            enqueue_upload(event)
            expected.add(event_id)
        print(f"Queued {count} notification(s)")
        wait_for_completions(expected, timeout)
    finally:
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
