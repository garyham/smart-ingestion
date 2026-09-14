import json
import os
import socket
import tempfile
import time
from pathlib import Path
from threading import Event, Thread
from urllib.error import URLError
from urllib.request import urlopen
from uuid import UUID, uuid4

# Prefect reads this setting when it is imported.
_DEFAULT_PREFECT_API_URL = "http://127.0.0.1:4200/api"
os.environ.setdefault("PREFECT_API_URL", _DEFAULT_PREFECT_API_URL)

from prefect import flow
from psycopg import Error as PostgresError

from ingestion.config import load_config
from ingestion.detect import identify_mime_type
from ingestion.publish import publish_bundle
from ingestion.routing import ensure_concurrency_limits, route_document
from postgres_queue import (
    claim_upload,
    complete_upload,
    ensure_schema,
    extend_upload_lease,
    fail_upload,
)
from upload_events import ARTIFACT_PREFIX, S3_BUCKET, s3_client, safe_filename

_config = load_config()
_allowed_mime_types = set(_config["mime_types"])
_concurrency_limits = _config.get("concurrency_limits", {})
LEASE_SECONDS = int(os.getenv("QUEUE_LEASE_SECONDS", "300"))
MAX_ATTEMPTS = int(os.getenv("QUEUE_MAX_ATTEMPTS", "5"))
POLL_SECONDS = float(os.getenv("QUEUE_POLL_SECONDS", "1"))


class InvalidUploadEvent(ValueError):
    pass


def parse_upload_event(value: bytes | dict) -> dict:
    if isinstance(value, bytes):
        try:
            event = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidUploadEvent("message body is not valid JSON") from exc
    else:
        event = value

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
def ingest_upload(event: dict) -> dict:
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
        return publish_bundle(
            client,
            S3_BUCKET,
            ARTIFACT_PREFIX,
            event["document_id"],
            event["event_id"],
            event,
            doc_dir,
        )


class LeaseHeartbeat:
    def __init__(self, job_id: UUID, worker_id: str):
        self.job_id = job_id
        self.worker_id = worker_id
        self.stop = Event()
        self.thread = Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.wait(max(1, LEASE_SECONDS // 3)):
            try:
                if not extend_upload_lease(self.job_id, self.worker_id, LEASE_SECONDS):
                    return
            except PostgresError as exc:
                print(f"Could not extend lease for {self.job_id}: {exc}")

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, _type, _value, _traceback):
        self.stop.set()
        self.thread.join()


def process_job(job: dict, worker_id: str) -> None:
    job_id = job["id"]
    try:
        event = parse_upload_event(job["event"])
    except InvalidUploadEvent as exc:
        print(f"Rejecting invalid upload event: {exc}")
        fail_upload(job_id, worker_id, str(exc), MAX_ATTEMPTS, MAX_ATTEMPTS)
        return

    try:
        with LeaseHeartbeat(job_id, worker_id):
            completion = ingest_upload(event)
        if not complete_upload(job_id, worker_id, completion):
            print(f"Lost lease before completing {event['object_key']}")
    except Exception as exc:  # noqa: BLE001 - failed work must remain queued
        print(f"Ingestion failed for {event['object_key']}: {exc}")
        fail_upload(job_id, worker_id, str(exc), job["attempts"], MAX_ATTEMPTS)


def consume_uploads() -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"
    print("Waiting for uploads in PostgreSQL")
    while True:
        job = claim_upload(worker_id, LEASE_SECONDS)
        if job is None:
            time.sleep(POLL_SECONDS)
            continue
        process_job(job, worker_id)


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
    ensure_schema()
    ensure_concurrency_limits(_concurrency_limits)
    while True:
        try:
            consume_uploads()
        except (PostgresError, OSError) as exc:
            print(f"PostgreSQL connection failed: {exc}; retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    main()
