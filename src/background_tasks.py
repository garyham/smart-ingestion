import json
import os
import tempfile
from pathlib import Path
from uuid import UUID

# Prefect reads this setting when it is imported.
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

from prefect import flow, task
from prefect.task_worker import serve
from prefect.tasks import exponential_backoff

from embeddings.flow import configured_models, embed_ingestion
from ingestion.config import load_config
from ingestion.publish import publish_bundle
from ingestion.routing import ensure_concurrency_limits, route_document
from upload_events import ARTIFACT_PREFIX, S3_BUCKET, s3_client, safe_filename


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


@flow(name="ingest-upload", log_prints=True)
def ingest_upload_flow(event: dict) -> dict:
    filename = safe_filename(event["filename"])
    with tempfile.TemporaryDirectory(prefix="smart-files-") as temp_dir:
        work_dir = Path(temp_dir)
        doc = work_dir / filename
        output_root = work_dir / "output"
        client = s3_client()
        client.download_file(event["bucket"], event["object_key"], str(doc))
        route_document(doc, output_root)
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


def _retry_valid_upload(_task, _task_run, state) -> bool:
    failure = state.result(raise_on_failure=False)
    return not isinstance(failure, InvalidUploadEvent)


@task(
    name="process-upload",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.2,
    retry_condition_fn=_retry_valid_upload,
    log_prints=True,
)
def process_upload(value: bytes | dict) -> dict:
    """Ingest one upload, then submit its embedding work to Prefect."""
    event = parse_upload_event(value)
    completion = ingest_upload_flow(event)
    if completion["status"] == "ok" and completion["artifact_type"] == "chunks":
        dense_model, sparse_model, device = configured_models()
        future = embed_document.delay(completion, dense_model, sparse_model, device)
        completion["embedding_task_run_id"] = str(future.task_run_id)
    return completion


@task(name="embed-document", log_prints=True)
def embed_document(
    completion_event: dict,
    dense_model_name: str,
    sparse_model_name: str,
    device: str,
) -> None:
    """Generate and store embeddings for one completed chunk bundle."""
    if completion_event.get("status") != "ok":
        return
    if completion_event.get("artifact_type") != "chunks":
        return
    embed_ingestion(completion_event, dense_model_name, sparse_model_name, device)


def ingest_worker() -> None:
    config = load_config()
    ensure_concurrency_limits(config.get("concurrency_limits", {}))
    serve(process_upload, limit=1)


def embedding_worker() -> None:
    serve(embed_document, limit=1)
