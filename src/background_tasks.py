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
from ingestion.publish import publish_bundle
from ingestion.routing import route_document
from ingestion.storage import (
    claim_ingestion,
    sha256_file,
    update_ingestion,
)
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
        source_event = {**event, "source_sha256": sha256_file(doc)}
        owned, ingestion = claim_ingestion(
            event["event_id"],
            event["document_id"],
            source_event["source_sha256"],
            source_event,
        )
        if not owned:
            return {
                **ingestion["outputs"],
                "event": "ingestion.duplicate",
                "status": ingestion["status"],
                "canonical_ingestion_id": str(ingestion["ingestion_id"]),
                "source_sha256": source_event["source_sha256"],
                "duplicate": True,
            }
        if ingestion["status"] in {"published", "embedding", "completed"}:
            return {
                **ingestion["outputs"],
                "ingestion_state": ingestion["status"],
                "duplicate": False,
            }

        try:
            route_document(doc, output_root)
            doc_dir = output_root / doc.stem
            if not (doc_dir / "status.json").exists():
                raise RuntimeError("no status.json was written")
            completion = publish_bundle(
                client,
                S3_BUCKET,
                ARTIFACT_PREFIX,
                event["document_id"],
                event["event_id"],
                source_event,
                doc_dir,
            )
            update_ingestion(
                event["event_id"], "published", "publish", outputs=completion
            )
            return {**completion, "ingestion_state": "published", "duplicate": False}
        except Exception as error:
            update_ingestion(
                event["event_id"],
                "failed",
                "ingestion",
                error={"type": type(error).__name__, "detail": str(error)},
            )
            raise


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
    if completion.get("duplicate"):
        return completion
    if completion.get("ingestion_state") == "completed":
        return completion
    if completion["status"] == "ok" and completion["artifact_type"] == "chunks":
        dense_model, sparse_model, device = configured_models()
        future = embed_document.delay(completion, dense_model, sparse_model, device)
        completion["embedding_task_run_id"] = str(future.task_run_id)
        update_ingestion(
            completion["ingestion_id"],
            "embedding",
            "embedding",
            outputs={"embedding_task_run_id": str(future.task_run_id)},
        )
    else:
        update_ingestion(
            completion["ingestion_id"], "completed", "publish", outputs=completion
        )
    return completion


@task(
    name="embed-document",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.2,
    log_prints=True,
)
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
    ingestion_id = completion_event["ingestion_id"]
    update_ingestion(ingestion_id, "embedding", "embedding")
    try:
        embed_ingestion(completion_event, dense_model_name, sparse_model_name, device)
    except Exception as error:
        update_ingestion(
            ingestion_id,
            "failed",
            "embedding",
            error={"type": type(error).__name__, "detail": str(error)},
        )
        raise
    update_ingestion(
        ingestion_id,
        "completed",
        "embedding",
        outputs={
            "dense_model": dense_model_name,
            "sparse_model": sparse_model_name,
        },
    )


def ingest_worker() -> None:
    serve(process_upload, limit=1)


def embedding_worker() -> None:
    serve(embed_document, limit=1)
