"""The immutable ingestion bundle.

This module owns the `ingested/` object layout: it is the only place those keys are built,
so neither the API nor the orchestrator constructs a bucket name or an object key.
"""

import hashlib
import json
import mimetypes
import os
from datetime import UTC, datetime
from pathlib import Path

from upload_events import ARTIFACT_PREFIX, S3_BUCKET, s3_client

# Recorded in each manifest for provenance only; it is never part of a reuse key.
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "1")
SCHEMA_VERSION = 1
_CONTENT_TYPES = {
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".duckdb": "application/vnd.duckdb",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def publish_bundle(
    document_id: str,
    ingestion_id: str,
    source: dict,
    doc_dir: Path,
    *,
    artifact_type: str,
    chunks: dict | None = None,
    client=None,
    bucket: str = S3_BUCKET,
    prefix: str = ARTIFACT_PREFIX,
) -> dict:
    """Upload all artifacts, then upload the manifest as the completion marker."""
    client = client or s3_client()
    bundle_prefix = f"{prefix}/{document_id}/{ingestion_id}"
    status = json.loads((doc_dir / "status.json").read_text())
    artifacts = []

    for path in sorted(item for item in doc_dir.rglob("*") if item.is_file()):
        relative_path = path.relative_to(doc_dir).as_posix()
        key = f"{bundle_prefix}/{relative_path}"
        content_type = _CONTENT_TYPES.get(path.suffix)
        content_type = content_type or mimetypes.guess_type(path.name)[0]
        content_type = content_type or "application/octet-stream"
        client.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )
        artifacts.append(
            {
                "name": relative_path,
                "uri": f"s3://{bucket}/{key}",
                "content_type": content_type,
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "ingestion_id": ingestion_id,
        "pipeline_version": PIPELINE_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "status": status,
        "artifact_type": artifact_type,
        # The source is named by reference. Where its bytes live is the document store's
        # business, not the bundle's.
        "source": source,
        # Chunks live in the store, named here by the chunker that cut them.
        "chunks": chunks,
        "artifacts": artifacts,
    }
    manifest_key = f"{bundle_prefix}/manifest.json"
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    return {
        "event": "ingestion.completed",
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "ingestion_id": ingestion_id,
        "pipeline_version": PIPELINE_VERSION,
        "status": status["status"],
        "artifact_type": artifact_type,
        "manifest_uri": f"s3://{bucket}/{manifest_key}",
        "chunks": chunks,
        "source_content_id": source.get("content_id"),
        "completed_at": manifest["created_at"],
    }
