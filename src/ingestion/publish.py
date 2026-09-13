import hashlib
import json
import mimetypes
from datetime import UTC, datetime
from pathlib import Path


SCHEMA_VERSION = 1
_CONTENT_TYPES = {
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".duckdb": "application/vnd.duckdb",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def publish_bundle(
    client,
    bucket: str,
    prefix: str,
    document_id: str,
    ingestion_id: str,
    source_event: dict,
    doc_dir: Path,
) -> dict:
    """Upload all artifacts, then upload the manifest as the completion marker."""
    bundle_prefix = f"{prefix}/{document_id}/{ingestion_id}"
    status = json.loads((doc_dir / "status.json").read_text())
    if next(doc_dir.glob("*.duckdb"), None):
        artifact_type = "duckdb"
    elif (doc_dir / "chunks.jsonl").exists():
        artifact_type = "chunks"
    else:
        artifact_type = "diagnostic"
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
                "sha256": _sha256(path),
            }
        )

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "ingestion_id": ingestion_id,
        "created_at": datetime.now(UTC).isoformat(),
        "status": status,
        "artifact_type": artifact_type,
        "source": {
            "bucket": source_event["bucket"],
            "object_key": source_event["object_key"],
            "filename": source_event["filename"],
        },
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
        "status": status["status"],
        "artifact_type": artifact_type,
        "manifest_uri": f"s3://{bucket}/{manifest_key}",
        "completed_at": manifest["created_at"],
    }
