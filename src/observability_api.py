"""Read-only observability API for the ingestion pipeline.

Exposes document-level state - the queue (`queue/`) and each document's ingestion outcome under
`ingested/` - as JSON. Deliberately has no Celery/Redis dependency of its own: it reads only the
same on-disk state the pipeline already writes, so it keeps working unchanged even if the
orchestration engine changes again later. Read-only by design - no requeue/cancel endpoints.
"""

import json
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException

_QUEUE_DIR = Path("queue")
_PROCESSING_DIR = _QUEUE_DIR / ".processing"
_OUTPUT_ROOT = Path("ingested")

app = FastAPI(title="smart-files observability API")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/queue")
def list_queue() -> list[dict]:
    """Queue state is just what's on disk: a file under queue/ is pending, a file under
    queue/.processing/ has been claimed and dispatched.
    """
    pending = [
        {"name": p.name, "status": "pending"} for p in sorted(_QUEUE_DIR.iterdir()) if p.is_file()
    ] if _QUEUE_DIR.exists() else []
    processing = [
        {"name": p.name, "status": "processing"}
        for p in sorted(_PROCESSING_DIR.iterdir())
        if p.is_file()
    ] if _PROCESSING_DIR.exists() else []
    return pending + processing


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _document_summary(doc_dir: Path) -> dict:
    status = _read_json(doc_dir / "status.json")
    return {
        "name": doc_dir.name,
        "status": status.get("status") if status else "in_progress",
    }


@app.get("/documents")
def list_documents() -> list[dict]:
    if not _OUTPUT_ROOT.exists():
        return []
    return [_document_summary(p) for p in sorted(_OUTPUT_ROOT.iterdir()) if p.is_dir()]


@app.get("/documents/{doc_name}")
def get_document(doc_name: str) -> dict:
    doc_dir = _OUTPUT_ROOT / doc_name
    if not doc_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"no ingested document named {doc_name!r}")

    assets_dir = doc_dir / "assets"
    duckdb_files = sorted(p.name for p in doc_dir.glob("*.duckdb"))
    return {
        "name": doc_name,
        "status": _read_json(doc_dir / "status.json"),
        "metadata": _read_json(doc_dir / "metadata"),
        "has_chunks": (doc_dir / "chunks.jsonl").exists(),
        "duckdb_files": duckdb_files,
        "assets": sorted(p.name for p in assets_dir.iterdir()) if assets_dir.is_dir() else [],
    }


def main() -> None:
    uvicorn.run("observability_api:app", host="127.0.0.1", port=8100)


if __name__ == "__main__":
    main()
