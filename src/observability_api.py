"""Read-only observability API for the ingestion pipeline.

Exposes document-level state - the queue (`state/queue.db`) and each document's ingestion
outcome under `ingested/` - as JSON. Deliberately has no Celery/Redis dependency of its own: it
reads only the same on-disk state the pipeline already writes, so it keeps working unchanged even
if the orchestration engine changes again later. Read-only by design - no requeue/cancel endpoints.
"""

import json
import sqlite3
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException

_DB_PATH = Path("state/queue.db")
_OUTPUT_ROOT = Path("ingested")

app = FastAPI(title="smart-files observability API")


def _connect_ro() -> sqlite3.Connection:
    """Open the queue DB strictly read-only at the SQLite level - a structural guarantee this
    process can never write to it, reinforcing the read-only scope beyond just omitting endpoints.
    """
    if not _DB_PATH.exists():
        raise HTTPException(status_code=503, detail=f"{_DB_PATH} does not exist yet")
    conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/queue")
def list_queue(status: str | None = None) -> list[dict]:
    with _connect_ro() as conn:
        if status is not None:
            rows = conn.execute(
                "SELECT * FROM queue_items WHERE status = ? ORDER BY id", (status,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM queue_items ORDER BY id").fetchall()
    return [dict(row) for row in rows]


@app.get("/queue/{item_id}")
def get_queue_item(item_id: int) -> dict:
    with _connect_ro() as conn:
        row = conn.execute("SELECT * FROM queue_items WHERE id = ?", (item_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no queue item with id {item_id}")
    return dict(row)


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
