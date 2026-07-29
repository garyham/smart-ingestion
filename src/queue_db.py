import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path, timeout=30)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def init_db(db_path: Path) -> None:
    """Create the queue_items table if it doesn't already exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_items (
                id INTEGER PRIMARY KEY,
                uri TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                enqueued_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                error TEXT
            )
            """
        )


def enqueue_new(db_path: Path, uris: list[str]) -> None:
    """Insert any URIs not already present in the queue, as pending."""
    now = _now()
    with _connect(db_path) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO queue_items (uri, status, enqueued_at) VALUES (?, 'pending', ?)",
            [(uri, now) for uri in uris],
        )


def list_pending(db_path: Path) -> list[tuple[int, str]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, uri FROM queue_items WHERE status = 'pending' ORDER BY id"
        ).fetchall()
    return list(rows)


def mark_processing(db_path: Path, item_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE queue_items SET status = 'processing', started_at = ? WHERE id = ?",
            (_now(), item_id),
        )


def mark_success(db_path: Path, item_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE queue_items SET status = 'success', finished_at = ? WHERE id = ?",
            (_now(), item_id),
        )


def mark_failed(db_path: Path, item_id: int, error: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE queue_items SET status = 'failed', finished_at = ?, error = ? WHERE id = ?",
            (_now(), error, item_id),
        )
