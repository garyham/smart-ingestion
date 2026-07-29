import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    # WAL mode lets readers (e.g. a poll cycle's list_pending) avoid blocking behind an
    # in-flight writer's lock - needed now that multiple worker processes can touch this
    # DB concurrently, rather than a single flow run's main thread as before.
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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


def enqueue(db_path: Path, uris: list[str]) -> None:
    """Record each URI as pending, regardless of whether it's been seen before.

    A URI that's currently `pending`/`processing` is left alone, so a document already
    in flight isn't queued a second time. Any other prior status (`success`/`failed`) is
    reset to `pending`, so dropping the same file back into queue/ re-ingests it.
    """
    now = _now()
    with _connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO queue_items (uri, status, enqueued_at)
            VALUES (?, 'pending', ?)
            ON CONFLICT(uri) DO UPDATE SET
                status = 'pending',
                enqueued_at = excluded.enqueued_at,
                started_at = NULL,
                finished_at = NULL,
                error = NULL
            WHERE status NOT IN ('pending', 'processing')
            """,
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
