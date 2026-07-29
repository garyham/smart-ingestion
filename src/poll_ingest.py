import json
import os
from pathlib import Path
from urllib.error import URLError
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

from prefect import flow, task

import queue_db
from ingestion.config import load_config
from ingestion.detect import identify_mime_type
from ingestion.routing import route_document

_config = load_config()
_allowed_mime_types = set(_config["mime_types"])


def _to_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _from_file_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"unsupported queue URI scheme: {uri!r} (only file:// is supported so far)")
    return Path(unquote(parsed.path))


def _read_outcome(output_root: Path, doc: Path) -> tuple[bool, str | None]:
    """Read back status.json for a document; returns (succeeded, error_detail)."""
    status_path = output_root / doc.stem / "status.json"
    if not status_path.exists():
        return False, "no status.json was written"
    status = json.loads(status_path.read_text())
    if status.get("status") == "ok":
        return True, None
    reason = status.get("reason", status.get("status"))
    detail = status.get("detail")
    return False, f"{reason}: {detail}" if detail else str(reason)


@task
def scan_queue_dir(queue_dir: Path) -> list[str]:
    """List files in `queue_dir` and return them as file:// URIs."""
    queue_dir.mkdir(parents=True, exist_ok=True)
    return [_to_file_uri(p) for p in sorted(queue_dir.iterdir()) if p.is_file()]


@task
def process_item(uri: str, output_root: Path) -> tuple[bool, str | None]:
    """Route a single queued document through ingestion; returns (succeeded, error_detail)."""
    try:
        doc = _from_file_uri(uri)
        detected = identify_mime_type(doc)
        route_document(doc, detected, output_root, _allowed_mime_types)
    except Exception as exc:  # noqa: BLE001 - must not strand the queue row in 'processing'
        return False, str(exc)
    return _read_outcome(output_root, doc)


@task
def remove_from_queue_dir(uri: str) -> None:
    """Delete a queued file once it's been processed (ok/needs_intervention/failed all count -
    the raw document is preserved under ingested/<doc>/assets/ regardless of outcome).
    """
    _from_file_uri(uri).unlink(missing_ok=True)


@flow(name="poll-and-ingest")
def poll_and_ingest_flow(
    queue_dir: Path = Path("queue"),
    output_root: Path = Path("ingested"),
    db_path: Path = Path("state/queue.db"),
) -> None:
    queue_db.init_db(db_path)

    uris = scan_queue_dir(queue_dir)
    queue_db.enqueue_new(db_path, uris)

    for item_id, uri in queue_db.list_pending(db_path):
        queue_db.mark_processing(db_path, item_id)
        succeeded, error = process_item(uri, output_root)
        if succeeded:
            queue_db.mark_success(db_path, item_id)
        else:
            queue_db.mark_failed(db_path, item_id, error or "unknown error")
        remove_from_queue_dir(uri)


def _require_prefect_server() -> None:
    """`.serve()`'s cron schedule silently no-ops on an ephemeral server, so fail loudly instead."""
    api_url = os.environ.get("PREFECT_API_URL")
    if not api_url:
        raise SystemExit(
            "PREFECT_API_URL is not set. poll_and_ingest_flow.serve() needs a real Prefect "
            "server to run its cron schedule - an ephemeral server can't schedule runs. "
            "Start one with `./ingest server`, or run this via `./ingest poll` (which sets "
            "PREFECT_API_URL for you)."
        )
    try:
        with urlopen(f"{api_url}/health", timeout=5) as resp:
            if resp.status != 200:
                raise URLError(f"unexpected status {resp.status}")
    except URLError as exc:
        raise SystemExit(
            f"Cannot reach Prefect server at {api_url} ({exc}). Start one with "
            "`./ingest server` before running the poller."
        ) from exc


def main() -> None:
    _require_prefect_server()
    poll_and_ingest_flow.serve(name="poll-and-ingest", cron="*/1 * * * *")


if __name__ == "__main__":
    main()
