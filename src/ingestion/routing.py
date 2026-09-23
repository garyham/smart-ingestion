"""Which ingestion path a MIME type belongs to, and the concurrency slots the work holds.

Routing decides and names; it neither detects nor runs. The flow's extractor reports the
type and the flow dispatches to the path this returns, so every stage of the work is a
Prefect task of its own rather than something hidden inside one opaque call.
"""

from contextlib import contextmanager

from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency

CHUNKS = "chunks"
DUCKDB = "duckdb"

# Spreadsheets become a queryable dataset, not chunks, so they never reach a chunker.
SPREADSHEET_MIME_TYPES = frozenset(
    {
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.oasis.opendocument.spreadsheet",
    }
)

# Maps a config/config.yaml `concurrency_limits` key to the Prefect global concurrency
# limit that bounds it: `tika` for extraction, `xlsx` for building a DuckDB dataset.
_CONCURRENCY_LIMIT_NAMES = {
    "tika": "tika-ingest",
    "xlsx": "xlsx-ingest",
}


def ensure_concurrency_limits(limits: dict[str, int]) -> None:
    """Idempotently create/update the global concurrency limits the flow relies on.

    Must run before any document is ingested - `concurrency()` is called with
    `strict=True`, so it raises rather than silently running unbounded if a limit hasn't
    been provisioned yet.
    """
    with get_client(sync_client=True) as client:
        for key, limit in limits.items():
            client.upsert_global_concurrency_limit_by_name(
                _CONCURRENCY_LIMIT_NAMES[key], limit
            )


def route_mime_type(mime_type: str) -> str:
    """Name the path a document of this MIME type belongs to: `chunks` or `duckdb`."""
    return DUCKDB if mime_type in SPREADSHEET_MIME_TYPES else CHUNKS


@contextmanager
def concurrency_slot(key: str):
    """Hold a slot in the `concurrency_limits` entry named `key` for the work inside."""
    with concurrency(_CONCURRENCY_LIMIT_NAMES[key], strict=True):
        yield
