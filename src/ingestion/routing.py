from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency

from ingestion.tika import extract_with_tika
from ingestion.tika_ingest import tika_ingest_flow
from ingestion.xlsx_ingest import xlsx_ingest_flow

_XLSX_MIME_TYPES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.oasis.opendocument.spreadsheet",
}

# Maps a config/config.yaml `concurrency_limits` key to the Prefect global concurrency
# limit name that `route_document` acquires a slot from before running that document type.
_CONCURRENCY_LIMIT_NAMES = {
    "tika": "tika-ingest",
    "xlsx": "xlsx-ingest",
}


def ensure_concurrency_limits(limits: dict[str, int]) -> None:
    """Idempotently create/update the global concurrency limits `route_document` relies on.

    Must run before any document is routed - `concurrency()` is called with `strict=True`,
    so it raises rather than silently running unbounded if a limit hasn't been provisioned yet.
    """
    with get_client(sync_client=True) as client:
        for key, limit in limits.items():
            client.upsert_global_concurrency_limit_by_name(
                _CONCURRENCY_LIMIT_NAMES[key], limit
            )


def route_document(doc: Path, output_root: Path) -> None:
    """Parse with Tika, then run a supplementary flow when needed."""
    with concurrency(_CONCURRENCY_LIMIT_NAMES["tika"], strict=True):
        tika = extract_with_tika(doc)

    print(f"{doc.name}: {tika.mime_type} (apache-tika)")

    if tika.mime_type in _XLSX_MIME_TYPES:
        print("  route: xlsx -> DuckDB metadata extraction")
        with concurrency(_CONCURRENCY_LIMIT_NAMES["xlsx"], strict=True):
            xlsx_ingest_flow(doc, tika, output_root)
    else:
        print("  route: tika text + chunking")
        tika_ingest_flow(doc, tika, output_root)
