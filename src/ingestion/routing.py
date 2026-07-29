from pathlib import Path

from prefect import task
from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency

from ingestion.assets import copy_to_assets, write_status
from ingestion.detect import DetectedType
from ingestion.markitdown_ingest import markitdown_ingest_flow
from ingestion.pdf_ingest import pdf_ingest_flow
from ingestion.xlsx_ingest import xlsx_ingest_flow

_PDF_MIME_TYPES = {"application/pdf"}
_XLSX_MIME_TYPES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.oasis.opendocument.spreadsheet",
}

# Maps a config/config.yaml `concurrency_limits` key to the Prefect global concurrency
# limit name that `route_document` acquires a slot from before running that document type.
_CONCURRENCY_LIMIT_NAMES = {
    "pdf": "pdf-ingest",
    "markitdown": "markitdown-ingest",
    "xlsx": "xlsx-ingest",
}


def ensure_concurrency_limits(limits: dict[str, int]) -> None:
    """Idempotently create/update the global concurrency limits `route_document` relies on.

    Must run before any document is routed - `concurrency()` is called with `strict=True`,
    so it raises rather than silently running unbounded if a limit hasn't been provisioned yet.
    """
    with get_client(sync_client=True) as client:
        for key, limit in limits.items():
            client.upsert_global_concurrency_limit_by_name(_CONCURRENCY_LIMIT_NAMES[key], limit)


@task
def mark_unsupported(doc: Path, detected: DetectedType, output_root: Path) -> None:
    """Record an unsupported MIME type as a failed ingestion, preserving the raw file."""
    doc_dir = output_root / doc.stem
    copy_to_assets(doc, doc_dir)

    write_status(
        doc_dir,
        {
            "status": "failed",
            "reason": "unsupported_mime_type",
            "detail": f"MIME type '{detected.mime_type}' is not in the configured whitelist.",
            "mime_type": detected.mime_type,
            "detected_from": "content" if detected.from_content else "extension fallback",
        },
    )


def route_document(
    doc: Path, detected: DetectedType, output_root: Path, allowed_mime_types: set[str]
) -> None:
    """Dispatch a document to the ingestion subflow for its MIME type."""
    source = "content" if detected.from_content else "extension fallback"
    print(f"{doc.name}: {detected.mime_type} ({source})")

    if detected.mime_type not in allowed_mime_types:
        print(f"  route: unsupported -> failed")
        mark_unsupported(doc, detected, output_root)
    elif detected.mime_type in _XLSX_MIME_TYPES:
        print(f"  route: xlsx -> DuckDB metadata extraction")
        with concurrency(_CONCURRENCY_LIMIT_NAMES["xlsx"], strict=True):
            xlsx_ingest_flow(doc, detected, output_root)
    elif detected.mime_type in _PDF_MIME_TYPES:
        print(f"  route: pdf -> pymupdf4llm + chunking")
        with concurrency(_CONCURRENCY_LIMIT_NAMES["pdf"], strict=True):
            pdf_ingest_flow(doc, detected, output_root)
    else:
        print(f"  route: markitdown + chunking")
        with concurrency(_CONCURRENCY_LIMIT_NAMES["markitdown"], strict=True):
            markitdown_ingest_flow(doc, detected, output_root)
