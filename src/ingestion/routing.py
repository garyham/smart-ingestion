from pathlib import Path

from prefect import task

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
        xlsx_ingest_flow(doc, detected, output_root)
    elif detected.mime_type in _PDF_MIME_TYPES:
        print(f"  route: pdf -> pymupdf4llm + chunking")
        pdf_ingest_flow(doc, detected, output_root)
    else:
        print(f"  route: markitdown + chunking")
        markitdown_ingest_flow(doc, detected, output_root)
