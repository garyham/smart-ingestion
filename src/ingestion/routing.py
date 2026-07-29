from pathlib import Path

from ingestion.assets import copy_to_assets, write_status
from ingestion.detect import DetectedType

_PDF_MIME_TYPES = {"application/pdf"}
_XLSX_MIME_TYPES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.oasis.opendocument.spreadsheet",
}


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


def classify_document(mime_type: str, allowed_mime_types: set[str]) -> str:
    """Classify a detected MIME type into the ingestion path that should handle it."""
    if mime_type not in allowed_mime_types:
        return "unsupported"
    if mime_type in _XLSX_MIME_TYPES:
        return "xlsx"
    if mime_type in _PDF_MIME_TYPES:
        return "pdf"
    return "markitdown"
