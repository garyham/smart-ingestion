from pathlib import Path

import pymupdf
import pymupdf4llm

from ingestion.assets import copy_to_assets, write_metadata, write_status
from ingestion.chunking import chunk_and_finalize
from ingestion.detect import DetectedType
from ingestion.outcome import read_outcome


def convert_pdf(doc: Path, detected: DetectedType, output_root: Path) -> str | None:
    """Convert a PDF to markdown via pymupdf4llm. Writes assets/metadata, and on failure the
    final status too. Returns the markdown text, or None on failure.
    """
    doc_dir = output_root / doc.stem
    copy_to_assets(doc, doc_dir)

    try:
        num_pages = pymupdf.open(doc).page_count
        markdown = pymupdf4llm.to_markdown(doc)
    except Exception as e:
        write_status(
            doc_dir, {"status": "failed", "reason": "pdf_conversion_failed", "detail": str(e)}
        )
        return None

    write_metadata(
        doc_dir,
        {
            "title": doc.stem,
            "mime_type": detected.mime_type,
            "origin_filename": doc.name,
            "num_pages": num_pages,
            "converter": "pymupdf4llm",
        },
    )
    return markdown


def run_pdf_ingest(doc: Path, detected: DetectedType, output_root: Path) -> tuple[bool, str | None]:
    """PDF ingestion path: convert via pymupdf4llm, then chunk. Returns (succeeded, error_detail).

    Wraps the whole pipeline in a catch-all so an unexpected failure (e.g. anywhere inside
    chunk_and_finalize, which has no try/except of its own) can't strand the caller without an
    outcome - mirrors the safety net that used to live in poll_ingest.process_item.
    """
    try:
        markdown = convert_pdf(doc, detected, output_root)
        chunk_and_finalize(doc, output_root, markdown)
    except Exception as exc:  # noqa: BLE001 - must not strand the queue row in 'processing'
        return False, str(exc)
    return read_outcome(output_root, doc)
