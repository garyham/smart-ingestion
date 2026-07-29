from pathlib import Path

import pymupdf
import pymupdf4llm
from prefect import task

from ingestion.assets import copy_to_assets, write_metadata, write_status
from ingestion.detect import DetectedType


@task
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
