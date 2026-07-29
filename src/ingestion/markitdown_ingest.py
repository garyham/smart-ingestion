from pathlib import Path

from markitdown import MarkItDown
from prefect import task

from ingestion.assets import copy_to_assets, write_metadata, write_status
from ingestion.detect import DetectedType

_markitdown = MarkItDown()


@task
def convert_with_markitdown(doc: Path, detected: DetectedType, output_root: Path) -> str | None:
    """Convert a document to markdown via markitdown. Writes assets/metadata, and on failure the
    final status too. Returns the markdown text, or None on failure.
    """
    doc_dir = output_root / doc.stem
    copy_to_assets(doc, doc_dir)

    try:
        result = _markitdown.convert(doc)
        markdown = result.text_content
    except Exception as e:
        write_status(
            doc_dir,
            {"status": "failed", "reason": "markitdown_conversion_failed", "detail": str(e)},
        )
        return None

    write_metadata(
        doc_dir,
        {
            "title": result.title or doc.stem,
            "mime_type": detected.mime_type,
            "origin_filename": doc.name,
            "converter": "markitdown",
        },
    )
    return markdown
