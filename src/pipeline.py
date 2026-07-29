from pathlib import Path

from prefect import flow, task

from ingestion.assets import copy_to_assets, write_status
from ingestion.config import load_config
from ingestion.detect import DetectedType, identify_mime_type
from ingestion.markitdown_ingest import markitdown_ingest_flow
from ingestion.pdf_ingest import pdf_ingest_flow
from ingestion.xlsx_ingest import xlsx_ingest_flow

_config = load_config()
_allowed_mime_types = set(_config["mime_types"])

_PDF_MIME_TYPES = {"application/pdf"}
_XLSX_MIME_TYPES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.oasis.opendocument.spreadsheet",
}


@task
def list_documents(directory: Path) -> list[Path]:
    """Find source documents in `directory` and print their names."""
    documents = sorted(p for p in directory.iterdir() if p.is_file())
    for doc in documents:
        print(doc.name)
    return documents


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


@flow(name="ingest-documents")
def ingest_flow(directory: Path = Path("data"), output_root: Path = Path("ingested")) -> list[Path]:
    documents = list_documents(directory)
    for doc in documents:
        detected = identify_mime_type(doc)
        source = "content" if detected.from_content else "extension fallback"
        print(f"{doc.name}: {detected.mime_type} ({source})")

        if detected.mime_type not in _allowed_mime_types:
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
    return documents


def main() -> None:
    ingest_flow()


if __name__ == "__main__":
    main()
