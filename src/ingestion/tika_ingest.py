from pathlib import Path

from prefect import flow, task

from ingestion.assets import copy_to_assets, write_metadata
from ingestion.chunking import chunk_and_finalize
from ingestion.tika import TikaDocument, document_metadata


@task
def stage_tika_document(doc: Path, tika: TikaDocument, output_root: Path) -> str:
    """Store the source and Tika metadata, then return the extracted text."""
    doc_dir = output_root / doc.stem
    copy_to_assets(doc, doc_dir)
    write_metadata(doc_dir, document_metadata(doc, tika))
    return tika.text


@flow(name="ingest-tika-document")
def tika_ingest_flow(doc: Path, tika: TikaDocument, output_root: Path) -> None:
    """Store and chunk a document already parsed by Tika."""
    text = stage_tika_document(doc, tika, output_root)
    chunk_and_finalize(doc, output_root, text)
