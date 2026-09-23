import json
import shutil
from pathlib import Path


def copy_to_assets(doc: Path, doc_dir: Path) -> None:
    """Preserve a raw copy of the source document under `<doc_dir>/assets/`."""
    assets_dir = doc_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(doc, assets_dir / doc.name)


def write_status(doc_dir: Path, status: dict) -> None:
    """Write the ingestion outcome (ok / needs_intervention / failed) for a document."""
    (doc_dir / "status.json").write_text(json.dumps(status, indent=2))


def write_metadata(doc_dir: Path, metadata: dict) -> None:
    """Write extracted metadata for a document, for later vectorization/search."""
    (doc_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))


def stage_chunked(
    doc: Path,
    doc_dir: Path,
    *,
    status: dict,
    extraction: dict,
    extractor: str,
    chunker: str | None,
    chunk_count: int,
) -> None:
    """Write the bundle for a chunked document from what chunking found.

    The chunks themselves live in the store and are not copied here. What stays in the
    bundle is what every document type gets: the raw file, the extracted metadata, and the
    outcome - so a failed or partial ingestion is still inspectable.
    """
    copy_to_assets(doc, doc_dir)
    write_metadata(
        doc_dir,
        {
            **extraction,
            "extractor": extractor,
            "chunker": chunker,
            "chunk_count": chunk_count,
        },
    )
    write_status(doc_dir, status)
