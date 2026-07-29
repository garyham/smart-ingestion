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
    (doc_dir / "metadata").write_text(json.dumps(metadata, indent=2, default=str))
