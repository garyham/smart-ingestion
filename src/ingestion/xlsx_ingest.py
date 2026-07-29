from pathlib import Path

from prefect import flow

from ingestion import xlsx
from ingestion.assets import copy_to_assets, write_metadata, write_status
from ingestion.detect import DetectedType


@flow(name="ingest-xlsx")
def xlsx_ingest_flow(doc: Path, detected: DetectedType, output_root: Path) -> None:
    """Subflow for the xlsx ingestion path: ingest into a queryable DuckDB database and
    extract its metadata via DuckDB standard queries. Not chunked, per CLAUDE.md.
    """
    doc_dir = output_root / doc.stem
    copy_to_assets(doc, doc_dir)

    db_path = doc_dir / f"{doc.stem}.duckdb"
    db_path.unlink(missing_ok=True)

    try:
        result = xlsx.extract_metadata(doc, db_path)
    except Exception as e:
        write_status(
            doc_dir, {"status": "failed", "reason": "xlsx_ingestion_error", "detail": str(e)}
        )
        return

    metadata = {
        "title": doc.stem,
        "mime_type": detected.mime_type,
        "duckdb_file": db_path.name,
        "background": result["background"],
        "tables": result["tables"],
    }
    write_metadata(doc_dir, metadata)

    if not result["tables"]:
        status = {
            "status": "needs_intervention",
            "reason": "no_data_tables_found",
            "detail": "No numbered data sheets could be parsed into tables.",
            "failed_sheets": result["failed_sheets"],
        }
    elif result["failed_sheets"]:
        status = {
            "status": "needs_intervention",
            "reason": "some_sheets_failed",
            "detail": f"{len(result['failed_sheets'])} sheet(s) could not be parsed into tables.",
            "failed_sheets": result["failed_sheets"],
        }
    else:
        status = {"status": "ok"}
    write_status(doc_dir, status)
