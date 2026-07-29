from pathlib import Path

from ingestion import xlsx
from ingestion.assets import copy_to_assets, write_metadata, write_status
from ingestion.detect import DetectedType
from ingestion.outcome import read_outcome


def run_xlsx_ingest(doc: Path, detected: DetectedType, output_root: Path) -> tuple[bool, str | None]:
    """xlsx ingestion path: ingest into a queryable DuckDB database and extract its metadata via
    DuckDB standard queries. Not chunked, per CLAUDE.md. Returns (succeeded, error_detail).

    Wraps the whole pipeline in a catch-all so an unexpected failure outside the inner
    xlsx.extract_metadata try/except (e.g. copy_to_assets, write_metadata) can't strand the caller
    without an outcome - mirrors the safety net that used to live in poll_ingest.process_item, which
    this flow previously only got by virtue of being called from inside it.
    """
    try:
        doc_dir = output_root / doc.stem
        copy_to_assets(doc, doc_dir)

        db_path = doc_dir / f"{doc.stem}.duckdb"
        db_path.unlink(missing_ok=True)

        try:
            result = xlsx.extract_metadata(doc, db_path)
        except Exception as e:  # noqa: BLE001 - must record failure detail rather than crash the task
            write_status(
                doc_dir, {"status": "failed", "reason": "xlsx_ingestion_error", "detail": str(e)}
            )
            return read_outcome(output_root, doc)

        metadata = {
            "title": doc.stem,
            "mime_type": detected.mime_type,
            "kind": "queryable_dataset",
            "query_engine": "duckdb",
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
    except Exception as exc:  # noqa: BLE001 - must not strand the queue row in 'processing'
        return False, str(exc)
    return read_outcome(output_root, doc)
