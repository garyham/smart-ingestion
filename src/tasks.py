from pathlib import Path

from celery import chain

from celery_app import app
from ingestion.config import load_config
from ingestion.detect import DetectedType, identify_mime_type
from ingestion.markitdown_ingest import run_markitdown_ingest
from ingestion.outcome import read_outcome
from ingestion.pdf_ingest import run_pdf_ingest
from ingestion.routing import classify_document, mark_unsupported
from ingestion.xlsx_ingest import run_xlsx_ingest
from queue_uri import from_file_uri, to_file_uri

_config = load_config()
_allowed_mime_types = set(_config["mime_types"])

_QUEUE_DIR = Path("queue")
_PROCESSING_DIR = _QUEUE_DIR / ".processing"
_OUTPUT_ROOT = Path("ingested")


@app.task
def mark_unsupported_task(
    doc_str: str, mime_type: str, from_content: bool, output_root_str: str
) -> tuple[bool, str | None]:
    doc = Path(doc_str)
    output_root = Path(output_root_str)
    mark_unsupported(doc, DetectedType(mime_type=mime_type, from_content=from_content), output_root)
    return read_outcome(output_root, doc)


@app.task
def ingest_pdf_task(
    doc_str: str, mime_type: str, from_content: bool, output_root_str: str
) -> tuple[bool, str | None]:
    return run_pdf_ingest(
        Path(doc_str), DetectedType(mime_type=mime_type, from_content=from_content), Path(output_root_str)
    )


@app.task
def ingest_markitdown_task(
    doc_str: str, mime_type: str, from_content: bool, output_root_str: str
) -> tuple[bool, str | None]:
    return run_markitdown_ingest(
        Path(doc_str), DetectedType(mime_type=mime_type, from_content=from_content), Path(output_root_str)
    )


@app.task
def ingest_xlsx_task(
    doc_str: str, mime_type: str, from_content: bool, output_root_str: str
) -> tuple[bool, str | None]:
    return run_xlsx_ingest(
        Path(doc_str), DetectedType(mime_type=mime_type, from_content=from_content), Path(output_root_str)
    )


@app.task
def finalize_task(outcome: tuple[bool, str | None], uri: str) -> None:
    # The claimed copy under queue/.processing/ is removed regardless of outcome - the raw
    # document is preserved separately under ingested/<doc>/assets/, and success/failure is
    # already durably recorded in that document's status.json.
    from_file_uri(uri).unlink(missing_ok=True)


_TASK_FOR_KIND = {
    "unsupported": mark_unsupported_task,
    "pdf": ingest_pdf_task,
    "xlsx": ingest_xlsx_task,
    "markitdown": ingest_markitdown_task,
}


@app.task
def dispatch_item_task(uri: str) -> None:
    """Detect a queued document's type and hand it off to the matching ingestion task, chained
    to finalize_task so both success and any unhandled exception reach finalization.
    """
    try:
        doc = from_file_uri(uri)
        detected = identify_mime_type(doc)
        kind = classify_document(detected.mime_type, _allowed_mime_types)
    except Exception as exc:  # noqa: BLE001 - must not strand a claimed file in .processing/ forever
        finalize_task.si((False, str(exc)), uri).apply_async()
        return

    ingest_task = _TASK_FOR_KIND[kind]
    args = (str(doc), detected.mime_type, detected.from_content, str(_OUTPUT_ROOT))
    chain(ingest_task.s(*args), finalize_task.s(uri)).apply_async()


@app.task
def poll_and_enqueue_task() -> None:
    """Beat-triggered, every minute: scan queue/ and claim every file found by moving it into
    queue/.processing/ before dispatching it. The move is what stops a file that's still being
    processed from being picked up again on the next tick - no separate pending/processing table
    needed, since the file's location on disk *is* the state.
    """
    _QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    _PROCESSING_DIR.mkdir(parents=True, exist_ok=True)

    for p in sorted(_QUEUE_DIR.iterdir()):
        if not p.is_file():
            continue
        claimed = _PROCESSING_DIR / p.name
        p.rename(claimed)
        dispatch_item_task.apply_async(args=(to_file_uri(claimed),))
