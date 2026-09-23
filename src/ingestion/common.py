"""What every ingestion flow shares: the store, the retry policy, and the document's status.

Two layers, and nothing between them. Flows and tasks control execution - what runs, in
what order, what is retried, what holds a concurrency slot. The store writes rows and bytes
to tables it owns. Chunkers and embedders are stateless operators a task runs before it
hands their output to the store. A task is the unit of retry, so each one wraps a stage
that can fail on its own: the network calls and the expensive compute. There is no lease,
no attempt counter, and no in-flight row; a task that dies is simply run again, and the
unique constraint on each table's reuse key is what stops two attempts becoming two
artifacts.

Flows record a document's status as they go, and publish it once it is completely
ingested. The status is for people to read: nothing here reads it back to decide what
runs.
"""

from uuid import UUID

from prefect.tasks import exponential_backoff

from contracts.refs import DocumentStatus
from store.service import DocumentStore

RETRY = {
    "retry_delay_seconds": exponential_backoff(backoff_factor=2),
    "retry_jitter_factor": 0.2,
}


def document_store() -> DocumentStore:
    """Built inside the task that uses it: the store is never passed between tasks."""
    return DocumentStore()


def error_detail(error: Exception) -> dict:
    return {"type": type(error).__name__, "detail": str(error)}


def record_status(
    document_id: UUID, status: DocumentStatus, error: dict | None = None
) -> None:
    """Show where the document got to. Display only: nothing reads it to decide."""
    document_store().set_status(document_id, status, error)


def record_outcome(document_id: UUID, outcome: dict, *, embeds: bool) -> None:
    """Carry a published bundle's `status.json` onto the document.

    A document that still has chunks to embed is not finished yet; one that has nothing
    to embed is published as soon as its bundle is.
    """
    status = DocumentStatus(outcome["status"])
    if status != DocumentStatus.OK:
        detail = {key: value for key, value in outcome.items() if key != "status"}
        record_status(document_id, status, detail or None)
    elif embeds:
        record_status(document_id, DocumentStatus.EMBEDDING)
    else:
        document_store().mark_published(document_id)


def record_crash(document_id: UUID, state) -> None:
    """A crash skips the flow's own `except`, so its hook records the failure instead."""
    record_status(
        document_id,
        DocumentStatus.FAILED,
        {"type": "Crashed", "detail": state.message or "the run crashed"},
    )
