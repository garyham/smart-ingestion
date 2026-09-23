"""The discard-abandoned-uploads flow, served hourly by the `sweep` service."""

from datetime import UTC, datetime, timedelta

from prefect import flow

from ingestion.common import document_store
from ingestion.ingestion_flow import UPLOAD_COMMIT_WINDOW

# A candidate is only abandoned once it is twice as old as the commit window, so a commit
# and the sweep never act on the same candidate unless one commit outlasts the whole window.
ABANDONED_AFTER = 2 * UPLOAD_COMMIT_WINDOW


@flow(name="discard-abandoned-uploads", retries=2, log_prints=True)
def discard_abandoned_uploads_flow() -> list[str]:
    """Remove bytes uploaded for candidates that never became a document.

    A client that uploads and never notifies, or a duplicate whose bytes could not be
    removed when it resolved, leaves bytes with no row. Removing them is idempotent, so
    the schedule can run this as often as it likes.
    """
    discarded = document_store().discard_abandoned_uploads(
        datetime.now(UTC) - ABANDONED_AFTER
    )
    print(f"discarded {len(discarded)} abandoned upload(s)")
    return [str(document_id) for document_id in discarded]


def upload_sweeper() -> None:
    discard_abandoned_uploads_flow.serve(
        name="discard-abandoned-uploads", interval=timedelta(hours=1)
    )
