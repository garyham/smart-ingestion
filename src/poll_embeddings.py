import os
import socket
import time
from threading import Event, Thread
from urllib.error import URLError
from urllib.request import urlopen
from uuid import UUID, uuid4

from psycopg import DataError
from psycopg import Error as PostgresError

# Prefect reads this setting when it is imported.
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

from embeddings.flow import configured_models, embed_ingestion
from postgres_queue import (
    claim_outbox,
    complete_outbox,
    ensure_schema,
    extend_outbox_lease,
    fail_outbox,
)

TOPIC = "ingestion.completed"
LEASE_SECONDS = int(os.getenv("EMBEDDING_LEASE_SECONDS", "900"))
MAX_ATTEMPTS = int(os.getenv("EMBEDDING_MAX_ATTEMPTS", "5"))
POLL_SECONDS = float(os.getenv("EMBEDDING_POLL_SECONDS", "1"))


class OutboxLeaseHeartbeat:
    def __init__(self, event_id: UUID, consumer_id: str):
        self.event_id = event_id
        self.consumer_id = consumer_id
        self.stop = Event()
        self.thread = Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.wait(max(1, LEASE_SECONDS // 3)):
            try:
                if not extend_outbox_lease(
                    self.event_id, self.consumer_id, LEASE_SECONDS
                ):
                    return
            except PostgresError as exc:
                print(f"Could not extend embedding lease for {self.event_id}: {exc}")

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, _type, _value, _traceback):
        self.stop.set()
        self.thread.join()


def process_event(job: dict, consumer_id: str) -> None:
    event_id = job["id"]
    event = job["event"]

    try:
        with OutboxLeaseHeartbeat(event_id, consumer_id):
            if event.get("status") == "ok" and event.get("artifact_type") == "chunks":
                dense_model, sparse_model, device = configured_models()
                embed_ingestion(event, dense_model, sparse_model, device)
        if not complete_outbox(event_id, consumer_id):
            print(f"Lost lease before completing embedding event {event_id}")
    except (DataError, ValueError) as exc:
        print(f"Embedding rejected for {event_id}: {exc}")
        fail_outbox(
            event_id,
            consumer_id,
            str(exc),
            MAX_ATTEMPTS,
            MAX_ATTEMPTS,
        )
    except Exception as exc:  # noqa: BLE001 - failed work must remain queued
        print(f"Embedding failed for {event_id}: {exc}")
        fail_outbox(
            event_id,
            consumer_id,
            str(exc),
            job["attempts"],
            MAX_ATTEMPTS,
        )


def consume_events() -> None:
    consumer_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"
    print("Waiting for ingestion completion events")
    while True:
        job = claim_outbox(TOPIC, consumer_id, LEASE_SECONDS)
        if job is None:
            time.sleep(POLL_SECONDS)
            continue
        process_event(job, consumer_id)


def _require_prefect_server() -> None:
    api_url = os.environ["PREFECT_API_URL"]
    try:
        with urlopen(f"{api_url}/health", timeout=5) as response:
            if response.status != 200:
                raise URLError(f"unexpected status {response.status}")
    except URLError as exc:
        raise SystemExit(f"Cannot reach Prefect server at {api_url} ({exc})") from exc


def main() -> None:
    _require_prefect_server()
    ensure_schema()
    while True:
        try:
            consume_events()
        except (PostgresError, OSError) as exc:
            print(f"Embedding worker connection failed: {exc}; retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    main()
