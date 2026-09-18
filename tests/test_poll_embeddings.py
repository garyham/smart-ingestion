import unittest
from unittest.mock import patch

from psycopg.errors import DataException

from poll_embeddings import process_event

EVENT_ID = "38e00553-d6cc-4bf7-9293-27bb86563c36"
EVENT = {
    "document_id": "c63752f4-8d18-4da2-a107-24c52d0707cc",
    "ingestion_id": EVENT_ID,
    "status": "ok",
    "artifact_type": "chunks",
    "manifest_uri": "s3://smart-files/ingested/doc/run/manifest.json",
}


class PollEmbeddingsTests(unittest.TestCase):
    @patch("poll_embeddings.OutboxLeaseHeartbeat")
    @patch("poll_embeddings.complete_outbox", return_value=True)
    @patch("poll_embeddings.configured_models", return_value=("dense", "sparse", "cpu"))
    @patch("poll_embeddings.embed_ingestion")
    def test_success_completes_outbox_event(
        self, embed_ingestion, _configured_models, complete_outbox, _heartbeat
    ):
        process_event({"id": EVENT_ID, "event": EVENT, "attempts": 1}, "worker")

        embed_ingestion.assert_called_once_with(EVENT, "dense", "sparse", "cpu")
        complete_outbox.assert_called_once_with(EVENT_ID, "worker")

    @patch("poll_embeddings.OutboxLeaseHeartbeat")
    @patch("poll_embeddings.fail_outbox")
    @patch("poll_embeddings.complete_outbox")
    @patch("poll_embeddings.embed_ingestion", side_effect=RuntimeError("dense failed"))
    def test_embedding_failure_requeues_without_completing(
        self, _embed_ingestion, complete_outbox, fail_outbox, _heartbeat
    ):
        process_event({"id": EVENT_ID, "event": EVENT, "attempts": 1}, "worker")

        complete_outbox.assert_not_called()
        fail_outbox.assert_called_once()

    @patch("poll_embeddings.OutboxLeaseHeartbeat")
    @patch("poll_embeddings.fail_outbox")
    @patch("poll_embeddings.complete_outbox")
    @patch(
        "poll_embeddings.embed_ingestion",
        side_effect=DataException("invalid sparse vector"),
    )
    def test_data_error_exhausts_event_without_repeating(
        self, _embed_ingestion, complete_outbox, fail_outbox, _heartbeat
    ):
        process_event({"id": EVENT_ID, "event": EVENT, "attempts": 1}, "worker")

        complete_outbox.assert_not_called()
        self.assertEqual(fail_outbox.call_args.args[3:], (5, 5))

    @patch("poll_embeddings.OutboxLeaseHeartbeat")
    @patch("poll_embeddings.complete_outbox", return_value=True)
    @patch("poll_embeddings.embed_ingestion")
    def test_non_chunk_event_is_completed_without_embedding(
        self, embed_ingestion, complete_outbox, _heartbeat
    ):
        event = {**EVENT, "artifact_type": "duckdb"}
        process_event({"id": EVENT_ID, "event": event, "attempts": 1}, "worker")

        embed_ingestion.assert_not_called()
        complete_outbox.assert_called_once()


if __name__ == "__main__":
    unittest.main()
