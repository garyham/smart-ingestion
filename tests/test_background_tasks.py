import json
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from background_tasks import (
    InvalidUploadEvent,
    embed_document,
    ingest_upload_flow,
    parse_upload_event,
    process_upload,
)

EVENT = {
    "event_id": "38e00553-d6cc-4bf7-9293-27bb86563c36",
    "event": "file.uploaded",
    "schema_version": 1,
    "document_id": "c63752f4-8d18-4da2-a107-24c52d0707cc",
    "bucket": "smart-files",
    "object_key": "uploads/id/notes.txt",
    "filename": "notes.txt",
}


class BackgroundTaskTests(unittest.TestCase):
    def test_parse_upload_event(self):
        self.assertEqual(parse_upload_event(json.dumps(EVENT).encode()), EVENT)

    def test_parse_upload_event_rejects_invalid_messages(self):
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b"not JSON")
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b'{"event": "something.else"}')

    @patch("background_tasks.embed_document.delay")
    @patch("background_tasks.update_ingestion")
    @patch(
        "background_tasks.configured_models", return_value=("dense", "sparse", "cpu")
    )
    @patch("background_tasks.ingest_upload_flow")
    def test_upload_submits_embedding_task(self, ingest, _models, update, delay):
        ingest.return_value = {
            "document_id": EVENT["document_id"],
            "ingestion_id": EVENT["event_id"],
            "status": "ok",
            "artifact_type": "chunks",
            "manifest_uri": "s3://smart-files/manifest.json",
        }
        delay.return_value.task_run_id = "embedding-run"

        result = process_upload.fn(EVENT)

        delay.assert_called_once_with(ingest.return_value, "dense", "sparse", "cpu")
        self.assertEqual(result["embedding_task_run_id"], "embedding-run")
        update.assert_called_once()

    @patch("background_tasks.embed_document.delay")
    @patch("background_tasks.update_ingestion")
    @patch("background_tasks.ingest_upload_flow")
    def test_upload_skips_embedding_for_duckdb(self, ingest, update, delay):
        ingest.return_value = {
            "ingestion_id": EVENT["event_id"],
            "status": "ok",
            "artifact_type": "duckdb",
        }

        process_upload.fn(EVENT)

        delay.assert_not_called()
        update.assert_called_once()

    @patch("background_tasks.update_ingestion")
    @patch("background_tasks.claim_ingestion")
    @patch("background_tasks.route_document")
    @patch("background_tasks.publish_bundle")
    @patch("background_tasks.s3_client")
    def test_ingest_flow_downloads_and_publishes(
        self, make_s3_client, publish_bundle, route_document, claim, update
    ):
        def download_file(_bucket, _object_key, destination):
            Path(destination).write_text("hello")

        make_s3_client.return_value.download_file.side_effect = download_file

        def route(doc, output_root):
            status_dir = output_root / doc.stem
            status_dir.mkdir(parents=True)
            (status_dir / "status.json").write_text('{"status": "ok"}')

        route_document.side_effect = route
        claim.return_value = (True, {"status": "processing", "outputs": {}})
        publish_bundle.return_value = {
            "event": "ingestion.completed",
            "ingestion_id": EVENT["event_id"],
        }

        result = ingest_upload_flow.fn(EVENT)

        route_document.assert_called_once()
        publish_bundle.assert_called_once()
        self.assertEqual(result["event"], "ingestion.completed")
        source_event = publish_bundle.call_args.args[5]
        self.assertEqual(source_event["source_sha256"], sha256(b"hello").hexdigest())
        update.assert_called_once()

    @patch("background_tasks.claim_ingestion")
    @patch("background_tasks.route_document")
    @patch("background_tasks.s3_client")
    def test_ingest_flow_returns_existing_ingestion_for_duplicate(
        self, make_s3_client, route_document, claim
    ):
        make_s3_client.return_value.download_file.side_effect = (
            lambda _bucket, _key, destination: Path(destination).write_bytes(b"same")
        )
        claim.return_value = (
            False,
            {
                "ingestion_id": "2cf8c7a2-f67e-4f0f-a231-c27a0d47986d",
                "status": "processing",
                "outputs": {},
            },
        )

        result = ingest_upload_flow.fn(EVENT)

        self.assertTrue(result["duplicate"])
        self.assertEqual(result["status"], "processing")
        route_document.assert_not_called()

    @patch("background_tasks.update_ingestion")
    @patch("background_tasks.embed_ingestion")
    def test_embedding_task_runs_flow(self, embed_ingestion, update):
        completion = {
            "ingestion_id": EVENT["event_id"],
            "status": "ok",
            "artifact_type": "chunks",
        }

        embed_document.fn(completion, "dense", "sparse", "cpu")

        embed_ingestion.assert_called_once_with(completion, "dense", "sparse", "cpu")
        self.assertEqual(update.call_count, 2)

    @patch("background_tasks.embed_ingestion")
    def test_embedding_task_ignores_non_chunk_bundle(self, embed_ingestion):
        embed_document.fn(
            {"status": "ok", "artifact_type": "duckdb"},
            "dense",
            "sparse",
            "cpu",
        )

        embed_ingestion.assert_not_called()


if __name__ == "__main__":
    unittest.main()
