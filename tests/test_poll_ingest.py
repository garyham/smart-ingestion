import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from poll_ingest import InvalidUploadEvent, handle_message, ingest_upload, parse_upload_event


EVENT = {
    "event_id": "38e00553-d6cc-4bf7-9293-27bb86563c36",
    "event": "file.uploaded",
    "schema_version": 1,
    "document_id": "c63752f4-8d18-4da2-a107-24c52d0707cc",
    "bucket": "smart-files",
    "object_key": "uploads/id/notes.txt",
    "filename": "notes.txt",
}


class PollIngestTests(unittest.TestCase):
    def test_parse_upload_event(self):
        self.assertEqual(parse_upload_event(json.dumps(EVENT).encode()), EVENT)

    def test_parse_upload_event_rejects_invalid_messages(self):
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b"not JSON")
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b'{"event": "something.else"}')
        invalid_version = {**EVENT, "schema_version": 2}
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(json.dumps(invalid_version).encode())

    @patch("poll_ingest.ingest_upload")
    def test_successful_ingestion_acknowledges_message(self, ingest_upload):
        channel = Mock()
        method = SimpleNamespace(delivery_tag=12)

        handle_message(channel, method, None, json.dumps(EVENT).encode())

        ingest_upload.assert_called_once_with(EVENT)
        channel.basic_ack.assert_called_once_with(delivery_tag=12)
        channel.basic_nack.assert_not_called()

    @patch("poll_ingest.ingest_upload", side_effect=RuntimeError("failed"))
    def test_failed_ingestion_requeues_message(self, _ingest_upload):
        channel = Mock()
        method = SimpleNamespace(delivery_tag=13)

        handle_message(channel, method, None, json.dumps(EVENT).encode())

        channel.basic_nack.assert_called_once_with(delivery_tag=13, requeue=True)
        channel.basic_ack.assert_not_called()

    @patch("poll_ingest.ingest_upload")
    def test_invalid_message_is_rejected(self, ingest_upload):
        channel = Mock()
        method = SimpleNamespace(delivery_tag=14)

        handle_message(channel, method, None, b"not JSON")

        ingest_upload.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag=14, requeue=False)

    @patch("poll_ingest.route_document")
    @patch("poll_ingest.identify_mime_type", return_value=object())
    @patch("poll_ingest.publish_completion")
    @patch("poll_ingest.publish_bundle")
    @patch("poll_ingest.s3_client")
    def test_ingest_upload_downloads_from_seaweedfs(
        self,
        make_s3_client,
        publish_bundle,
        publish_completion,
        _identify_mime_type,
        route_document,
    ):
        def download_file(bucket, object_key, destination):
            self.assertEqual(bucket, "smart-files")
            self.assertEqual(object_key, "uploads/id/notes.txt")
            Path(destination).write_text("hello")

        make_s3_client.return_value.download_file.side_effect = download_file

        def route(doc, _detected, output_root, _allowed):
            status_dir = output_root / doc.stem
            status_dir.mkdir(parents=True)
            (status_dir / "status.json").write_text('{"status": "ok"}')

        completion = {"event": "ingestion.completed"}
        route_document.side_effect = route
        publish_bundle.return_value = completion
        ingest_upload.fn(EVENT)

        route_document.assert_called_once()
        publish_bundle.assert_called_once()
        publish_completion.assert_called_once_with(completion)


if __name__ == "__main__":
    unittest.main()
