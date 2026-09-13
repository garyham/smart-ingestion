import json
import tempfile
import unittest
from pathlib import Path

from ingestion.publish import publish_bundle


class FakeS3Client:
    def __init__(self):
        self.uploads = []
        self.objects = []

    def upload_file(self, filename, bucket, key, ExtraArgs):
        self.uploads.append((filename, bucket, key, ExtraArgs))

    def put_object(self, **kwargs):
        self.objects.append(kwargs)


class PublishBundleTests(unittest.TestCase):
    def test_manifest_is_uploaded_after_artifacts(self):
        client = FakeS3Client()
        source_event = {
            "bucket": "smart-files",
            "object_key": "uploads/doc/notes.txt",
            "filename": "notes.txt",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            doc_dir = Path(temp_dir)
            (doc_dir / "status.json").write_text('{"status": "ok"}')
            (doc_dir / "metadata.json").write_text('{"title": "Notes"}')
            (doc_dir / "chunks.jsonl").write_text('{"index": 0, "text": "hello"}')

            event = publish_bundle(
                client,
                "smart-files",
                "ingested",
                "document-id",
                "ingestion-id",
                source_event,
                doc_dir,
            )

        self.assertEqual(len(client.uploads), 3)
        manifest_object = client.objects[0]
        manifest = json.loads(manifest_object["Body"])
        self.assertEqual(manifest_object["Key"], "ingested/document-id/ingestion-id/manifest.json")
        self.assertEqual(manifest["artifact_type"], "chunks")
        self.assertEqual(len(manifest["artifacts"]), 3)
        self.assertEqual(event["status"], "ok")
        self.assertEqual(event["manifest_uri"], "s3://smart-files/ingested/document-id/ingestion-id/manifest.json")


if __name__ == "__main__":
    unittest.main()
