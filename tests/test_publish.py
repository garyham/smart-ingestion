import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from ingestion.publish import publish_bundle, sha256_file

SOURCE = {
    "document_id": "c63752f4-8d18-4da2-a107-24c52d0707cc",
    "content_id": "a" * 64,
    "filename": "notes.txt",
}


class FakeS3Client:
    def __init__(self):
        self.uploads = []
        self.objects = []

    def upload_file(self, filename, bucket, key, ExtraArgs):
        self.uploads.append((filename, bucket, key, ExtraArgs))

    def put_object(self, **kwargs):
        self.objects.append(kwargs)


class PublishBundleTests(unittest.TestCase):
    def publish(self, client, doc_dir, **overrides):
        return publish_bundle(
            SOURCE["document_id"],
            "ingestion-id",
            SOURCE,
            doc_dir,
            client=client,
            bucket="smart-files",
            prefix="ingested",
            **{
                "artifact_type": "chunks",
                "chunks": {"extractor": "tika@1", "chunker": "chunker@2", "count": 3},
                **overrides,
            },
        )

    def test_manifest_is_uploaded_after_artifacts(self):
        client = FakeS3Client()

        with tempfile.TemporaryDirectory() as temp_dir:
            doc_dir = Path(temp_dir)
            (doc_dir / "status.json").write_text('{"status": "ok"}')
            (doc_dir / "metadata.json").write_text('{"title": "Notes"}')
            (doc_dir / "assets").mkdir()
            (doc_dir / "assets" / "notes.txt").write_text("hello")

            event = self.publish(client, doc_dir)

        self.assertEqual(len(client.uploads), 3)
        manifest_object = client.objects[0]
        manifest = json.loads(manifest_object["Body"])
        self.assertEqual(
            manifest_object["Key"],
            f"ingested/{SOURCE['document_id']}/ingestion-id/manifest.json",
        )
        self.assertEqual(manifest["artifact_type"], "chunks")
        self.assertEqual(manifest["pipeline_version"], "1")
        self.assertEqual(len(manifest["artifacts"]), 3)
        self.assertEqual(event["status"], "ok")
        self.assertEqual(
            event["manifest_uri"],
            f"s3://smart-files/ingested/{SOURCE['document_id']}"
            "/ingestion-id/manifest.json",
        )

    def test_the_source_is_named_by_reference_not_by_object_key(self):
        client = FakeS3Client()

        with tempfile.TemporaryDirectory() as temp_dir:
            doc_dir = Path(temp_dir)
            (doc_dir / "status.json").write_text('{"status": "ok"}')

            event = self.publish(client, doc_dir)

        manifest = json.loads(client.objects[0]["Body"])
        self.assertEqual(manifest["source"], SOURCE)
        self.assertNotIn("bucket", manifest["source"])
        self.assertNotIn("object_key", manifest["source"])
        self.assertEqual(manifest["chunks"], {"extractor": "tika@1", "chunker": "chunker@2", "count": 3})
        self.assertEqual(event["source_content_id"], "a" * 64)

    def test_a_failed_ingestion_still_publishes_its_bundle(self):
        client = FakeS3Client()

        with tempfile.TemporaryDirectory() as temp_dir:
            doc_dir = Path(temp_dir)
            (doc_dir / "status.json").write_text(
                '{"status": "needs_intervention", "reason": "no_content_extracted"}'
            )
            (doc_dir / "assets").mkdir()
            (doc_dir / "assets" / "notes.txt").write_text("hello")

            event = self.publish(client, doc_dir, artifact_type="diagnostic")

        manifest = json.loads(client.objects[0]["Body"])
        self.assertEqual(event["status"], "needs_intervention")
        self.assertEqual(manifest["status"]["reason"], "no_content_extracted")
        self.assertTrue(
            any(item["name"] == "assets/notes.txt" for item in manifest["artifacts"])
        )


class Sha256FileTests(unittest.TestCase):
    def test_hashes_the_file_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "document.bin"
            path.write_bytes(b"document contents")

            digest = sha256_file(path)

        self.assertEqual(digest, hashlib.sha256(b"document contents").hexdigest())


if __name__ == "__main__":
    unittest.main()
