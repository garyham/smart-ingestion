import hashlib
import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from ingestion.schemas import IngestionCreate
from ingestion.storage import _sqlalchemy_url, sha256_file


class IngestionStorageTests(unittest.TestCase):
    def test_sha256_file_hashes_source_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "document.bin"
            path.write_bytes(b"document contents")

            digest = sha256_file(path)

        self.assertEqual(digest, hashlib.sha256(b"document contents").hexdigest())

    def test_sqlalchemy_url_uses_psycopg_driver(self):
        self.assertEqual(
            _sqlalchemy_url("postgresql://user:pass@db/name"),
            "postgresql+psycopg://user:pass@db/name",
        )

    def test_ingestion_create_rejects_invalid_sha256(self):
        with self.assertRaises(ValidationError):
            IngestionCreate(
                ingestion_id="38e00553-d6cc-4bf7-9293-27bb86563c36",
                document_id="c63752f4-8d18-4da2-a107-24c52d0707cc",
                source_sha256="invalid",
                pipeline_version="1",
                source={},
            )


if __name__ == "__main__":
    unittest.main()
