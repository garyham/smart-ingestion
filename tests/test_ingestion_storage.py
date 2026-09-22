import hashlib
import tempfile
import unittest
from pathlib import Path

from ingestion.storage import sha256_file


class IngestionStorageTests(unittest.TestCase):
    def test_sha256_file_hashes_source_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "document.bin"
            path.write_bytes(b"document contents")

            digest = sha256_file(path)

        self.assertEqual(digest, hashlib.sha256(b"document contents").hexdigest())


if __name__ == "__main__":
    unittest.main()
