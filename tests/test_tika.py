import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.tika import TikaDocument, document_metadata, extract_with_tika


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class TikaTests(unittest.TestCase):
    @patch("ingestion.tika.urlopen")
    def test_extracts_type_metadata_and_text_in_one_request(self, urlopen):
        payload = [
            {
                "Content-Type": "text/plain; charset=UTF-8",
                "dc:title": "Notes",
                "tk:content": "hello world",
            }
        ]
        urlopen.return_value = Response(json.dumps(payload).encode())

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "notes.txt"
            path.write_text("hello world")
            result = extract_with_tika.fn(path)

        self.assertEqual(result.mime_type, "text/plain")
        self.assertEqual(result.text, "hello world")
        self.assertEqual(result.metadata["dc:title"], "Notes")
        self.assertNotIn("tk:content", result.metadata)
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.endswith("/rmeta/text"))

    def test_builds_common_metadata(self):
        tika = TikaDocument("application/pdf", {"dc:title": "Report"}, "text")
        metadata = document_metadata(Path("report.pdf"), tika)
        self.assertEqual(metadata["title"], "Report")
        self.assertEqual(metadata["converter"], "apache-tika")


if __name__ == "__main__":
    unittest.main()
