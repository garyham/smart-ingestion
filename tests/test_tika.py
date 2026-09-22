import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ingestion.tika import TikaDocument, document_metadata, extract_with_tika


class TikaTests(unittest.TestCase):
    @patch("ingestion.tika.httpx.put")
    def test_extracts_type_metadata_and_text_in_one_request(self, put):
        payload = [
            {
                "Content-Type": "text/plain; charset=UTF-8",
                "dc:title": "Notes",
                "tk:content": "hello world",
            }
        ]
        response = Mock()
        response.json.return_value = payload
        put.return_value = response

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "notes.txt"
            path.write_text("hello world")
            result = extract_with_tika.fn(path)

        self.assertEqual(result.mime_type, "text/plain")
        self.assertEqual(result.text, "hello world")
        self.assertEqual(result.metadata["dc:title"], "Notes")
        self.assertNotIn("tk:content", result.metadata)
        self.assertTrue(put.call_args.args[0].endswith("/rmeta/text"))
        response.raise_for_status.assert_called_once_with()

    def test_builds_common_metadata(self):
        tika = TikaDocument("application/pdf", {"dc:title": "Report"}, "text")
        metadata = document_metadata(Path("report.pdf"), tika)
        self.assertEqual(metadata["title"], "Report")
        self.assertEqual(metadata["converter"], "apache-tika")


if __name__ == "__main__":
    unittest.main()
