import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ingestion.tika import (
    TikaDocument,
    document_metadata,
    parse_document,
)


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

        result = parse_document(b"hello world", "notes.txt")

        self.assertEqual(result.mime_type, "text/plain")
        self.assertEqual(result.text, "hello world")
        self.assertEqual(result.metadata["dc:title"], "Notes")
        self.assertNotIn("tk:content", result.metadata)
        self.assertTrue(put.call_args.args[0].endswith("/rmeta/text"))
        response.raise_for_status.assert_called_once_with()

    @patch("ingestion.tika.httpx.put")
    def test_keeps_embedded_document_metadata(self, put):
        response = Mock()
        response.json.return_value = [
            {"Content-Type": "application/zip", "tk:content": "outer"},
            {"Content-Type": "text/plain", "tk:content": "inner"},
        ]
        put.return_value = response

        result = parse_document(b"PK", "bundle.zip")

        self.assertEqual(len(result.metadata["embedded"]), 1)
        self.assertNotIn("tk:content", result.metadata["embedded"][0])

    def test_builds_common_metadata_from_a_path_or_a_name(self):
        tika = TikaDocument("application/pdf", {"dc:title": "Report"}, "text")

        self.assertEqual(document_metadata(Path("report.pdf"), tika)["title"], "Report")
        self.assertEqual(
            document_metadata("report.pdf", tika)["origin_filename"], "report.pdf"
        )

    def test_falls_back_to_the_filename_for_an_untitled_document(self):
        tika = TikaDocument("application/pdf", {}, "text")
        self.assertEqual(document_metadata("report.pdf", tika)["title"], "report")


if __name__ == "__main__":
    unittest.main()
