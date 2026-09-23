import unittest
from unittest.mock import patch

from ingestion.tika import TikaDocument
from operators import extractors
from operators.extractors import UnknownExtractor
from operators.extractors.tika_v1 import TikaV1


class RegistryTests(unittest.TestCase):
    def test_finds_an_extractor_by_version(self):
        extractor = extractors.get("tika@1")
        self.assertIsInstance(extractor, TikaV1)
        self.assertEqual(str(extractor.ref), "tika@1")

    def test_rejects_an_unknown_extractor(self):
        with self.assertRaises(UnknownExtractor):
            extractors.get("tika@99")


class TikaV1Tests(unittest.TestCase):
    def test_extraction_carries_the_type_text_and_document_metadata(self):
        tika = TikaDocument("text/plain", {"dc:title": "Notes"}, "hello")
        with patch(
            "operators.extractors.tika_v1.parse_document", return_value=tika
        ) as parse:
            extraction = TikaV1().extract(b"hello", "notes.md")

        parse.assert_called_once_with(b"hello", "notes.md")
        self.assertEqual(extraction.mime_type, "text/plain")
        self.assertEqual(extraction.text, "hello")
        self.assertEqual(extraction.metadata["title"], "Notes")
        self.assertEqual(extraction.metadata["character_count"], 5)


if __name__ == "__main__":
    unittest.main()
