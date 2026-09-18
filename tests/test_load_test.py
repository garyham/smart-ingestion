import argparse
import tempfile
import unittest
from pathlib import Path

from load_test import make_event, positive_int


class LoadTestTests(unittest.TestCase):
    def test_positive_int(self):
        self.assertEqual(positive_int("4"), 4)
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int("0")

    def test_make_event_uses_unique_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.pdf"
            path.write_bytes(b"test")
            first = make_event(path, "uploads/test/report.pdf")
            second = make_event(path, "uploads/test/report.pdf")

        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["document_id"], second["document_id"])
        self.assertEqual(first["size"], 4)
        self.assertEqual(first["content_type"], "application/pdf")


if __name__ == "__main__":
    unittest.main()
