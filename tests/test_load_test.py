import argparse
import tempfile
import unittest
from pathlib import Path

from load_test import positive_int, upload_details


class LoadTestTests(unittest.TestCase):
    def test_positive_int(self):
        self.assertEqual(positive_int("4"), 4)
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int("0")

    def test_upload_details_describe_the_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.pdf"
            path.write_bytes(b"test")
            details = upload_details(path)

        self.assertEqual(details["size"], 4)
        self.assertEqual(details["content_type"], "application/pdf")
        self.assertEqual(details["filename"], "report.pdf")


if __name__ == "__main__":
    unittest.main()
