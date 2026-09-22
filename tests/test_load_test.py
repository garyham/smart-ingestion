import argparse
import tempfile
import unittest
from pathlib import Path

from load_test import make_notification, positive_int


class LoadTestTests(unittest.TestCase):
    def test_positive_int(self):
        self.assertEqual(positive_int("4"), 4)
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int("0")

    def test_make_notification_describes_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.pdf"
            path.write_bytes(b"test")
            notification = make_notification(
                path,
                "c63752f4-8d18-4da2-a107-24c52d0707cc",
                "uploads/c63752f4-8d18-4da2-a107-24c52d0707cc/report.pdf",
            )

        self.assertEqual(notification["size"], 4)
        self.assertEqual(notification["content_type"], "application/pdf")
        self.assertEqual(
            notification["document_id"], "c63752f4-8d18-4da2-a107-24c52d0707cc"
        )


if __name__ == "__main__":
    unittest.main()
