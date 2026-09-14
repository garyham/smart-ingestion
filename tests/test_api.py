import unittest
from unittest.mock import patch

from fastapi import HTTPException

from api import FileDetails, NotifyRequest, notify, presign, safe_filename


class FakeS3Client:
    def generate_presigned_url(self, operation, Params, ExpiresIn):
        assert operation == "put_object"
        assert Params["Bucket"] == "smart-files"
        assert Params["ContentType"] == "text/plain"
        assert ExpiresIn == 900
        return f"http://127.0.0.1:8333/{Params['Bucket']}/{Params['Key']}?signed=yes"


class ApiTests(unittest.TestCase):
    def test_safe_filename_removes_paths_and_unsafe_characters(self):
        self.assertEqual(safe_filename("../folder/my file.txt"), "my_file.txt")
        self.assertEqual(safe_filename(r"C:\folder\report.pdf"), "report.pdf")

    @patch("api.s3_client", return_value=FakeS3Client())
    def test_presign_returns_upload_details(self, _client):
        response = presign(
            FileDetails(filename="notes.txt", content_type="text/plain", size=12)
        )

        self.assertEqual(response["expires_in"], 900)
        self.assertTrue(str(response["object_key"]).endswith("/notes.txt"))
        self.assertTrue(response["document_id"])
        self.assertIn("?signed=yes", str(response["upload_url"]))

    @patch("api.enqueue_upload")
    def test_notify_queues_file_event(self, enqueue):
        response = notify(
            NotifyRequest(
                filename="notes.txt",
                content_type="text/plain",
                size=12,
                document_id="c63752f4-8d18-4da2-a107-24c52d0707cc",
                object_key="uploads/c63752f4-8d18-4da2-a107-24c52d0707cc/notes.txt",
            )
        )

        self.assertEqual(response["status"], "notified")
        event = enqueue.call_args.args[0]
        self.assertEqual(event["event"], "file.uploaded")
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["document_id"], "c63752f4-8d18-4da2-a107-24c52d0707cc")
        self.assertEqual(
            event["object_key"],
            "uploads/c63752f4-8d18-4da2-a107-24c52d0707cc/notes.txt",
        )

    @patch("api.enqueue_upload")
    def test_notify_rejects_a_mismatched_document_id(self, enqueue):
        with self.assertRaises(HTTPException):
            notify(
                NotifyRequest(
                    filename="notes.txt",
                    content_type="text/plain",
                    size=12,
                    document_id="c63752f4-8d18-4da2-a107-24c52d0707cc",
                    object_key="uploads/a59966fb-c923-41d9-aa88-f088dca07a98/notes.txt",
                )
            )
        enqueue.assert_not_called()


if __name__ == "__main__":
    unittest.main()
