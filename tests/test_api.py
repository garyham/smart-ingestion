import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import UUID

from fastapi import HTTPException

from api import (
    FileDetails,
    NotifyRequest,
    QueryRequest,
    notify,
    presign,
    query_chunks,
    query_ingestions,
    read_ingestion,
    safe_filename,
)
from ingestion.schemas import IngestionRead, IngestionStatus

INGESTION = IngestionRead(
    ingestion_id="38e00553-d6cc-4bf7-9293-27bb86563c36",
    document_id="c63752f4-8d18-4da2-a107-24c52d0707cc",
    source_sha256="a" * 64,
    pipeline_version="1",
    status="completed",
    current_step="embedding",
    source={"filename": "notes.txt"},
    steps={},
    outputs={},
    error=None,
    created_at=datetime.now(UTC),
    updated_at=datetime.now(UTC),
    completed_at=datetime.now(UTC),
)


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

    @patch("api.process_upload.delay")
    def test_notify_queues_file_event(self, delay):
        delay.return_value.task_run_id = "prefect-task-run"
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
        event = delay.call_args.args[0]
        self.assertEqual(event["event"], "file.uploaded")
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["document_id"], "c63752f4-8d18-4da2-a107-24c52d0707cc")
        self.assertEqual(
            event["object_key"],
            "uploads/c63752f4-8d18-4da2-a107-24c52d0707cc/notes.txt",
        )
        self.assertEqual(response["task_run_id"], "prefect-task-run")

    @patch("api.process_upload.delay")
    def test_notify_rejects_a_mismatched_document_id(self, delay):
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
        delay.assert_not_called()

    @patch("api.hybrid_search")
    @patch("api.generate_query_embeddings", return_value=([0.1], {"indices": []}))
    @patch("api.available_embedding_providers", return_value=("CPUExecutionProvider",))
    @patch("api.configured_models", return_value=("dense", "sparse", "cpu"))
    def test_query_combines_dense_and_sparse_results(
        self, _models, _providers, _generate, search
    ):
        search.return_value = [{"content": "matching chunk"}]

        response = query_chunks(QueryRequest(query="test query"))

        self.assertEqual(response["results"][0]["content"], "matching chunk")
        search.assert_called_once_with([0.1], {"indices": []}, "dense", "sparse", 3)

    @patch("api.list_ingestions", return_value=[INGESTION])
    def test_query_ingestions_supports_filters(self, list_records):
        result = query_ingestions(
            status_filter=IngestionStatus.COMPLETED,
            source_sha256="a" * 64,
            document_id=INGESTION.document_id,
            pipeline_version="1",
            limit=20,
            offset=10,
        )

        self.assertEqual(result, [INGESTION])
        list_records.assert_called_once_with(
            status=IngestionStatus.COMPLETED,
            source_sha256="a" * 64,
            document_id=INGESTION.document_id,
            pipeline_version="1",
            limit=20,
            offset=10,
        )

    @patch("api.get_ingestion", return_value=INGESTION)
    def test_read_ingestion_returns_record(self, get_record):
        ingestion_id = UUID("38e00553-d6cc-4bf7-9293-27bb86563c36")

        result = read_ingestion(ingestion_id)

        self.assertEqual(result, INGESTION)
        get_record.assert_called_once_with(ingestion_id)

    @patch("api.get_ingestion", return_value=None)
    def test_read_ingestion_returns_not_found(self, _get_record):
        with self.assertRaises(HTTPException) as raised:
            read_ingestion(UUID("38e00553-d6cc-4bf7-9293-27bb86563c36"))

        self.assertEqual(raised.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
