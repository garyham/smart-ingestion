import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy.exc import OperationalError

from api import (
    FileDetails,
    NotifyRequest,
    QueryRequest,
    delete_document,
    list_documents,
    lookup_document,
    notify,
    presign,
    query_chunks,
    search,
    search_release,
)
from contracts.chunks import Chunk
from contracts.embeddings import SearchHit
from contracts.refs import DocumentRef, UploadRef
from releases import Release
from store.service import (
    DocumentInfo,
    UnknownDocument,
    UploadInfo,
    UploadNotWritten,
)

DOCUMENT_ID = UUID("c63752f4-8d18-4da2-a107-24c52d0707cc")
EXTRACTION_ID = UUID("0d6f4c1e-7b2a-4f3e-9c8d-5a1b2c3d4e5f")
CANDIDATE_ID = UUID("38e00553-d6cc-4bf7-9293-27bb86563c36")
CONTENT_ID = "a" * 64
CONTENT = b"document contents"

RELEASE = Release(
    name="release-1", extractor="tika@1", chunker="chunker@2", embedders=["hybrid@1"]
)

UPLOAD = UploadRef(
    document_id=CANDIDATE_ID,
    upload_url="http://seaweedfs/smart-files/documents/key?signed=yes",
    upload_headers={"Content-Type": "text/plain", "If-None-Match": "*"},
    expires_at=datetime.now(UTC) + timedelta(minutes=15),
)
DOCUMENT = DocumentRef(document_id=DOCUMENT_ID, content_id=CONTENT_ID)

class PresignTests(unittest.TestCase):
    @patch("api.documents")
    def test_hands_out_a_candidate_and_the_headers_it_must_send(self, make_documents):
        make_documents.return_value.create_upload.return_value = UPLOAD

        response = presign(
            FileDetails(filename="notes.txt", content_type="text/plain", size=12)
        )

        self.assertEqual(response["document_id"], str(CANDIDATE_ID))
        self.assertIn("?signed=yes", str(response["upload_url"]))
        self.assertEqual(response["upload_headers"]["If-None-Match"], "*")

    def test_an_empty_file_is_refused(self):
        with self.assertRaises(ValueError):
            FileDetails(filename="notes.txt", content_type="text/plain", size=0)

    @patch("api.documents")
    def test_does_not_build_any_object_key_itself(self, make_documents):
        make_documents.return_value.create_upload.return_value = UPLOAD

        response = presign(
            FileDetails(filename="notes.txt", content_type="text/plain", size=12)
        )

        self.assertNotIn("object_key", response)


class LookupTests(unittest.TestCase):
    @patch("api.documents")
    def test_reports_bytes_that_are_already_a_document(self, make_documents):
        make_documents.return_value.lookup.return_value = DOCUMENT

        response = lookup_document(sha256=CONTENT_ID)

        make_documents.return_value.lookup.assert_called_once_with(CONTENT_ID)
        self.assertTrue(response["known"])
        self.assertEqual(response["document_id"], str(DOCUMENT_ID))

    @patch("api.documents")
    def test_reports_bytes_that_have_not_been_seen(self, make_documents):
        make_documents.return_value.lookup.return_value = None

        response = lookup_document(sha256=CONTENT_ID)

        self.assertEqual(response, {"known": False})

    @patch("api.documents")
    def test_answers_nothing_but_the_question(self, make_documents):
        # A hash is not proof of possession, so a hit must not hand out the bytes or
        # anywhere they can be read from.
        make_documents.return_value.lookup.return_value = DOCUMENT

        response = lookup_document(sha256=CONTENT_ID)

        self.assertNotIn("object_key", response)
        self.assertNotIn("bucket", response)
        self.assertNotIn("upload_url", response)


def upload_info(size=None, uploaded_at=None):
    return UploadInfo(
        document_id=CANDIDATE_ID,
        filename="notes.txt",
        content_type="text/plain",
        size=len(CONTENT) if size is None else size,
        uploaded_at=uploaded_at or datetime.now(UTC),
    )


class NotifyTests(unittest.TestCase):
    def setUp(self):
        patcher = patch("api.documents")
        self.store = patcher.start().return_value
        self.addCleanup(patcher.stop)
        self.store.document_ref.side_effect = UnknownDocument("not yet")
        self.store.describe_upload.return_value = upload_info()

    @patch("api.active_release", return_value=RELEASE)
    @patch("api.process_upload.delay")
    def test_queues_the_candidate_without_reading_its_bytes(self, delay, _release):
        delay.return_value.task_run_id = "prefect-task-run"

        response = notify(NotifyRequest(document_id=CANDIDATE_ID))

        self.store.open_upload.assert_not_called()
        self.store.add_document.assert_not_called()
        event = delay.call_args.args[0]
        self.assertEqual(event["event"], "upload.notified")
        self.assertEqual(event["schema_version"], 4)
        self.assertEqual(event["release"], "release-1")
        self.assertEqual(event["document_id"], str(CANDIDATE_ID))
        self.assertNotIn("object_key", event)
        self.assertNotIn("bucket", event)
        self.assertEqual(response["document_id"], str(CANDIDATE_ID))
        self.assertEqual(response["task_run_id"], "prefect-task-run")

    @patch("api.active_release", return_value=RELEASE)
    @patch("api.process_upload.delay")
    def test_a_committed_candidate_is_queued_again(self, delay, _release):
        self.store.document_ref.side_effect = None
        self.store.document_ref.return_value = DocumentRef(
            document_id=CANDIDATE_ID, content_id=CONTENT_ID
        )

        notify(NotifyRequest(document_id=CANDIDATE_ID))

        self.store.describe_upload.assert_not_called()
        delay.assert_called_once()

    @patch("api.process_upload.delay")
    def test_rejects_an_upload_too_old_to_commit(self, delay):
        self.store.describe_upload.return_value = upload_info(
            uploaded_at=datetime.now(UTC) - timedelta(days=1)
        )

        with self.assertRaises(HTTPException) as raised:
            notify(NotifyRequest(document_id=CANDIDATE_ID))

        self.assertEqual(raised.exception.status_code, 400)
        delay.assert_not_called()

    @patch("api.process_upload.delay")
    def test_rejects_an_empty_upload(self, delay):
        self.store.describe_upload.return_value = upload_info(size=0)

        with self.assertRaises(HTTPException) as raised:
            notify(NotifyRequest(document_id=CANDIDATE_ID))

        self.assertEqual(raised.exception.status_code, 400)
        delay.assert_not_called()

    @patch("api.process_upload.delay")
    def test_reports_an_upload_that_never_arrived(self, delay):
        self.store.describe_upload.side_effect = UploadNotWritten("nothing")

        with self.assertRaises(HTTPException) as raised:
            notify(NotifyRequest(document_id=CANDIDATE_ID))

        self.assertEqual(raised.exception.status_code, 404)
        delay.assert_not_called()


def hit(chunk_id, score):
    return SearchHit(
        chunk=Chunk(
            chunk_id=chunk_id,
            extraction_id=EXTRACTION_ID,
            document_id=DOCUMENT_ID,
            extractor="tika@1",
            chunker="chunker@2",
            index=0,
            text="first",
        ),
        document_name="notes",
        score=score,
    )


class QueryTests(unittest.TestCase):
    @patch("api.active_release", return_value=RELEASE)
    @patch("api.search", return_value=[])
    def test_searches_the_active_release_by_default(self, search, _release):
        query_chunks(QueryRequest(query="a question", limit=3))

        search.assert_called_once_with("a question", "hybrid@1", 3)

    @patch("api.search", return_value=[])
    def test_searches_one_named_embedder(self, search):
        query_chunks(QueryRequest(query="a question", limit=3, embedder="hybrid@1"))

        search.assert_called_once_with("a question", "hybrid@1", 3)

    @patch("api.documents")
    @patch("api.embedders.get")
    def test_the_embedder_embeds_the_query_and_the_store_searches(
        self, get_embedder, make_documents
    ):
        embedder = get_embedder.return_value
        make_documents.return_value.search_embeddings.return_value = []

        search("a question", "hybrid@1", 5)

        get_embedder.assert_called_once_with("hybrid@1")
        embedder.embed_query.assert_called_once_with("a question")
        embedder.search_options.assert_called_once_with(5)
        make_documents.return_value.search_embeddings.assert_called_once_with(
            embedder.embed_query.return_value, embedder.search_options.return_value
        )

    @patch("api.search")
    def test_two_embedders_do_not_return_the_same_chunk_twice(self, search):
        chunk_id = uuid4()
        search.side_effect = lambda _text, embedder, _limit: [
            hit(chunk_id, 0.02 if embedder == "hybrid@2" else 0.01)
        ]
        release = Release(
            name="release-both",
            extractor="tika@1",
            chunker="chunker@2",
            embedders=["hybrid@1", "hybrid@2"],
        )

        results = search_release(release, "a question", 3)

        # Both embedders indexed the same chunk; it appears once, at its best score.
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].score, 0.02)

    def test_an_unknown_embedder_is_not_found(self):
        with self.assertRaises(HTTPException) as raised:
            query_chunks(QueryRequest(query="a question", embedder="hybrid@99"))

        self.assertEqual(raised.exception.status_code, 404)


class ListDocumentsTests(unittest.TestCase):
    @patch("api.documents")
    def test_lists_documents_through_the_store(self, make_documents):
        info = DocumentInfo(
            document_id=DOCUMENT_ID,
            content_id=CONTENT_ID,
            filename="notes.txt",
            content_type="text/plain",
            size=12,
            status="ok",
            published=True,
            created_at=datetime.now(UTC),
        )
        make_documents.return_value.list_documents.return_value = [info]

        response = list_documents(limit=20, offset=40)

        self.assertEqual(response, [info])
        make_documents.return_value.list_documents.assert_called_once_with(
            limit=20, offset=40
        )

    @patch("api.documents")
    def test_a_database_failure_is_a_503(self, make_documents):
        make_documents.return_value.list_documents.side_effect = OperationalError(
            "select", {}, Exception("down")
        )

        with self.assertRaises(HTTPException) as raised:
            list_documents()

        self.assertEqual(raised.exception.status_code, 503)


class DeleteDocumentTests(unittest.TestCase):
    @patch("api.documents")
    def test_deletes_the_document_through_its_service(self, make_documents):
        delete_document(DOCUMENT_ID)

        make_documents.return_value.delete_document.assert_called_once_with(
            DOCUMENT_ID
        )

    @patch("api.documents")
    def test_an_unknown_document_is_a_404(self, make_documents):
        make_documents.return_value.delete_document.side_effect = UnknownDocument("x")

        with self.assertRaises(HTTPException) as raised:
            delete_document(DOCUMENT_ID)

        self.assertEqual(raised.exception.status_code, 404)

    @patch("api.documents")
    def test_a_failed_delete_is_reported_and_can_be_retried(self, make_documents):
        make_documents.return_value.delete_document.side_effect = RuntimeError("s3")

        with self.assertRaises(HTTPException) as raised:
            delete_document(DOCUMENT_ID)

        self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
