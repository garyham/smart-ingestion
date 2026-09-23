import hashlib
import io
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from botocore.exceptions import ClientError

from contracts.chunks import ChunkDraft
from contracts.embeddings import Embedding, SearchOptions, SparseVector
from contracts.extractions import Extraction
from contracts.operators import OperatorRef
from contracts.refs import DocumentStatus, ExtractionRef
from store.objects import (
    document_id_of,
    document_key,
    document_prefixes,
    extraction_text_key,
)
from store.repository import DocumentRecord, ExtractionRecord
from store.service import (
    DocumentStore,
    UnknownDocument,
    UploadNotWritten,
    UploadRequest,
)

DOCUMENT_ID = UUID("c63752f4-8d18-4da2-a107-24c52d0707cc")
CANDIDATE_ID = UUID("38e00553-d6cc-4bf7-9293-27bb86563c36")
EXTRACTION_ID = UUID("0d6f4c1e-7b2a-4f3e-9c8d-5a1b2c3d4e5f")
EXTRACTION_REF = ExtractionRef(
    extraction_id=EXTRACTION_ID,
    document_id=DOCUMENT_ID,
    extractor="tika@1",
    mime_type="text/plain",
)
CONTENT = b"document contents"
CONTENT_ID = hashlib.sha256(CONTENT).hexdigest()


class FakeS3Client:
    def __init__(self, objects=None, written_at=None):
        self.objects = dict(objects or {})
        self.written_at = written_at or datetime.now(UTC)
        self.presigned = []
        self.reads = []
        self.deleted = []

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.presigned.append(Params)
        return f"http://seaweedfs/{Params['Bucket']}/{Params['Key']}?signed=yes"

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        self.reads.append(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {
            "ContentLength": len(self.objects[(Bucket, Key)]),
            "ContentType": "text/plain",
            "LastModified": self.written_at,
        }

    def get_paginator(self, operation):
        client = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                keys = [
                    key
                    for bucket, key in client.objects
                    if bucket == Bucket and key.startswith(Prefix)
                ]
                contents = [
                    {"Key": key, "LastModified": client.written_at} for key in keys
                ]
                yield {"Contents": contents} if keys else {}

        return Paginator()

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = Body

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.deleted.append(item["Key"])
            self.objects.pop((Bucket, item["Key"]), None)
        return {}


def candidate_key(document_id=CANDIDATE_ID):
    return document_key(document_id, "notes.txt")


def document_record(content_id=CONTENT_ID, document_id=DOCUMENT_ID):
    return DocumentRecord(
        document_id=document_id,
        content_id=content_id,
        title="notes.txt",
        bucket="smart-files",
        object_key=document_key(document_id, "notes.txt"),
        filename="notes.txt",
        content_type="text/plain",
        size=len(CONTENT),
        status="uploaded",
        error=None,
        published=False,
        created_at=datetime.now(UTC),
    )


class ObjectKeyTests(unittest.TestCase):
    def test_a_candidate_uploads_to_its_own_document_key(self):
        self.assertEqual(
            candidate_key(), f"documents/{CANDIDATE_ID}/notes.txt"
        )

    def test_a_document_owns_its_source_prefix_extractions_and_bundles(self):
        self.assertEqual(
            document_prefixes(DOCUMENT_ID),
            [
                f"documents/{DOCUMENT_ID}/",
                f"extractions/{DOCUMENT_ID}/",
                f"ingested/{DOCUMENT_ID}/",
            ],
        )

    def test_an_extraction_is_kept_outside_the_source_prefix(self):
        # The source prefix holds nothing but the uploaded bytes: a candidate's upload is
        # whatever single key is found there.
        self.assertEqual(
            extraction_text_key(DOCUMENT_ID, EXTRACTION_ID),
            f"extractions/{DOCUMENT_ID}/{EXTRACTION_ID}.txt",
        )

    def test_keys_are_built_from_safe_filenames(self):
        self.assertTrue(
            document_key(DOCUMENT_ID, "../secrets/my file.txt").endswith(
                "/my_file.txt"
            )
        )

    def test_a_source_key_names_its_document(self):
        self.assertEqual(document_id_of(candidate_key()), CANDIDATE_ID)
        self.assertIsNone(document_id_of(f"ingested/{CANDIDATE_ID}/x"))
        self.assertIsNone(document_id_of("documents/not-a-uuid/x"))


class CreateUploadTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.service = DocumentStore(client=self.client)
        self.request = UploadRequest(
            filename="notes.txt", content_type="text/plain", size=17
        )

    @patch("store.service.repository")
    def test_records_nothing(self, repository):
        self.service.create_upload(self.request)

        self.assertEqual(repository.mock_calls, [])

    def test_signs_one_write_of_the_declared_size_to_the_candidates_key(self):
        upload = self.service.create_upload(self.request)

        [params] = self.client.presigned
        self.assertEqual(params["Key"], document_key(upload.document_id, "notes.txt"))
        self.assertEqual(params["ContentLength"], 17)
        self.assertEqual(params["IfNoneMatch"], "*")
        self.assertEqual(
            upload.upload_headers,
            {"Content-Type": "text/plain", "If-None-Match": "*"},
        )
        self.assertIn("?signed=yes", upload.upload_url)

    def test_each_upload_is_a_distinct_candidate(self):
        first = self.service.create_upload(self.request)
        second = self.service.create_upload(self.request)

        self.assertNotEqual(first.document_id, second.document_id)

    def test_an_empty_upload_is_refused_before_it_is_signed(self):
        with self.assertRaises(ValueError):
            UploadRequest(filename="notes.txt", content_type="text/plain", size=0)


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3Client({("smart-files", candidate_key()): CONTENT})
        self.service = DocumentStore(client=self.client)

    def test_describes_an_upload_from_its_object_without_leaking_a_key(self):
        info = self.service.describe_upload(CANDIDATE_ID)

        self.assertEqual(info.document_id, CANDIDATE_ID)
        self.assertEqual(info.filename, "notes.txt")
        self.assertEqual(info.content_type, "text/plain")
        self.assertEqual(info.size, len(CONTENT))
        self.assertNotIn("object_key", info.model_dump())
        self.assertEqual(self.client.reads, [])

    def test_opens_the_bytes_the_client_wrote(self):
        with self.service.open_upload(CANDIDATE_ID) as body:
            self.assertEqual(body.read(), CONTENT)

    def test_an_upload_that_never_arrived_has_nothing_to_open(self):
        self.client.objects.clear()

        with self.assertRaises(UploadNotWritten):
            self.service.open_upload(CANDIDATE_ID)
        with self.assertRaises(UploadNotWritten):
            self.service.describe_upload(CANDIDATE_ID)


class AddDocumentTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3Client({("smart-files", candidate_key()): CONTENT})
        self.service = DocumentStore(client=self.client)

    @patch("store.service.repository.add_document")
    def test_a_new_document_is_the_candidate_and_its_bytes_stay_put(
        self, add_document
    ):
        add_document.return_value = document_record(document_id=CANDIDATE_ID)

        ref = self.service.add_document(
            CANDIDATE_ID, content_id=CONTENT_ID, size=len(CONTENT)
        )

        self.assertEqual(ref.document_id, CANDIDATE_ID)
        self.assertEqual(add_document.call_args.args, (CANDIDATE_ID,))
        self.assertEqual(add_document.call_args.kwargs["content_id"], CONTENT_ID)
        self.assertEqual(add_document.call_args.kwargs["object_key"], candidate_key())
        self.assertEqual(add_document.call_args.kwargs["filename"], "notes.txt")
        self.assertEqual(self.client.deleted, [])
        self.assertIn(("smart-files", candidate_key()), self.client.objects)

    @patch("store.service.repository.add_document")
    def test_content_seen_before_discards_the_candidate(self, add_document):
        add_document.return_value = document_record()

        ref = self.service.add_document(
            CANDIDATE_ID, content_id=CONTENT_ID, size=len(CONTENT)
        )

        self.assertEqual(ref.document_id, DOCUMENT_ID)
        self.assertEqual(self.client.deleted, [candidate_key()])
        self.assertEqual(self.client.objects, {})

    @patch("store.service.repository.add_document")
    def test_failing_to_discard_a_duplicate_does_not_fail_the_commit(
        self, add_document
    ):
        add_document.return_value = document_record()
        self.client.delete_objects = lambda Bucket, Delete: {"Errors": [{"Key": "x"}]}

        with self.assertLogs("store.service", level="WARNING"):
            ref = self.service.add_document(
                CANDIDATE_ID, content_id=CONTENT_ID, size=len(CONTENT)
            )

        self.assertEqual(ref.document_id, DOCUMENT_ID)


class AbandonedUploadTests(unittest.TestCase):
    def setUp(self):
        self.written_at = datetime(2026, 9, 1, tzinfo=UTC)
        self.client = FakeS3Client(
            {
                ("smart-files", candidate_key()): CONTENT,
                ("smart-files", document_key(DOCUMENT_ID, "notes.txt")): CONTENT,
                ("smart-files", f"ingested/{uuid4()}/manifest.json"): b"{}",
            },
            written_at=self.written_at,
        )
        self.service = DocumentStore(client=self.client)

    @patch("store.service.repository.existing_document_ids")
    def test_removes_old_candidates_that_never_became_a_document(self, existing):
        existing.return_value = {DOCUMENT_ID}

        discarded = self.service.discard_abandoned_uploads(
            self.written_at + timedelta(seconds=1)
        )

        self.assertEqual(discarded, [CANDIDATE_ID])
        self.assertEqual(existing.call_args.args[0], {CANDIDATE_ID, DOCUMENT_ID})
        self.assertEqual(self.client.deleted, [candidate_key()])

    @patch("store.service.repository.existing_document_ids", return_value=set())
    def test_leaves_recent_uploads_alone(self, _existing):
        discarded = self.service.discard_abandoned_uploads(self.written_at)

        self.assertEqual(discarded, [])
        self.assertEqual(self.client.deleted, [])


class DeleteTests(unittest.TestCase):
    def setUp(self):
        self.document = document_record()
        self.client = FakeS3Client(
            {
                ("smart-files", self.document.object_key): CONTENT,
                ("smart-files", f"ingested/{DOCUMENT_ID}/run-1/manifest.json"): b"{}",
                ("smart-files", f"ingested/{DOCUMENT_ID}/run-1/status.json"): b"{}",
                ("smart-files", f"extractions/{DOCUMENT_ID}/{EXTRACTION_ID}.txt"): b"x",
                ("smart-files", "documents/someone-else/notes.txt"): CONTENT,
            }
        )
        self.service = DocumentStore(client=self.client)

    def deleting(self, found=True):
        def delete_document(document_id, remove_objects):
            if found:
                remove_objects(self.document)
            return found

        return patch(
            "store.service.repository.delete_document",
            side_effect=delete_document,
        )

    def test_removes_every_object_the_document_owns(self):
        with self.deleting():
            self.service.delete_document(DOCUMENT_ID)

        self.assertEqual(
            set(self.client.objects),
            {("smart-files", "documents/someone-else/notes.txt")},
        )

    def test_objects_are_removed_inside_the_transaction(self):
        # A failure while removing objects escapes before the rows commit, so the
        # document is still there to delete again.
        self.client.delete_objects = lambda Bucket, Delete: {
            "Errors": [{"Key": "x", "Code": "InternalError"}]
        }

        with self.deleting(), self.assertRaises(RuntimeError):
            self.service.delete_document(DOCUMENT_ID)

    def test_deleting_an_unknown_document_is_an_error(self):
        with self.deleting(found=False), self.assertRaises(UnknownDocument):
            self.service.delete_document(DOCUMENT_ID)


class LookupTests(unittest.TestCase):
    @patch("store.service.repository.find_by_content")
    def test_finds_the_document_the_bytes_already_are(self, find_by_content):
        find_by_content.return_value = document_record()

        ref = DocumentStore(client=FakeS3Client()).lookup(CONTENT_ID)

        assert ref is not None
        self.assertEqual(ref.document_id, DOCUMENT_ID)
        self.assertEqual(ref.content_id, CONTENT_ID)
        find_by_content.assert_called_once_with(CONTENT_ID)

    @patch("store.service.repository.find_by_content", return_value=None)
    def test_unseen_bytes_are_no_document(self, _find_by_content):
        self.assertIsNone(
            DocumentStore(client=FakeS3Client()).lookup(CONTENT_ID)
        )

    @patch("store.service.repository.find_by_content")
    def test_reads_no_bytes_to_answer(self, find_by_content):
        # The caller supplies the hash, so the question costs nothing but a row read.
        find_by_content.return_value = document_record()
        client = FakeS3Client()

        DocumentStore(client=client).lookup(CONTENT_ID)

        self.assertEqual(client.reads, [])


class ReadTests(unittest.TestCase):
    @patch("store.service.repository.get_document")
    def test_describe_reports_the_document_without_leaking_a_key(self, get_document):
        get_document.return_value = document_record()
        service = DocumentStore(client=FakeS3Client())

        info = service.describe(DOCUMENT_ID)

        self.assertEqual(info.document_id, DOCUMENT_ID)
        self.assertEqual(info.content_id, CONTENT_ID)
        self.assertEqual(info.filename, "notes.txt")
        self.assertNotIn("object_key", info.model_dump())

    @patch("store.service.repository.list_documents")
    def test_lists_documents_without_leaking_a_key(self, list_documents):
        list_documents.return_value = [document_record()]
        service = DocumentStore(client=FakeS3Client())

        infos = service.list_documents(limit=10, offset=5)

        list_documents.assert_called_once_with(limit=10, offset=5)
        self.assertEqual([info.document_id for info in infos], [DOCUMENT_ID])
        self.assertNotIn("object_key", infos[0].model_dump())
        self.assertNotIn("bucket", infos[0].model_dump())

    @patch("store.service.repository.get_document")
    def test_download_writes_the_bytes_to_a_path(self, get_document):
        document = document_record()
        get_document.return_value = document
        client = FakeS3Client({("smart-files", document.object_key): CONTENT})
        service = DocumentStore(client=client)

        with tempfile.TemporaryDirectory() as directory:
            destination = service.download(DOCUMENT_ID, Path(directory) / "notes.txt")
            self.assertEqual(destination.read_bytes(), CONTENT)


class StatusTests(unittest.TestCase):
    @patch("store.service.repository.set_status", return_value=True)
    def test_records_a_status_and_its_error(self, set_status):
        DocumentStore(client=FakeS3Client()).set_status(
            DOCUMENT_ID, DocumentStatus.FAILED, {"detail": "tika down"}
        )

        set_status.assert_called_once_with(
            DOCUMENT_ID, "failed", {"detail": "tika down"}
        )

    def test_rejects_a_status_that_is_not_one(self):
        with self.assertRaises(ValueError):
            DocumentStore(client=FakeS3Client()).set_status(
                DOCUMENT_ID,
                "done",  # pyright: ignore[reportArgumentType]
            )

    @patch("store.service.repository.mark_published", return_value=True)
    def test_publishes_a_document(self, mark_published):
        DocumentStore(client=FakeS3Client()).mark_published(DOCUMENT_ID)

        mark_published.assert_called_once_with(DOCUMENT_ID)


class FakeExtractions:
    """The extraction rows, kept in memory, with the reuse key the table enforces."""

    def __init__(self):
        self.rows = {}

    def find_extraction(self, document_id, extractor):
        return self.rows.get((document_id, str(extractor)))

    def get_extraction(self, extraction_id):
        return next(
            (row for row in self.rows.values() if row.extraction_id == extraction_id),
            None,
        )

    def add_extraction(self, extraction_id, document_id, extractor, *, mime_type, metadata):
        return self.rows.setdefault(
            (document_id, str(extractor)),
            ExtractionRecord(
                extraction_id=extraction_id,
                document_id=document_id,
                extractor_name=extractor.name,
                extractor_version=extractor.version,
                mime_type=mime_type,
                document_metadata=metadata,
                created_at=datetime.now(UTC),
            ),
        )


class ExtractionTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.store = DocumentStore(client=self.client)
        self.extractions = FakeExtractions()
        for name, value in (
            ("get_document", Mock(return_value=document_record())),
            ("find_extraction", self.extractions.find_extraction),
            ("get_extraction", self.extractions.get_extraction),
            ("add_extraction", self.extractions.add_extraction),
        ):
            patcher = patch(f"store.service.repository.{name}", value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.extraction = Extraction(
            mime_type="text/plain", text="hello", metadata={"title": "Notes"}
        )

    def test_nothing_extracted_has_nothing_to_reuse(self):
        self.assertIsNone(self.store.find_extraction(DOCUMENT_ID, "tika@1"))

    def test_keeps_an_extraction_and_hands_back_a_reference(self):
        ref = self.store.add_extraction(DOCUMENT_ID, "tika@1", self.extraction)

        self.assertEqual(ref.extractor, "tika@1")
        self.assertEqual(ref.mime_type, "text/plain")
        self.assertFalse(ref.reused)
        self.assertEqual(self.store.extracted_text(ref), "hello")
        self.assertEqual(self.store.extraction_metadata(ref), {"title": "Notes"})

    def test_the_text_is_kept_under_the_extraction_id(self):
        ref = self.store.add_extraction(DOCUMENT_ID, "tika@1", self.extraction)

        key = extraction_text_key(DOCUMENT_ID, ref.extraction_id)
        self.assertEqual(self.client.objects[("smart-files", key)], b"hello")

    def test_metadata_is_kept_as_json(self):
        written = datetime(2026, 9, 23, tzinfo=UTC)
        extraction = self.extraction.model_copy(update={"metadata": {"date": written}})

        ref = self.store.add_extraction(DOCUMENT_ID, "tika@1", extraction)

        self.assertEqual(
            self.store.extraction_metadata(ref), {"date": str(written)}
        )

    def test_an_extraction_already_kept_is_reused(self):
        kept = self.store.add_extraction(DOCUMENT_ID, "tika@1", self.extraction)

        ref = self.store.find_extraction(DOCUMENT_ID, "tika@1")

        assert ref is not None
        self.assertTrue(ref.reused)
        self.assertEqual(ref.extraction_id, kept.extraction_id)
        self.assertEqual(ref.mime_type, "text/plain")
        self.assertIsNone(self.store.find_extraction(DOCUMENT_ID, "tika@2"))

    def test_a_run_that_loses_the_race_removes_its_text_and_returns_the_winner(self):
        winner = self.store.add_extraction(DOCUMENT_ID, "tika@1", self.extraction)

        loser = self.store.add_extraction(
            DOCUMENT_ID, "tika@1", self.extraction.model_copy(update={"text": "other"})
        )

        self.assertEqual(loser.extraction_id, winner.extraction_id)
        self.assertTrue(loser.reused)
        self.assertEqual(self.store.extracted_text(loser), "hello")
        self.assertEqual(
            [key for _, key in self.client.objects if key.startswith("extractions/")],
            [extraction_text_key(DOCUMENT_ID, winner.extraction_id)],
        )

    def test_a_row_is_only_written_once_its_text_is(self):
        self.client.put_object = Mock(side_effect=ClientError({}, "PutObject"))

        with self.assertRaises(ClientError):
            self.store.add_extraction(DOCUMENT_ID, "tika@1", self.extraction)

        self.assertIsNone(self.store.find_extraction(DOCUMENT_ID, "tika@1"))


class ChunkTests(unittest.TestCase):
    def setUp(self):
        self.store = DocumentStore(client=FakeS3Client())

    @patch("store.service.repository.count_chunks", return_value=0)
    def test_an_extraction_with_no_chunks_has_nothing_to_reuse(self, _count):
        self.assertIsNone(self.store.find_chunks(EXTRACTION_REF, "chunker@2"))

    @patch("store.service.repository.count_chunks", return_value=4)
    def test_existing_chunks_are_reused(self, count):
        ref = self.store.find_chunks(EXTRACTION_REF, "chunker@2")

        assert ref is not None
        self.assertTrue(ref.reused)
        self.assertEqual(ref.chunk_count, 4)
        self.assertEqual(ref.extraction_id, EXTRACTION_ID)
        self.assertEqual(ref.document_id, DOCUMENT_ID)
        self.assertEqual(ref.extractor, "tika@1")
        self.assertEqual(ref.chunker, "chunker@2")
        extraction_id, chunker = count.call_args.args
        self.assertEqual(extraction_id, EXTRACTION_ID)
        self.assertEqual(str(chunker), "chunker@2")

    @patch("store.service.repository.add_chunks", return_value=2)
    def test_adds_every_chunk_at_once(self, add_chunks):
        chunks = [ChunkDraft(index=0, text="a"), ChunkDraft(index=1, text="b")]

        ref = self.store.add_chunks(EXTRACTION_REF, "chunker@2", chunks)

        self.assertEqual(ref.chunk_count, 2)
        self.assertFalse(ref.reused)
        extraction_id, chunker, written = add_chunks.call_args.args
        self.assertEqual(extraction_id, EXTRACTION_ID)
        self.assertEqual(str(chunker), "chunker@2")
        self.assertEqual(written, chunks)

    def test_rejects_an_unversioned_operator(self):
        with self.assertRaises(ValueError):
            self.store.find_chunks(EXTRACTION_REF, "chunker")


class EmbeddingTests(unittest.TestCase):
    def setUp(self):
        self.store = DocumentStore(client=FakeS3Client())
        self.embedding = Embedding(
            dense=[0.1], sparse=SparseVector(indices=[0], values=[1.0], dimension=4)
        )

    @patch("store.service.repository.add_embeddings", return_value=1)
    def test_adds_embeddings_under_the_embedder_version(self, add_embeddings):
        chunk_id = uuid4()

        written = self.store.add_embeddings("hybrid@1", {chunk_id: self.embedding})

        self.assertEqual(written, 1)
        operator, embeddings = add_embeddings.call_args.args
        self.assertEqual(str(operator), "hybrid@1")
        self.assertEqual(embeddings, {chunk_id: self.embedding})

    @patch("store.service.repository.unembedded_chunks", return_value=[])
    def test_names_every_version_when_asking_what_is_unembedded(self, unembedded):
        self.store.unembedded_chunks(EXTRACTION_REF, "chunker@2", "hybrid@1")

        extraction_id, chunker, embedder = unembedded.call_args.args
        self.assertEqual(extraction_id, EXTRACTION_ID)
        self.assertEqual(str(chunker), "chunker@2")
        self.assertEqual(str(embedder), "hybrid@1")

    @patch("store.service.vectors.search", return_value=[])
    def test_a_search_runs_no_model_and_passes_the_options_through(self, search):
        options = SearchOptions(
            embedder=OperatorRef.parse("hybrid@1"), candidates=50, fusion_k=60
        )

        self.store.search_embeddings(self.embedding, options)

        search.assert_called_once_with(self.embedding, options)
        self.assertEqual(options.limit, 3)

    def test_search_options_must_name_an_embedder(self):
        with self.assertRaises(ValueError):
            SearchOptions(candidates=50, fusion_k=60)  # pyright: ignore[reportCallIssue]


if __name__ == "__main__":
    unittest.main()
