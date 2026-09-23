"""The flow and task graph: what runs, in what order, and what a failure records.

The flow-level cases run against `prefect_test_harness()`, so the graph is exercised as
Prefect actually runs it - every stage a task of its own - rather than as one opaque call.

Every flow reaches the store through `ingestion.common.document_store()`, so the one
`STORE` patch covers the store for all of them.
"""

import hashlib
import io
import json
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from prefect.testing.utilities import prefect_test_harness

from contracts.chunks import Chunk, ChunkDraft
from contracts.embeddings import Embedding, SparseVector
from contracts.extractions import Extraction
from contracts.refs import (
    ArtifactStatus,
    ChunksRef,
    DocumentRef,
    DocumentStatus,
    EmbeddingsRef,
    ExtractionRef,
)
from ingestion.chunking_flow import ChunkedDocument, chunk_document_flow
from ingestion.embedding_flow import embed_document_flow, embed_release
from ingestion.ingestion_flow import (
    InvalidUpload,
    InvalidUploadEvent,
    _ingest_crashed,
    commit_upload,
    ingest_document_flow,
    parse_upload_event,
    process_upload,
)
from ingestion.sweep_flow import discard_abandoned_uploads_flow
from releases import Release
from store.service import UnknownDocument, UploadInfo, UploadNotWritten

DOCUMENT_ID = UUID("c63752f4-8d18-4da2-a107-24c52d0707cc")
EVENT_ID = "38e00553-d6cc-4bf7-9293-27bb86563c36"
CANDIDATE_ID = UUID("5b0e8a4f-2f1c-4b8e-9a52-0c6f3d7e1a90")
CONTENT = b"document contents"
DOCUMENT = DocumentRef(document_id=DOCUMENT_ID, content_id="a" * 64)

RELEASE = Release(
    name="release-1", extractor="tika@1", chunker="chunker@2", embedders=["hybrid@1"]
)

EVENT = {
    "event_id": EVENT_ID,
    "event": "upload.notified",
    "schema_version": 4,
    "document_id": str(CANDIDATE_ID),
    "release": "release-1",
}

EXTRACTION_ID = UUID("0d6f4c1e-7b2a-4f3e-9c8d-5a1b2c3d4e5f")
CHUNKS_REF = ChunksRef(
    extraction_id=EXTRACTION_ID,
    document_id=DOCUMENT_ID,
    extractor="tika@1",
    chunker="chunker@2",
    chunk_count=3,
)
EXTRACTION_REF = ExtractionRef(
    extraction_id=EXTRACTION_ID,
    document_id=DOCUMENT_ID,
    extractor="tika@1",
    mime_type="text/markdown",
)

CHUNKED = ChunkedDocument(status=ArtifactStatus.OK, chunks=CHUNKS_REF)

STORE = "ingestion.common.DocumentStore"

_harness = None


def setUpModule():
    """One temporary Prefect server for every flow-level case in this module."""
    global _harness
    _harness = prefect_test_harness()
    _harness.__enter__()


def tearDownModule():
    assert _harness is not None
    _harness.__exit__(None, None, None)


class ParseEventTests(unittest.TestCase):
    def test_parses_a_notified_upload(self):
        self.assertEqual(parse_upload_event(json.dumps(EVENT).encode()), EVENT)

    def test_rejects_messages_it_cannot_act_on(self):
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b"not JSON")
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event(b'{"event": "something.else"}')
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event({**EVENT, "schema_version": 3})
        with self.assertRaises(InvalidUploadEvent):
            parse_upload_event({**EVENT, "document_id": "not-a-uuid"})


@patch(STORE)
class CommitUploadTests(unittest.TestCase):
    def upload(self, store, body=CONTENT, uploaded_at=None):
        service = store.return_value
        service.document_ref.side_effect = UnknownDocument("not yet")
        service.describe_upload.return_value = UploadInfo(
            document_id=CANDIDATE_ID,
            filename="notes.txt",
            content_type="text/plain",
            size=len(body),
            uploaded_at=uploaded_at or datetime.now(UTC),
        )
        service.open_upload.side_effect = lambda _id: io.BytesIO(body)
        service.add_document.return_value = DOCUMENT
        return service

    def test_hashes_what_arrived_and_records_it_as_that_document(self, store):
        service = self.upload(store)

        ref = commit_upload.fn(CANDIDATE_ID)

        self.assertEqual(ref, DOCUMENT)
        service.add_document.assert_called_once_with(
            CANDIDATE_ID,
            content_id=hashlib.sha256(CONTENT).hexdigest(),
            size=len(CONTENT),
        )

    def test_committing_twice_returns_the_same_document(self, store):
        service = self.upload(store)
        service.document_ref.side_effect = None
        service.document_ref.return_value = DOCUMENT

        ref = commit_upload.fn(CANDIDATE_ID)

        self.assertEqual(ref, DOCUMENT)
        service.open_upload.assert_not_called()
        service.add_document.assert_not_called()

    def test_rejects_an_upload_too_old_to_commit(self, store):
        service = self.upload(
            store, uploaded_at=datetime.now(UTC) - timedelta(days=1)
        )

        with self.assertRaises(InvalidUpload):
            commit_upload.fn(CANDIDATE_ID)
        service.open_upload.assert_not_called()

    def test_rejects_an_empty_upload(self, store):
        self.upload(store, body=b"")

        with self.assertRaises(InvalidUpload):
            commit_upload.fn(CANDIDATE_ID)

    def test_rejects_an_upload_that_never_arrived(self, store):
        # Including a duplicate whose bytes were already discarded: there is nothing
        # left to hash, and no retry will change that.
        service = self.upload(store)
        service.describe_upload.side_effect = UploadNotWritten("nothing")

        with self.assertRaises(InvalidUpload):
            commit_upload.fn(CANDIDATE_ID)


class DiscardAbandonedUploadsTests(unittest.TestCase):
    @patch(STORE)
    def test_discards_only_candidates_older_than_twice_the_commit_window(self, store):
        store.return_value.discard_abandoned_uploads.return_value = [CANDIDATE_ID]

        discarded = discard_abandoned_uploads_flow()

        self.assertEqual(discarded, [str(CANDIDATE_ID)])
        [cutoff] = store.return_value.discard_abandoned_uploads.call_args.args
        age = datetime.now(UTC) - cutoff
        self.assertGreaterEqual(age, timedelta(hours=24))
        self.assertLess(age, timedelta(hours=24, minutes=1))


class ChunkDocumentFlowTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.store.extracted_text.return_value = "# One\n\nhello"
        self.chunker = Mock()
        for name, value in (
            (STORE, Mock(return_value=self.store)),
            (
                "ingestion.chunking_flow.chunkers",
                Mock(get=Mock(return_value=self.chunker)),
            ),
        ):
            patcher = patch(name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_existing_chunks_are_reused_without_reading_the_text(self):
        self.store.find_chunks.return_value = CHUNKS_REF.model_copy(
            update={"reused": True}
        )

        result = chunk_document_flow(EXTRACTION_REF, "chunker@2")

        assert result.chunks is not None
        self.assertTrue(result.chunks.reused)
        self.store.find_chunks.assert_called_once_with(EXTRACTION_REF, "chunker@2")
        self.store.extracted_text.assert_not_called()
        self.chunker.split.assert_not_called()
        self.store.add_chunks.assert_not_called()

    def test_the_chunker_splits_the_stored_text_then_the_store_keeps_the_chunks(self):
        self.store.find_chunks.return_value = None
        drafts = [ChunkDraft(index=0, text="hello")]
        self.chunker.split.return_value = drafts
        self.store.add_chunks.return_value = CHUNKS_REF

        result = chunk_document_flow(EXTRACTION_REF, "chunker@2")

        self.assertEqual(result.status, ArtifactStatus.OK)
        self.assertEqual(result.chunks, CHUNKS_REF)
        self.store.extracted_text.assert_called_once_with(EXTRACTION_REF)
        self.chunker.split.assert_called_once_with("# One\n\nhello")
        self.store.add_chunks.assert_called_once_with(
            EXTRACTION_REF, "chunker@2", drafts
        )

    def test_text_that_yields_no_chunks_needs_intervention(self):
        self.store.find_chunks.return_value = None
        self.chunker.split.return_value = []

        result = chunk_document_flow(EXTRACTION_REF, "chunker@2")

        self.assertEqual(result.status, ArtifactStatus.NEEDS_INTERVENTION)
        assert result.error is not None
        self.assertEqual(result.error["reason"], "no_content_extracted")
        self.assertIsNone(result.chunks)
        self.store.add_chunks.assert_not_called()

    def test_a_failure_is_raised_and_writes_nothing(self):
        self.store.find_chunks.return_value = None
        self.chunker.split.side_effect = RuntimeError("splitter broke")

        with self.assertRaises(RuntimeError):
            chunk_document_flow(EXTRACTION_REF, "chunker@2")

        self.store.add_chunks.assert_not_called()


class EmbedDocumentFlowTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.store.find_extraction.return_value = EXTRACTION_REF
        self.store.find_chunks.return_value = CHUNKS_REF
        self.embedder = Mock()
        for name, value in (
            (STORE, Mock(return_value=self.store)),
            (
                "ingestion.embedding_flow.embedders",
                Mock(get=Mock(return_value=self.embedder)),
            ),
        ):
            patcher = patch(name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.chunk = Chunk(
            chunk_id=uuid4(),
            extraction_id=EXTRACTION_ID,
            document_id=DOCUMENT_ID,
            extractor="tika@1",
            chunker="chunker@2",
            index=0,
            text="first",
        )
        self.embedding = Embedding(
            dense=[0.1], sparse=SparseVector(indices=[0], values=[1.0], dimension=4)
        )

    def test_nothing_pending_is_reused_without_encoding(self):
        self.store.unembedded_chunks.return_value = []

        result = embed_document_flow(DOCUMENT_ID, "tika@1", "chunker@2", "hybrid@1")

        self.assertTrue(result.reused)
        self.assertEqual(result.chunk_count, 3)
        self.embedder.embed.assert_not_called()
        self.store.add_embeddings.assert_not_called()

    def test_the_embedder_encodes_only_what_is_pending_then_the_store_keeps_it(self):
        self.store.unembedded_chunks.return_value = [self.chunk]
        self.embedder.embed.return_value = [self.embedding]
        self.store.add_embeddings.return_value = 1

        result = embed_document_flow(DOCUMENT_ID, "tika@1", "chunker@2", "hybrid@1")

        self.assertEqual(result.embedded, 1)
        self.assertFalse(result.reused)
        self.store.find_extraction.assert_called_once_with(DOCUMENT_ID, "tika@1")
        self.store.unembedded_chunks.assert_called_once_with(
            EXTRACTION_REF, "chunker@2", "hybrid@1"
        )
        self.embedder.embed.assert_called_once_with(["first"])
        self.store.add_embeddings.assert_called_once_with(
            "hybrid@1", {self.chunk.chunk_id: self.embedding}
        )

    def test_a_document_never_extracted_has_nothing_to_embed(self):
        self.store.find_extraction.return_value = None

        result = embed_document_flow(DOCUMENT_ID, "tika@1", "chunker@2", "hybrid@1")

        self.assertEqual(result.chunk_count, 0)
        self.store.unembedded_chunks.assert_not_called()
        self.embedder.embed.assert_not_called()

    def test_a_failed_encode_stores_nothing(self):
        self.store.unembedded_chunks.return_value = [self.chunk]
        self.embedder.embed.side_effect = RuntimeError("no gpu")

        with self.assertRaises(RuntimeError):
            embed_document_flow(DOCUMENT_ID, "tika@1", "chunker@2", "hybrid@1")

        self.store.add_embeddings.assert_not_called()


XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class IngestFlowTests(unittest.TestCase):
    def setUp(self):
        self.patches = {
            name: patch(f"ingestion.ingestion_flow.{name}")
            for name in (
                "commit_upload",
                "chunk_document_flow",
                "stage_chunked",
                "publish_bundle",
                "xlsx_ingest_flow",
            )
        }
        self.patches["document_store"] = patch(STORE)
        self.patches["extractors"] = patch("ingestion.ingestion_flow.extractors")
        self.mocks = {name: item.start() for name, item in self.patches.items()}
        for item in self.patches.values():
            self.addCleanup(item.stop)
        release_patch = patch(
            "ingestion.ingestion_flow.active_release", return_value=RELEASE
        )
        release_patch.start()
        self.addCleanup(release_patch.stop)
        slot_patch = patch(
            "ingestion.ingestion_flow.concurrency_slot",
            side_effect=lambda _key: nullcontext(),
        )
        slot_patch.start()
        self.addCleanup(slot_patch.stop)

        self.store = self.mocks["document_store"].return_value
        self.store.describe.return_value.filename = "notes.txt"
        self.store.download.side_effect = self.download
        self.store.find_extraction.return_value = None
        self.store.extraction_metadata.return_value = {"title": "Notes"}
        self.extractor = self.mocks["extractors"].get.return_value
        self.mocks["commit_upload"].return_value = DOCUMENT
        self.outcome = {"status": "ok"}
        self.mocks["chunk_document_flow"].return_value = CHUNKED
        self.mocks["stage_chunked"].side_effect = self.write_status
        self.mocks["publish_bundle"].return_value = {
            "event": "ingestion.completed",
            "ingestion_id": EVENT_ID,
            "status": "ok",
        }

    @staticmethod
    def download(_document_id, destination):
        Path(destination).write_text("hello")
        return Path(destination)

    def write_status(self, _doc, doc_dir, **_found):
        doc_dir.mkdir(parents=True, exist_ok=True)
        (doc_dir / "status.json").write_text(json.dumps(self.outcome))

    def route(self, mime_type="text/plain"):
        self.extractor.extract.return_value = Extraction(
            mime_type=mime_type, text="hello", metadata={"title": "Notes"}
        )
        self.store.add_extraction.return_value = EXTRACTION_REF.model_copy(
            update={"mime_type": mime_type}
        )

    def statuses(self):
        return [call.args[1] for call in self.store.set_status.call_args_list]

    def test_dispatches_to_chunking_then_publishes_its_bundle(self):
        self.route()

        result = ingest_document_flow(EVENT)

        self.mocks["commit_upload"].assert_called_once_with(CANDIDATE_ID)
        self.mocks["extractors"].get.assert_called_once_with("tika@1")
        self.extractor.extract.assert_called_once_with(b"hello", "notes.txt")
        extracted = self.store.add_extraction.call_args.args
        self.assertEqual(extracted[:2], (DOCUMENT_ID, "tika@1"))

        self.assertEqual(result["event"], "ingestion.completed")
        self.assertEqual(result["release"], "release-1")
        self.assertFalse(result["extraction_reused"])
        self.assertFalse(result["chunks_reused"])
        # Chunking is handed a reference to the extraction, never its text.
        self.mocks["chunk_document_flow"].assert_called_once_with(
            self.store.add_extraction.return_value, "chunker@2"
        )
        self.mocks["xlsx_ingest_flow"].assert_not_called()
        source = self.mocks["publish_bundle"].call_args.args[2]
        self.assertEqual(source["content_id"], "a" * 64)
        self.assertEqual(source["document_id"], str(DOCUMENT_ID))
        self.assertEqual(source["filename"], "notes.txt")
        self.assertNotIn("object_key", source)
        self.assertEqual(
            self.mocks["publish_bundle"].call_args.kwargs["chunks"],
            {"extractor": "tika@1", "chunker": "chunker@2", "count": 3},
        )
        staged = self.mocks["stage_chunked"].call_args.kwargs
        self.assertEqual(staged["extraction"], {"title": "Notes"})
        self.assertEqual(staged["extractor"], "tika@1")
        self.assertEqual(staged["status"], {"status": "ok"})

    def test_an_extraction_already_kept_is_reused_without_calling_tika(self):
        self.store.find_extraction.return_value = EXTRACTION_REF.model_copy(
            update={"reused": True}
        )

        result = ingest_document_flow(EVENT)

        self.assertTrue(result["extraction_reused"])
        self.extractor.extract.assert_not_called()
        self.store.add_extraction.assert_not_called()
        self.mocks["chunk_document_flow"].assert_called_once_with(
            self.store.find_extraction.return_value, "chunker@2"
        )

    def test_a_chunked_document_waits_for_embedding_before_it_is_published(self):
        self.route()

        ingest_document_flow(EVENT)

        self.assertEqual(
            self.statuses(), [DocumentStatus.INGESTING, DocumentStatus.EMBEDDING]
        )
        self.store.mark_published.assert_not_called()

    def test_a_spreadsheet_is_dispatched_to_its_own_path_and_published(self):
        self.route(XLSX_MIME)
        self.mocks["xlsx_ingest_flow"].side_effect = (
            lambda doc, output_root, _extraction: self.write_status(
                doc, output_root / doc.stem
            )
        )

        ingest_document_flow(EVENT)

        # The spreadsheet path gets the same extraction, and parses nothing again.
        self.assertEqual(
            self.mocks["xlsx_ingest_flow"].call_args.args[2].mime_type, XLSX_MIME
        )
        self.extractor.extract.assert_called_once()
        self.mocks["chunk_document_flow"].assert_not_called()
        self.mocks["stage_chunked"].assert_not_called()
        self.assertIsNone(self.mocks["publish_bundle"].call_args.kwargs["chunks"])
        self.store.mark_published.assert_called_once_with(DOCUMENT_ID)

    def test_records_needs_intervention_with_its_reason(self):
        self.route(XLSX_MIME)
        self.outcome = {"status": "needs_intervention", "reason": "no sheets"}
        self.mocks["xlsx_ingest_flow"].side_effect = (
            lambda doc, output_root, _extraction: self.write_status(
                doc, output_root / doc.stem
            )
        )

        ingest_document_flow(EVENT)

        self.store.set_status.assert_called_with(
            DOCUMENT_ID, DocumentStatus.NEEDS_INTERVENTION, {"reason": "no sheets"}
        )
        self.store.mark_published.assert_not_called()

    def test_every_run_does_the_work_again(self):
        self.route()

        ingest_document_flow(EVENT)
        ingest_document_flow(EVENT)

        self.assertEqual(self.mocks["chunk_document_flow"].call_count, 2)

    def test_records_a_failed_ingestion_before_re_raising(self):
        self.route()
        self.mocks["chunk_document_flow"].side_effect = RuntimeError("tika down")

        with self.assertRaises(RuntimeError):
            ingest_document_flow(EVENT)

        self.store.set_status.assert_called_with(
            DOCUMENT_ID,
            DocumentStatus.FAILED,
            {"type": "RuntimeError", "detail": "tika down"},
        )

    def test_an_upload_that_cannot_be_committed_records_nothing(self):
        self.mocks["commit_upload"].side_effect = InvalidUpload("upload is empty")

        with self.assertRaises(InvalidUpload):
            ingest_document_flow(EVENT)

        self.store.set_status.assert_not_called()
        self.extractor.extract.assert_not_called()

    def test_a_crash_is_recorded_on_the_candidate(self):
        # A candidate with no row - never committed, or a duplicate - has nowhere to
        # record it, and recording on a missing document is a no-op.
        flow_run = Mock(parameters={"event": EVENT})
        state = Mock(message="worker lost")

        _ingest_crashed(None, flow_run, state)

        self.store.set_status.assert_called_once_with(
            CANDIDATE_ID,
            DocumentStatus.FAILED,
            {"type": "Crashed", "detail": "worker lost"},
        )


class ProcessUploadTests(unittest.TestCase):
    @patch("ingestion.ingestion_flow.embed_release.delay")
    @patch("ingestion.ingestion_flow.ingest_document_flow")
    def test_submits_embedding_for_a_chunked_document(self, ingest, delay):
        ingest.return_value = {
            "ingestion_id": EVENT_ID,
            "document_id": str(DOCUMENT_ID),
            "status": "ok",
            "chunks": {"extractor": "tika@1", "chunker": "chunker@2", "count": 3},
        }
        delay.return_value.task_run_id = "embedding-run"

        result = process_upload.fn(EVENT)

        delay.assert_called_once_with(str(DOCUMENT_ID), "release-1")
        self.assertEqual(result["embedding_task_run_id"], "embedding-run")

    @patch("ingestion.ingestion_flow.embed_release.delay")
    @patch("ingestion.ingestion_flow.ingest_document_flow")
    def test_skips_embedding_for_a_queryable_dataset(self, ingest, delay):
        ingest.return_value = {
            "ingestion_id": EVENT_ID,
            "status": "ok",
            "artifact_type": "duckdb",
            "chunks": None,
        }

        process_upload.fn(EVENT)

        delay.assert_not_called()


@patch(STORE)
@patch("ingestion.embedding_flow.embed_document_flow")
@patch("ingestion.embedding_flow.active_release", return_value=RELEASE)
class EmbedReleaseTests(unittest.TestCase):
    def test_embeds_with_every_embedder_then_publishes(self, _release, embed, store):
        embed.return_value = EmbeddingsRef(
            document_id=DOCUMENT_ID, embedder="hybrid@1", chunk_count=3, embedded=3
        )

        result = embed_release.fn(str(DOCUMENT_ID), "release-1")

        embed.assert_called_once_with(DOCUMENT_ID, "tika@1", "chunker@2", "hybrid@1")
        self.assertEqual(result["embeddings"][0]["embedder"], "hybrid@1")
        self.assertEqual(result["embeddings"][0]["embedded"], 3)
        store.return_value.set_status.assert_called_once_with(
            DOCUMENT_ID, DocumentStatus.EMBEDDING, None
        )
        store.return_value.mark_published.assert_called_once_with(DOCUMENT_ID)

    def test_records_a_failed_embedding_before_re_raising(self, _release, embed, store):
        embed.side_effect = RuntimeError("no gpu")

        with self.assertRaises(RuntimeError):
            embed_release.fn(str(DOCUMENT_ID), "release-1")

        store.return_value.set_status.assert_called_with(
            DOCUMENT_ID,
            DocumentStatus.FAILED,
            {"type": "RuntimeError", "detail": "no gpu"},
        )
        store.return_value.mark_published.assert_not_called()


if __name__ == "__main__":
    unittest.main()
